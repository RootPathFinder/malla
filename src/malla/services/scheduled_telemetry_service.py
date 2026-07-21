"""
Background runner for operator-scheduled solicited telemetry.

Mesh operators configure per-node intervals (minimum 30 minutes). This service
periodically claims due schedules and solicits telemetry over TCP/Serial using
the same hop-aware path as live Admin polls.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from typing import Any

from ..database.scheduled_telemetry_repository import (
    ScheduledTelemetryRepository,
    init_scheduled_telemetry_tables,
)
from .live_telemetry import get_connected_mesh_publisher, solicit_node_telemetry

logger = logging.getLogger(__name__)

# How often the worker looks for due schedules
_DEFAULT_TICK_SECONDS = 60
# Cap RF load per tick
_DEFAULT_MAX_PER_TICK = 3
# Pause between node solicitations in a tick
_DEFAULT_INTER_NODE_DELAY_S = 2.0

_runner_thread: threading.Thread | None = None
_runner_stop_event = threading.Event()
_tick_seconds = _DEFAULT_TICK_SECONDS
_max_per_tick = _DEFAULT_MAX_PER_TICK
_inter_node_delay_s = _DEFAULT_INTER_NODE_DELAY_S
_last_run_summary: dict[str, Any] = {
    "last_tick_at": None,
    "claimed": 0,
    "succeeded": 0,
    "failed": 0,
    "skipped_reason": None,
}


def _safe_log(level: int, message: str) -> None:
    if sys.is_finalizing():
        return
    old_raise = logging.raiseExceptions
    try:
        logging.raiseExceptions = False
        logger.log(level, message)
    except (ValueError, OSError, AttributeError):
        pass
    finally:
        logging.raiseExceptions = old_raise


def get_runner_status() -> dict[str, Any]:
    """Return runner status for Admin UI."""
    publisher, connection_type = get_connected_mesh_publisher()
    return {
        "running": _runner_thread is not None and _runner_thread.is_alive(),
        "tick_seconds": _tick_seconds,
        "max_per_tick": _max_per_tick,
        "publisher_connected": publisher is not None,
        "connection_type": connection_type,
        "last_run": dict(_last_run_summary),
    }


def start_scheduled_telemetry_runner(
    *,
    tick_seconds: int = _DEFAULT_TICK_SECONDS,
    max_per_tick: int = _DEFAULT_MAX_PER_TICK,
    inter_node_delay_s: float = _DEFAULT_INTER_NODE_DELAY_S,
) -> None:
    """Start the background scheduled telemetry runner."""
    global _runner_thread, _tick_seconds, _max_per_tick, _inter_node_delay_s

    init_scheduled_telemetry_tables()

    if _runner_thread is not None and _runner_thread.is_alive():
        logger.warning("Scheduled telemetry runner already running")
        return

    _tick_seconds = max(15, int(tick_seconds))
    _max_per_tick = max(1, int(max_per_tick))
    _inter_node_delay_s = max(0.0, float(inter_node_delay_s))
    _runner_stop_event.clear()

    _runner_thread = threading.Thread(
        target=_scheduled_telemetry_worker,
        name="ScheduledTelemetryRunner",
        daemon=True,
    )
    _runner_thread.start()
    logger.info(
        "Scheduled telemetry runner started "
        f"(tick={_tick_seconds}s, max_per_tick={_max_per_tick})"
    )


def stop_scheduled_telemetry_runner() -> None:
    """Stop the background runner."""
    global _runner_thread
    if _runner_thread is None or not _runner_thread.is_alive():
        _safe_log(logging.DEBUG, "Scheduled telemetry runner not running")
        return
    _safe_log(logging.INFO, "Stopping scheduled telemetry runner...")
    _runner_stop_event.set()
    _runner_thread.join(timeout=5)
    _runner_thread = None
    _safe_log(logging.INFO, "Scheduled telemetry runner stopped")


def _evaluate_schedule_outcome(
    outcome: dict[str, Any],
    *,
    requested_type: str = "device_metrics",
) -> tuple[bool, str | None, str, dict[str, Any]]:
    """
    Decide schedule success and build a verbose last-run detail payload.

    Success requires a fresh mesh reply that was written to packet_history
    (so node-detail "last telemetry" age updates).
    """
    fresh = bool(outcome.get("fresh")) and bool(
        outcome.get("persisted_packet_history")
    )
    ok = bool(outcome.get("success")) and fresh
    used_type = str(outcome.get("telemetry_type") or requested_type)
    error: str | None = None
    if not ok:
        if outcome.get("source") == "last_known":
            error = "No fresh mesh reply (stale cache only)"
        elif outcome.get("success") and not outcome.get("persisted_packet_history"):
            error = "Got reply but failed to persist telemetry packet for node details"
        else:
            error = str(outcome.get("error") or "request failed")

    detail = {
        "ok": ok,
        "fresh": bool(outcome.get("fresh")),
        "source": outcome.get("source"),
        "attempts": outcome.get("attempts"),
        "estimated_hops": outcome.get("estimated_hops"),
        "hop_source": outcome.get("hop_source"),
        "persisted_packet_history": bool(outcome.get("persisted_packet_history")),
        "persisted_telemetry_data": bool(outcome.get("persisted_telemetry_data")),
        "telemetry_type": used_type,
        "error": error,
        "routing_warning": outcome.get("routing_warning"),
    }
    return ok, error, used_type, detail


def run_due_schedules_once(
    *,
    limit: int | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """
    Claim and solicit due schedules once.

    Safe to call from tests or a manual Admin "run due" action.
    """
    global _last_run_summary

    publisher, connection_type = get_connected_mesh_publisher()
    if publisher is None:
        summary = {
            "last_tick_at": time.time(),
            "claimed": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped_reason": (
                "mqtt_unsupported"
                if connection_type == "mqtt"
                else "publisher_disconnected"
            ),
            "results": [],
        }
        _last_run_summary = {
            k: summary[k]
            for k in (
                "last_tick_at",
                "claimed",
                "succeeded",
                "failed",
                "skipped_reason",
            )
        }
        return summary

    claimed = ScheduledTelemetryRepository.claim_due_schedules(
        limit=limit if limit is not None else _max_per_tick,
        now=now,
    )
    succeeded = 0
    failed = 0
    results: list[dict[str, Any]] = []

    for idx, schedule in enumerate(claimed):
        if _runner_stop_event.is_set():
            break
        node_id = int(schedule["node_id"])
        # Always solicit device_metrics for health freshness; secondary types
        # are unreliable on routers and previously marked "success" without
        # updating node-detail last telemetry (packet_history).
        telemetry_type = "device_metrics"
        outcome = solicit_node_telemetry(
            node_id,
            telemetry_type,
            fallback_device_metrics=False,
            accept_last_known_s=0.0,
            persist=True,
        )
        ok, error, used_type, detail = _evaluate_schedule_outcome(
            outcome, requested_type=telemetry_type
        )
        ScheduledTelemetryRepository.record_result(
            node_id,
            success=ok,
            telemetry_type=used_type,
            error=error,
            run_detail=detail,
        )
        if ok:
            succeeded += 1
        else:
            failed += 1
        results.append(
            {
                "node_id": node_id,
                "hex_id": f"!{node_id:08x}",
                "telemetry_type": used_type,
                "success": ok,
                "error": error,
                "source": outcome.get("source"),
                "attempts": outcome.get("attempts"),
                "estimated_hops": outcome.get("estimated_hops"),
                "persisted": outcome.get("persisted"),
                "persisted_packet_history": outcome.get("persisted_packet_history"),
                "fresh": outcome.get("fresh"),
                "detail": detail,
            }
        )
        if idx < len(claimed) - 1 and _inter_node_delay_s > 0:
            if _runner_stop_event.wait(_inter_node_delay_s):
                break

    summary = {
        "last_tick_at": time.time(),
        "claimed": len(claimed),
        "succeeded": succeeded,
        "failed": failed,
        "skipped_reason": None,
        "results": results,
    }
    _last_run_summary = {
        k: summary[k]
        for k in ("last_tick_at", "claimed", "succeeded", "failed", "skipped_reason")
    }
    return summary


def run_schedule_now(node_id: int) -> dict[str, Any]:
    """
    Manually solicit one scheduled node now (does not require due).

    Still advances next_type_index and updates last_* fields. Also bumps
    last_requested_at so the regular schedule does not immediately re-fire.
    """
    schedule = ScheduledTelemetryRepository.get_schedule(node_id)
    if schedule is None:
        return {"success": False, "error": "No schedule for this node"}

    # Mark requested now to avoid double-fire with the background runner.
    from ..database.connection import get_db_connection

    now = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE scheduled_telemetry_requests
        SET last_requested_at = ?, updated_at = ?
        WHERE node_id = ?
        """,
        (now, now, node_id),
    )
    conn.commit()
    conn.close()

    # Health schedules always solicit device_metrics so node-detail last
    # telemetry (packet_history) and battery tables stay in sync.
    telemetry_type = "device_metrics"
    outcome = solicit_node_telemetry(
        node_id,
        telemetry_type,
        fallback_device_metrics=False,
        accept_last_known_s=0.0,
        persist=True,
    )
    ok, error, used_type, detail = _evaluate_schedule_outcome(
        outcome, requested_type=telemetry_type
    )
    ScheduledTelemetryRepository.record_result(
        node_id,
        success=ok,
        telemetry_type=used_type,
        error=error,
        run_detail=detail,
    )
    outcome = dict(outcome)
    outcome["success"] = ok
    outcome["telemetry_type"] = used_type
    outcome["detail"] = detail
    if error and not ok:
        outcome["error"] = error
    outcome["schedule"] = ScheduledTelemetryRepository.get_schedule(node_id)
    return outcome


def _scheduled_telemetry_worker() -> None:
    _safe_log(logging.INFO, "Scheduled telemetry worker started")
    # Brief delay so TCP auto-connect can finish first.
    if _runner_stop_event.wait(45):
        _safe_log(logging.INFO, "Scheduled telemetry worker stopped during startup wait")
        return

    while not _runner_stop_event.is_set():
        try:
            summary = run_due_schedules_once()
            if summary["claimed"]:
                _safe_log(
                    logging.INFO,
                    "Scheduled telemetry tick: "
                    f"claimed={summary['claimed']} "
                    f"ok={summary['succeeded']} fail={summary['failed']}",
                )
            elif summary.get("skipped_reason"):
                _safe_log(
                    logging.DEBUG,
                    f"Scheduled telemetry tick skipped: {summary['skipped_reason']}",
                )
        except Exception as e:
            _safe_log(logging.ERROR, f"Error in scheduled telemetry worker: {e}")

        _runner_stop_event.wait(_tick_seconds)

    _safe_log(logging.INFO, "Scheduled telemetry worker stopped")
