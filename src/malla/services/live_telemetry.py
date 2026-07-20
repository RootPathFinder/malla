"""
Shared helpers for solicited (live) telemetry over TCP/Serial.

Used by the Admin live-poll API and the scheduled telemetry runner.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..utils.telemetry_request import (
    LIVE_TELEMETRY_MAX_BUDGET_S,
    live_telemetry_budget,
    split_live_telemetry_attempts,
)

logger = logging.getLogger(__name__)


def estimate_hops_from_recent_packets(node_id: int) -> int | None:
    """Fallback hop estimate from recent packet hop_start/hop_limit fields."""
    try:
        from ..database.connection import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT AVG(hop_start - hop_limit) AS avg_hops
            FROM packet_history
            WHERE from_node_id = ?
              AND hop_start IS NOT NULL
              AND hop_limit IS NOT NULL
              AND hop_start >= hop_limit
              AND timestamp > ?
            """,
            (node_id, time.time() - 7 * 86400),
        )
        row = cursor.fetchone()
        conn.close()
        if not row or row["avg_hops"] is None:
            return None
        return int(round(float(row["avg_hops"])))
    except Exception as e:
        logger.debug(f"Packet hop estimate failed for !{node_id:08x}: {e}")
        return None


def resolve_live_telemetry_hops(
    node_id_int: int, client_hops: Any = None
) -> tuple[int, str]:
    """
    Resolve hop distance for live telemetry budgeting.

    Preference: traceroute estimate -> client hint -> recent packet avg -> 1.
    """
    try:
        from .job_service import JobService

        estimated = JobService()._estimate_hop_count(node_id_int)
        if estimated is not None:
            return int(estimated), "traceroute"
    except Exception as e:
        logger.debug(f"Traceroute hop estimate failed for !{node_id_int:08x}: {e}")

    if client_hops is not None and client_hops != "":
        try:
            return int(round(float(client_hops))), "client"
        except (TypeError, ValueError):
            pass

    packet_hops = estimate_hops_from_recent_packets(node_id_int)
    if packet_hops is not None:
        return packet_hops, "packets"

    return 1, "default"


def request_live_telemetry_with_retry(
    publisher: Any,
    node_id_int: int,
    telemetry_type: str,
    timeout: float,
    *,
    attempts: int = 2,
    hop_limit: int | None = None,
    want_ack: bool = False,
    retry_delay_s: float = 0.5,
) -> tuple[dict[str, Any] | None, int]:
    """Try live telemetry with hop-aware retries. Returns (result, attempts_used)."""
    attempts_used = 0
    result = None
    attempt_timeouts = split_live_telemetry_attempts(timeout, attempts=attempts)
    for idx, attempt_timeout in enumerate(attempt_timeouts):
        attempts_used += 1
        result = publisher.send_telemetry_request(
            target_node_id=node_id_int,
            telemetry_type=telemetry_type,
            timeout=attempt_timeout,
            hop_limit=hop_limit,
            want_ack=want_ack,
        )
        if result:
            if attempts_used > 1:
                result = dict(result)
                result["retry_attempt"] = attempts_used - 1
            return result, attempts_used
        if idx < len(attempt_timeouts) - 1 and retry_delay_s > 0:
            time.sleep(retry_delay_s)
    return None, attempts_used


def get_connected_mesh_publisher() -> tuple[Any | None, str | None]:
    """
    Return (publisher, connection_type) when TCP or Serial is connected.

    MQTT is unsupported for solicited telemetry (no response wait).
    """
    try:
        from .admin_service import get_admin_service
        from .serial_publisher import get_serial_publisher
        from .tcp_publisher import get_tcp_publisher

        admin_service = get_admin_service()
        connection_type = admin_service.connection_type.value

        if connection_type == "tcp":
            publisher = get_tcp_publisher()
            if publisher.is_connected:
                return publisher, "tcp"
            return None, "tcp"
        if connection_type == "serial":
            publisher = get_serial_publisher()
            if publisher.is_connected:
                return publisher, "serial"
            return None, "serial"
        return None, connection_type
    except Exception as e:
        logger.debug(f"Could not resolve mesh publisher: {e}")
        return None, None


def solicit_node_telemetry(
    node_id_int: int,
    telemetry_type: str = "device_metrics",
    *,
    client_hops: Any = None,
) -> dict[str, Any]:
    """
    Solicit telemetry from a node using hop-aware retries.

    Returns a result dict with success/error fields (does not raise for RF miss).
    """
    publisher, connection_type = get_connected_mesh_publisher()
    if publisher is None:
        if connection_type == "mqtt":
            return {
                "success": False,
                "error": "Live telemetry requires TCP or Serial connection.",
                "node_id": node_id_int,
            }
        return {
            "success": False,
            "error": "No TCP/Serial connection available for solicited telemetry.",
            "node_id": node_id_int,
            "connection_type": connection_type,
        }

    estimated_hops, hop_source = resolve_live_telemetry_hops(node_id_int, client_hops)
    budget = live_telemetry_budget(estimated_hops)
    timeout = min(LIVE_TELEMETRY_MAX_BUDGET_S, float(budget["timeout_s"]))

    result, attempts = request_live_telemetry_with_retry(
        publisher,
        node_id_int,
        telemetry_type,
        timeout,
        attempts=budget["attempts"],
        hop_limit=budget["hop_limit"],
        want_ack=budget["want_ack"],
        retry_delay_s=budget["retry_delay_s"],
    )

    if result:
        source = "late_cache" if result.get("late_cache") else "live"
        return {
            "success": True,
            "node_id": node_id_int,
            "hex_id": f"!{node_id_int:08x}",
            "telemetry": result.get("telemetry", {}),
            "timestamp": result.get("timestamp"),
            "stats": result.get("stats", {}),
            "source": source,
            "attempts": attempts,
            "estimated_hops": budget["estimated_hops"],
            "hop_source": hop_source,
            "budget": budget,
            "telemetry_type": telemetry_type,
        }

    return {
        "success": False,
        "error": (
            f"No response from node after {attempts} attempt(s) "
            f"(~{budget.get('total_budget_s', timeout)}s, "
            f"{budget['estimated_hops']}-hop path)"
        ),
        "node_id": node_id_int,
        "hex_id": f"!{node_id_int:08x}",
        "attempts": attempts,
        "estimated_hops": budget["estimated_hops"],
        "hop_source": hop_source,
        "budget": budget,
        "telemetry_type": telemetry_type,
    }
