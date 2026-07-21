"""
Shared helpers for solicited (live) telemetry over TCP/Serial.

Used by the Admin live-poll API and the scheduled telemetry runner.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from ..utils.telemetry_request import (
    LIVE_TELEMETRY_MAX_BUDGET_S,
    live_telemetry_budget,
    split_live_telemetry_attempts,
    telemetry_has_requested_metrics,
)

logger = logging.getLogger(__name__)

# Serialize solicits so background schedules and live UI polls do not
# overwrite each other's pending request for the same publisher.
_solicit_lock = threading.RLock()


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

    Prefers an actually-connected publisher over AdminService's configured
    type, so schedules still work if the UI connection type is stale.
    MQTT is unsupported for solicited telemetry (no response wait).
    """
    try:
        from .admin_service import get_admin_service
        from .serial_publisher import get_serial_publisher
        from .tcp_publisher import get_tcp_publisher

        tcp_publisher = get_tcp_publisher()
        if getattr(tcp_publisher, "is_connected", False):
            return tcp_publisher, "tcp"

        serial_publisher = get_serial_publisher()
        if getattr(serial_publisher, "is_connected", False):
            return serial_publisher, "serial"

        try:
            connection_type = get_admin_service().connection_type.value
        except Exception:
            connection_type = None
        return None, connection_type
    except Exception as e:
        logger.debug(f"Could not resolve mesh publisher: {e}")
        return None, None


def persist_solicited_device_telemetry(
    node_id: int,
    telemetry: dict[str, Any] | None,
    *,
    timestamp: float | None = None,
) -> bool:
    """
    Persist device_metrics from a solicited reply into telemetry_data.

    Prefer ``persist_solicited_telemetry`` which also writes packet_history so
    node-detail "last telemetry" age updates.
    """
    if not telemetry or not isinstance(telemetry, dict):
        return False

    metrics = telemetry.get("device_metrics") or telemetry.get("deviceMetrics")
    if not isinstance(metrics, dict) or not metrics:
        return False

    def _num(key: str, alt: str | None = None) -> Any:
        value = metrics.get(key)
        if value is None and alt:
            value = metrics.get(alt)
        return value

    battery_level = _num("battery_level", "batteryLevel")
    voltage = _num("voltage")
    channel_utilization = _num("channel_utilization", "channelUtilization")
    air_util_tx = _num("air_util_tx", "airUtilTx")
    uptime_seconds = _num("uptime_seconds", "uptimeSeconds")

    if all(
        v is None
        for v in (
            battery_level,
            voltage,
            channel_utilization,
            air_util_tx,
            uptime_seconds,
        )
    ):
        return False

    try:
        from ..database.connection import get_db_connection

        now = time.time() if timestamp is None else float(timestamp)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                node_id INTEGER NOT NULL,
                battery_level INTEGER,
                voltage REAL,
                channel_utilization REAL,
                air_util_tx REAL,
                uptime_seconds INTEGER
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO telemetry_data (
                timestamp, node_id, battery_level, voltage,
                channel_utilization, air_util_tx, uptime_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                int(node_id) & 0xFFFFFFFF,
                battery_level,
                voltage,
                channel_utilization,
                air_util_tx,
                uptime_seconds,
            ),
        )
        if voltage is not None:
            try:
                cursor.execute(
                    """
                    UPDATE node_info
                    SET last_battery_voltage = ?
                    WHERE node_id = ?
                    """,
                    (voltage, int(node_id) & 0xFFFFFFFF),
                )
            except Exception:
                pass
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(
            "Failed to persist solicited telemetry for !%08x: %s",
            node_id & 0xFFFFFFFF,
            e,
        )
        return False


def persist_solicited_telemetry(
    node_id: int,
    telemetry: dict[str, Any] | None,
    *,
    timestamp: float | None = None,
    raw_payload: bytes | None = None,
    mesh_packet_id: int | None = None,
) -> dict[str, bool]:
    """
    Persist a fresh solicited telemetry reply for UI + health.

    Writes:
    - ``packet_history`` TELEMETRY_APP row (what node detail "last telemetry" reads)
    - ``telemetry_data`` device metrics (what battery/health analytics read)
    """
    from ..utils.telemetry_request import telemetry_dict_to_raw_payload

    result = {"packet_history": False, "telemetry_data": False}
    if not telemetry or not isinstance(telemetry, dict):
        return result

    payload = raw_payload or telemetry_dict_to_raw_payload(telemetry)
    if not payload:
        return result

    now = time.time() if timestamp is None else float(timestamp)
    node_id_int = int(node_id) & 0xFFFFFFFF

    try:
        from meshtastic import portnums_pb2

        from ..database.connection import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS packet_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                topic TEXT NOT NULL,
                from_node_id INTEGER,
                to_node_id INTEGER,
                portnum INTEGER,
                portnum_name TEXT,
                gateway_id TEXT,
                channel_id TEXT,
                mesh_packet_id INTEGER,
                payload_length INTEGER,
                raw_payload BLOB,
                processed_successfully BOOLEAN DEFAULT TRUE,
                message_type TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO packet_history (
                timestamp, topic, from_node_id, to_node_id, portnum, portnum_name,
                gateway_id, channel_id, mesh_packet_id, payload_length, raw_payload,
                processed_successfully, message_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                "solicited/telemetry",
                node_id_int,
                None,
                int(portnums_pb2.PortNum.TELEMETRY_APP),
                "TELEMETRY_APP",
                "local-solicit",
                None,
                mesh_packet_id,
                len(payload),
                payload,
                True,
                "solicited_telemetry",
            ),
        )
        conn.commit()
        conn.close()
        result["packet_history"] = True
    except Exception as e:
        logger.warning(
            "Failed to persist solicited packet_history for !%08x: %s",
            node_id_int,
            e,
        )

    result["telemetry_data"] = persist_solicited_device_telemetry(
        node_id_int, telemetry, timestamp=now
    )
    return result


def solicit_node_telemetry(
    node_id_int: int,
    telemetry_type: str = "device_metrics",
    *,
    client_hops: Any = None,
    fallback_device_metrics: bool = True,
    accept_last_known_s: float = 0.0,
    persist: bool = True,
) -> dict[str, Any]:
    """
    Solicit telemetry from a node using hop-aware retries.

    Returns a result dict with success/error fields (does not raise for RF miss).

    ``fresh`` is True only for live/late RF replies (not recycled last_known).
    Scheduled polls should require ``fresh`` + packet_history persistence.
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
    requested_type = (telemetry_type or "device_metrics").strip() or "device_metrics"

    types_to_try = [requested_type]
    if fallback_device_metrics and requested_type != "device_metrics":
        types_to_try.append("device_metrics")

    result: dict[str, Any] | None = None
    attempts = 0
    used_type = requested_type

    with _solicit_lock:
        for type_key in types_to_try:
            used_type = type_key
            result, attempt_count = request_live_telemetry_with_retry(
                publisher,
                node_id_int,
                type_key,
                timeout,
                attempts=budget["attempts"],
                hop_limit=budget["hop_limit"],
                want_ack=budget["want_ack"],
                retry_delay_s=budget["retry_delay_s"],
            )
            attempts += attempt_count
            if result:
                break

        if not result and accept_last_known_s > 0:
            getter = getattr(publisher, "get_latest_node_telemetry", None)
            if callable(getter):
                cached = getter(node_id_int, max_age_s=float(accept_last_known_s))
                if (
                    isinstance(cached, dict)
                    and cached.get("telemetry")
                    and (
                        telemetry_has_requested_metrics(
                            cached["telemetry"], used_type
                        )
                        or telemetry_has_requested_metrics(
                            cached["telemetry"], "device_metrics"
                        )
                    )
                ):
                    result = {
                        "telemetry": cached["telemetry"],
                        "timestamp": cached.get("timestamp"),
                        "stats": publisher.get_telemetry_stats(node_id_int)
                        if hasattr(publisher, "get_telemetry_stats")
                        else {},
                        "late_cache": True,
                        "last_known": True,
                        "raw_payload": cached.get("raw_payload"),
                    }

    if result:
        source = "late_cache" if result.get("late_cache") else "live"
        if result.get("last_known"):
            source = "last_known"
        fresh = source in ("live", "late_cache")
        telemetry = result.get("telemetry", {}) or {}
        persisted = False
        persisted_packet = False
        if persist and fresh:
            persist_info = persist_solicited_telemetry(
                node_id_int,
                telemetry,
                timestamp=time.time(),  # wall-clock now so UI age is "just now"
                raw_payload=result.get("raw_payload"),
                mesh_packet_id=result.get("request_id"),
            )
            persisted_packet = bool(persist_info.get("packet_history"))
            persisted = persisted_packet or bool(persist_info.get("telemetry_data"))
        return {
            "success": True,
            "fresh": fresh,
            "node_id": node_id_int,
            "hex_id": f"!{node_id_int:08x}",
            "telemetry": telemetry,
            "timestamp": result.get("timestamp"),
            "stats": result.get("stats", {}),
            "source": source,
            "attempts": attempts,
            "estimated_hops": budget["estimated_hops"],
            "hop_source": hop_source,
            "budget": budget,
            "telemetry_type": used_type,
            "requested_telemetry_type": requested_type,
            "persisted": persisted,
            "persisted_packet_history": persisted_packet,
            "routing_warning": result.get("routing_warning"),
        }

    return {
        "success": False,
        "fresh": False,
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
        "telemetry_type": used_type,
        "requested_telemetry_type": requested_type,
    }
