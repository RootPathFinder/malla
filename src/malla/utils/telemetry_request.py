"""
Helpers for reliable live telemetry request/response correlation.

Live telemetry previously matched responses only by from-node id, which made
unsolicited broadcasts and concurrent requests race with the real reply.
These helpers normalize ids and validate requestId + metric type before
completing a pending wait.
"""

from __future__ import annotations

from typing import Any

# Requested telemetry type -> possible decoded dict keys (proto JSON variants)
TELEMETRY_TYPE_KEYS: dict[str, tuple[str, ...]] = {
    "device_metrics": ("device_metrics", "deviceMetrics"),
    "environment_metrics": ("environment_metrics", "environmentMetrics"),
    "air_quality_metrics": ("air_quality_metrics", "airQualityMetrics"),
    "power_metrics": ("power_metrics", "powerMetrics"),
    "local_stats": ("local_stats", "localStats"),
    "health_metrics": ("health_metrics", "healthMetrics"),
    "host_metrics": ("host_metrics", "hostMetrics"),
}

# Keep device metrics frequent; rotate secondary types so RF load stays light.
LIVE_TELEMETRY_TYPE_ROTATION: tuple[str, ...] = (
    "device_metrics",
    "environment_metrics",
    "device_metrics",
    "local_stats",
    "device_metrics",
    "power_metrics",
    "device_metrics",
    "air_quality_metrics",
)


def next_live_telemetry_type(poll_index: int) -> str:
    """Return the telemetry type for a 0-based live-poll index."""
    if not LIVE_TELEMETRY_TYPE_ROTATION:
        return "device_metrics"
    idx = int(poll_index) % len(LIVE_TELEMETRY_TYPE_ROTATION)
    return LIVE_TELEMETRY_TYPE_ROTATION[idx]


def apply_telemetry_request_type(telemetry: Any, telemetry_type: str) -> str:
    """
    Fill an empty Telemetry protobuf with the requested metrics oneof.

    Returns the type that was applied (falls back to device_metrics).
    """
    from meshtastic import telemetry_pb2

    requested = (telemetry_type or "device_metrics").strip()
    if requested not in TELEMETRY_TYPE_KEYS:
        requested = "device_metrics"

    if requested == "environment_metrics":
        telemetry.environment_metrics.CopyFrom(telemetry_pb2.EnvironmentMetrics())
    elif requested == "power_metrics":
        telemetry.power_metrics.CopyFrom(telemetry_pb2.PowerMetrics())
    elif requested == "local_stats":
        telemetry.local_stats.CopyFrom(telemetry_pb2.LocalStats())
    elif requested == "air_quality_metrics":
        telemetry.air_quality_metrics.CopyFrom(telemetry_pb2.AirQualityMetrics())
    elif requested == "health_metrics":
        telemetry.health_metrics.CopyFrom(telemetry_pb2.HealthMetrics())
    elif requested == "host_metrics":
        telemetry.host_metrics.CopyFrom(telemetry_pb2.HostMetrics())
    else:
        telemetry.device_metrics.CopyFrom(telemetry_pb2.DeviceMetrics())
        requested = "device_metrics"
    return requested


def normalize_mesh_node_id(value: Any) -> int | None:
    """
    Normalize a mesh node id from packet fields to an unsigned 32-bit int.

    Accepts ints and common string forms: ``!aabbccdd``, ``0xaabbccdd``,
    decimal digits, or bare 8-char hex.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value & 0xFFFFFFFF
    if not isinstance(value, str):
        try:
            return int(value) & 0xFFFFFFFF
        except (TypeError, ValueError):
            return None

    text = value.strip()
    if not text:
        return None

    try:
        if text.startswith("!"):
            return int(text[1:], 16) & 0xFFFFFFFF
        if text.startswith(("0x", "0X")):
            return int(text, 16) & 0xFFFFFFFF
        if text.isdigit() or (text[0] == "-" and text[1:].isdigit()):
            return int(text, 10) & 0xFFFFFFFF
        # Bare hex (common when fromId omits the '!' prefix)
        if all(c in "0123456789abcdefABCDEF" for c in text):
            return int(text, 16) & 0xFFFFFFFF
        return int(text, 10) & 0xFFFFFFFF
    except ValueError:
        return None


def normalize_request_id(request_id: Any) -> int | None:
    """Normalize packet/request ids to unsigned 32-bit ints."""
    if request_id is None or request_id == "":
        return None
    try:
        return int(request_id) & 0xFFFFFFFF
    except (TypeError, ValueError):
        return None


def extract_request_id(packet: dict[str, Any]) -> int | None:
    """Pull requestId from a meshtastic receive packet (top-level or decoded)."""
    decoded = packet.get("decoded") or {}
    return normalize_request_id(
        packet.get("requestId")
        or packet.get("request_id")
        or decoded.get("requestId")
        or decoded.get("request_id")
    )


def extract_from_node_id(packet: dict[str, Any]) -> int | None:
    """Pull sender node id from a meshtastic receive packet."""
    return normalize_mesh_node_id(
        packet.get("from") if packet.get("from") is not None else packet.get("fromId")
    )


def telemetry_to_dict(telemetry_data: Any) -> dict[str, Any]:
    """Convert decoded telemetry (dict or protobuf) to a JSON-safe dict."""
    if not telemetry_data:
        return {}

    if hasattr(telemetry_data, "DESCRIPTOR"):
        from google.protobuf.json_format import MessageToDict

        try:
            # Keep zero/default scalars (battery 0 etc.) so match/persist see fields.
            return MessageToDict(
                telemetry_data,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
        except TypeError:
            return MessageToDict(telemetry_data, preserving_proto_field_name=True)

    if isinstance(telemetry_data, dict):
        result: dict[str, Any] = {}
        for key, value in telemetry_data.items():
            if key == "raw":
                continue
            if hasattr(value, "DESCRIPTOR"):
                from google.protobuf.json_format import MessageToDict

                try:
                    result[key] = MessageToDict(
                        value,
                        preserving_proto_field_name=True,
                        always_print_fields_with_no_presence=True,
                    )
                except TypeError:
                    result[key] = MessageToDict(
                        value, preserving_proto_field_name=True
                    )
            elif isinstance(value, dict):
                # Drop nested raw protobuf copies
                result[key] = {
                    k: v for k, v in value.items() if k != "raw" and not hasattr(v, "DESCRIPTOR")
                }
            else:
                result[key] = value
        return result

    return {}


def telemetry_has_requested_metrics(
    telemetry: dict[str, Any] | None, telemetry_type: str
) -> bool:
    """
    Return True if telemetry contains the requested metric oneof.

    An empty dict for the oneof still counts — firmware sometimes answers
    wantResponse with a DeviceMetrics shell whose scalars are default/omitted.
    Matching must succeed so we keep the raw payload for persistence.
    """
    if not telemetry or not isinstance(telemetry, dict):
        return False

    keys = TELEMETRY_TYPE_KEYS.get(telemetry_type, (telemetry_type,))
    for key in keys:
        if key not in telemetry:
            continue
        value = telemetry.get(key)
        if value is None:
            continue
        return True
    return False


def extract_telemetry_raw_payload(packet: dict[str, Any]) -> bytes | None:
    """Extract TELEMETRY_APP payload bytes from a received mesh packet, if present."""
    decoded = packet.get("decoded") or {}
    payload = decoded.get("payload")
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    if isinstance(payload, str):
        # Some paths hex-encode payload
        try:
            return bytes.fromhex(payload)
        except ValueError:
            return None

    telemetry_obj = decoded.get("telemetry")
    if telemetry_obj is not None and hasattr(telemetry_obj, "SerializeToString"):
        try:
            return telemetry_obj.SerializeToString()
        except Exception:
            return None
    return None


def telemetry_dict_to_raw_payload(telemetry: dict[str, Any] | None) -> bytes | None:
    """Rebuild a Telemetry protobuf payload from a decoded metrics dict."""
    if not telemetry or not isinstance(telemetry, dict):
        return None
    try:
        from google.protobuf.json_format import ParseDict
        from meshtastic import telemetry_pb2

        # Drop non-proto helpers
        cleaned = {
            k: v
            for k, v in telemetry.items()
            if k not in ("raw", "time") and not str(k).startswith("_")
        }
        if not cleaned:
            return None
        msg = telemetry_pb2.Telemetry()
        ParseDict(cleaned, msg, ignore_unknown_fields=True)
        payload = msg.SerializeToString()
        return payload if payload else None
    except Exception:
        return None


def extract_portnum(packet: dict[str, Any]) -> str | None:
    """Return decoded portnum name if present."""
    decoded = packet.get("decoded") or {}
    portnum = decoded.get("portnum") or packet.get("portnum")
    if portnum is None:
        return None
    return str(portnum)


def is_routing_no_response(packet: dict[str, Any]) -> bool:
    """True when mesh reports the destination did not produce an app response."""
    decoded = packet.get("decoded") or {}
    routing = decoded.get("routing") or {}
    reason = str(routing.get("errorReason") or routing.get("error_reason") or "")
    return reason.upper() == "NO_RESPONSE"


# After the main wait ends, keep listening briefly for a late TELEMETRY_APP.
TELEMETRY_REPLY_GRACE_S = 1.25
# Accept publisher-cached replies that arrived during the wait but missed match.
TELEMETRY_LATE_CACHE_S = 12.0


def note_telemetry_routing_no_response(pending: dict[str, Any]) -> None:
    """
    Record mesh NO_RESPONSE without completing the pending wait.

    Firmware often emits ROUTING_APP NO_RESPONSE while the TELEMETRY_APP reply
    is still in flight (especially with wantAck+wantResponse). Waking the waiter
    early aborts the attempt and drops the real metrics that arrive milliseconds
    later — a common failure mode for nearby 0/1-hop nodes.
    """
    response_data = pending.setdefault("response_data", {})
    response_data["routing_warning"] = "NO_RESPONSE"
    # Intentionally do NOT set pending["event"] or mark completed.


def wait_for_telemetry_reply(
    response_event: Any,
    response_data: dict[str, Any],
    *,
    timeout: float,
    grace_s: float = TELEMETRY_REPLY_GRACE_S,
) -> None:
    """Wait for metrics (or full timeout), then a short late-RF grace."""
    response_event.wait(timeout=max(0.0, float(timeout)))
    if not response_data.get("telemetry"):
        response_event.wait(timeout=max(0.0, float(grace_s)))


def pickup_late_telemetry_cache(
    *,
    late_by_request: dict[int, dict[str, Any]],
    latest_by_node: dict[int, dict[str, Any]],
    request_id: int | None,
    target_node_id: int,
    max_age_s: float = TELEMETRY_LATE_CACHE_S,
) -> dict[str, Any] | None:
    """Return a recently cached telemetry payload suitable for late pickup."""
    import time

    late = None
    if request_id is not None:
        late = late_by_request.pop(request_id, None)
    if (not late or not late.get("telemetry")) and target_node_id in latest_by_node:
        cached = latest_by_node.get(target_node_id)
        if cached and cached.get("telemetry"):
            age = time.time() - float(cached.get("timestamp") or 0)
            if age <= float(max_age_s):
                late = cached
    if late and late.get("telemetry"):
        return late
    return None



def find_matching_telemetry_request(
    pending_by_node: dict[int, dict[str, Any]],
    *,
    from_node_id: int | None,
    request_id: int | None,
    telemetry: dict[str, Any],
) -> tuple[int, dict[str, Any]] | None:
    """
    Find the pending request that this TELEMETRY_APP packet should complete.

    Preference order:
    1. Exact ``request_id`` match (most reliable)
    2. Same ``from_node_id`` with the requested metric type

    Some firmware/gateway paths omit ``requestId`` on the telemetry reply even
    when the request used ``want_response``. While a solicited wait is active,
    same-node metrics are accepted so nearby monitoring does not time out.
    """
    if not pending_by_node:
        return None

    # 1) Strong match on request id (requires real metrics, not routing/empty)
    if request_id is not None:
        for node_id, pending in pending_by_node.items():
            if pending.get("completed"):
                continue
            pending_rid = normalize_request_id(pending.get("request_id"))
            if pending_rid is None or pending_rid != request_id:
                continue
            telemetry_type = pending.get("telemetry_type") or "device_metrics"
            if telemetry_has_requested_metrics(telemetry, telemetry_type):
                return node_id, pending

    if from_node_id is None or from_node_id not in pending_by_node:
        return None

    pending = pending_by_node[from_node_id]
    if pending.get("completed"):
        return None

    pending_rid = normalize_request_id(pending.get("request_id"))
    telemetry_type = pending.get("telemetry_type") or "device_metrics"

    # Reject clearly mismatched request ids for this node
    if (
        request_id is not None
        and pending_rid is not None
        and request_id != pending_rid
    ):
        return None

    # 2) Same-node reply with the requested metrics — covers:
    #    - response before we stored packet id
    #    - firmware that omits requestId on TELEMETRY_APP replies
    #    - useful unsolicited telemetry while a live wait is active
    if telemetry_has_requested_metrics(telemetry, telemetry_type):
        return from_node_id, pending

    return None


def complete_pending_telemetry(
    pending: dict[str, Any],
    *,
    telemetry: dict[str, Any],
    from_node_id: int | None,
    request_id: int | None = None,
    raw_payload: bytes | None = None,
) -> bool:
    """
    Mark a pending request complete and signal its waiters.

    Returns False if it was already completed (idempotent dual-path delivery).
    """
    if pending.get("completed"):
        return False

    pending["completed"] = True
    if request_id is not None and pending.get("request_id") is None:
        pending["request_id"] = request_id

    import time

    response_data = pending.setdefault("response_data", {})
    response_data["telemetry"] = telemetry
    response_data["timestamp"] = time.time()
    if from_node_id is not None:
        response_data["from_id"] = f"!{from_node_id:08x}"
        response_data["from_node"] = from_node_id
    if request_id is not None:
        response_data["request_id"] = request_id
    if raw_payload is not None:
        response_data["raw_payload"] = raw_payload

    event = pending.get("event")
    if event is not None:
        event.set()
    return True


# Max wall-clock wait for a single live-telemetry HTTP request. Must stay under
# the Gunicorn worker timeout (raised to 90s for multi-hop monitoring).
LIVE_TELEMETRY_MAX_BUDGET_S = 55.0


def clamp_estimated_hops(estimated_hops: int | float | None) -> int:
    """Normalize hop estimates into 0..7 (typical Meshtastic hop_limit range)."""
    if estimated_hops is None:
        # Unknown path: assume 1 hop so we are not overly aggressive
        return 1
    try:
        hops = int(round(float(estimated_hops)))
    except (TypeError, ValueError):
        return 1
    return max(0, min(hops, 7))


def live_telemetry_budget(estimated_hops: int | float | None) -> dict[str, Any]:
    """
    Recommend wait/retry/send settings for live telemetry at a given hop distance.

    ``timeout_s`` is the wait **per send attempt**. Nearby nodes alternate
    ``want_ack`` across retries — ACK+wantResponse often emits a premature
    ROUTING NO_RESPONSE that used to abort healthy local solicits.
    """
    hops = clamp_estimated_hops(estimated_hops)

    if hops <= 0:
        # Direct / local: prefer a no-ACK first try, then ACK retries
        per_attempt_s = 14.0
        attempts = 3
        poll_interval_ms = 5000
        min_gap_ms = 1500
        want_ack = False
        want_ack_sequence = [False, True, True]
        hop_limit = 7
        retry_delay_s = 0.75
    elif hops == 1:
        per_attempt_s = 14.0
        attempts = 3
        poll_interval_ms = 8000
        min_gap_ms = 2000
        want_ack = False
        want_ack_sequence = [False, True, True]
        hop_limit = 7
        retry_delay_s = 0.75
    elif hops == 2:
        per_attempt_s = 18.0
        attempts = 2
        poll_interval_ms = 12000
        min_gap_ms = 3000
        want_ack = True
        want_ack_sequence = [True, True]
        hop_limit = 4
        retry_delay_s = 1.25
    else:
        per_attempt_s = min(22.0, 12.0 + 2.5 * hops)
        attempts = 3
        poll_interval_ms = min(30000, 8000 + hops * 4000)
        min_gap_ms = min(8000, 2500 + hops * 800)
        want_ack = True
        want_ack_sequence = [True, True, True]
        hop_limit = min(7, hops + 2)
        retry_delay_s = min(3.0, 0.75 + 0.4 * hops)

    total_budget_s = min(
        LIVE_TELEMETRY_MAX_BUDGET_S,
        per_attempt_s * attempts + retry_delay_s * max(0, attempts - 1),
    )

    return {
        "estimated_hops": hops,
        "timeout_s": round(per_attempt_s, 1),
        "total_budget_s": round(total_budget_s, 1),
        "attempts": attempts,
        "poll_interval_ms": poll_interval_ms,
        "min_gap_ms": min_gap_ms,
        "want_ack": want_ack,
        "want_ack_sequence": want_ack_sequence,
        "hop_limit": hop_limit,
        "retry_delay_s": retry_delay_s,
    }


def split_live_telemetry_attempts(
    timeout: float, attempts: int = 2
) -> list[float]:
    """
    Build per-attempt wait windows.

    Each attempt gets the full ``timeout`` (capped). Nearby links were failing
    when a shared budget was split into a tiny final retry (~2s).
    """
    per_attempt = max(5.0, float(timeout))
    per_attempt = min(per_attempt, LIVE_TELEMETRY_MAX_BUDGET_S)
    attempts = max(1, min(int(attempts), 3))

    # Keep cumulative wait under the global HTTP budget.
    max_total = LIVE_TELEMETRY_MAX_BUDGET_S
    if per_attempt * attempts > max_total:
        per_attempt = max(5.0, max_total / attempts)

    return [round(per_attempt, 2)] * attempts


__all__ = [
    "TELEMETRY_TYPE_KEYS",
    "LIVE_TELEMETRY_MAX_BUDGET_S",
    "TELEMETRY_REPLY_GRACE_S",
    "TELEMETRY_LATE_CACHE_S",
    "normalize_mesh_node_id",
    "normalize_request_id",
    "extract_request_id",
    "extract_from_node_id",
    "extract_portnum",
    "is_routing_no_response",
    "note_telemetry_routing_no_response",
    "wait_for_telemetry_reply",
    "pickup_late_telemetry_cache",
    "telemetry_to_dict",
    "telemetry_has_requested_metrics",
    "find_matching_telemetry_request",
    "complete_pending_telemetry",
    "clamp_estimated_hops",
    "live_telemetry_budget",
    "split_live_telemetry_attempts",
    "apply_telemetry_request_type",
    "extract_telemetry_raw_payload",
    "telemetry_dict_to_raw_payload",
]
