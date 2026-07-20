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

        return MessageToDict(telemetry_data, preserving_proto_field_name=True)

    if isinstance(telemetry_data, dict):
        result: dict[str, Any] = {}
        for key, value in telemetry_data.items():
            if key == "raw":
                continue
            if hasattr(value, "DESCRIPTOR"):
                from google.protobuf.json_format import MessageToDict

                result[key] = MessageToDict(value, preserving_proto_field_name=True)
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
    """Return True if telemetry contains a non-empty payload for the requested type."""
    if not telemetry or not isinstance(telemetry, dict):
        return False

    keys = TELEMETRY_TYPE_KEYS.get(telemetry_type, (telemetry_type,))
    for key in keys:
        value = telemetry.get(key)
        if value is None:
            continue
        if isinstance(value, dict) and len(value) == 0:
            continue
        return True
    return False


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
    2. Same ``from_node_id`` when the pending request has no request_id yet
       (race between send return and response) and metrics type matches
    3. Same ``from_node_id`` with matching metrics when the packet has no
       requestId (legacy / gateway quirks) — only if pending also lacks a
       stored request_id, to avoid stealing waits with unsolicited broadcasts
    """
    if not pending_by_node:
        return None

    # 1) Strong match on request id
    if request_id is not None:
        for node_id, pending in pending_by_node.items():
            if pending.get("completed"):
                continue
            pending_rid = normalize_request_id(pending.get("request_id"))
            if pending_rid is not None and pending_rid == request_id:
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

    # 2) Response arrived before we stored packet id — accept if metrics match
    if pending_rid is None and telemetry_has_requested_metrics(telemetry, telemetry_type):
        return from_node_id, pending

    # 3) No requestId on packet: only accept if we also never got a packet id
    #    (otherwise unsolicited telemetry would falsely complete the wait)
    if request_id is None and pending_rid is None:
        if telemetry_has_requested_metrics(telemetry, telemetry_type):
            return from_node_id, pending

    # 4) requestId present and matches node pending that somehow missed index —
    #    already handled in (1). If packet has requestId but pending has none
    #    and metrics match, accept and bind.
    if (
        request_id is not None
        and pending_rid is None
        and telemetry_has_requested_metrics(telemetry, telemetry_type)
    ):
        return from_node_id, pending

    return None


def complete_pending_telemetry(
    pending: dict[str, Any],
    *,
    telemetry: dict[str, Any],
    from_node_id: int | None,
    request_id: int | None = None,
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

    event = pending.get("event")
    if event is not None:
        event.set()
    return True


__all__ = [
    "TELEMETRY_TYPE_KEYS",
    "normalize_mesh_node_id",
    "normalize_request_id",
    "extract_request_id",
    "extract_from_node_id",
    "telemetry_to_dict",
    "telemetry_has_requested_metrics",
    "find_matching_telemetry_request",
    "complete_pending_telemetry",
]
