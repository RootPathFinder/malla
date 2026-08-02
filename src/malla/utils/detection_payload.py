"""Parse DETECTION_SENSOR_APP text payloads, including dwell/burst reporting.

Firmware formats (DetectionSensorModule / PR #5):
  "{name} detected"
  "{name} detected dwell_ms=<ms>"          # older dwell-only builds
  "{name} detected burst_ms=<ms>"          # when minimum_alert_secs > 0
  "{name} cleared active_ms=<ms> burst_ms=<ms>"
  "{name} state: <0|1>"                    # heartbeat / state broadcast
"""

from __future__ import annotations

import re
from typing import Any

_TRIP_RE = re.compile(
    r"^(?P<head>.*?)\s+detected(?:\s+(?:dwell_ms|burst_ms)=(?P<ms>\d+))?\s*$",
    re.IGNORECASE,
)
_CLEARED_RE = re.compile(
    r"^(?P<head>.*?)\s+cleared(?:\s+active_ms=(?P<active>\d+))?(?:\s+burst_ms=(?P<burst>\d+))?\s*$",
    re.IGNORECASE,
)
_STATE_RE = re.compile(
    r"^(?P<head>.*?)\s+state:\s*(?P<state>0|1)\s*$",
    re.IGNORECASE,
)
_MS_TOKEN_RE = re.compile(
    r"\s+(?:dwell_ms|burst_ms|active_ms)=\d+",
    re.IGNORECASE,
)
_DETECTED_SUFFIX_RE = re.compile(r"\s+detected\s*$", re.IGNORECASE)
_CLEARED_SUFFIX_RE = re.compile(r"\s+cleared\s*$", re.IGNORECASE)


def decode_detection_text(raw_payload: Any) -> str | None:
    """Decode bytes/str payload to text, or None if empty/unusable.

    Firmware C strings are often stored with a trailing NUL; strip NULs and
    other control characters so parsers can match ``… detected``.
    """
    if raw_payload is None:
        return None
    try:
        if isinstance(raw_payload, (bytes, bytearray, memoryview)):
            # Drop NULs before decode so they never become U+FFFD / boxes
            raw = bytes(raw_payload).replace(b"\x00", b"")
            text = raw.decode("utf-8", errors="replace")
        else:
            text = str(raw_payload)
    except Exception:
        return None
    # Remove leftover NULs / replacement chars / other controls (keep \t \n)
    text = text.replace("\x00", "").replace("\ufffd", "")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    text = text.strip()
    return text or None


def parse_detection_payload(raw_payload: Any) -> dict[str, Any]:
    """Parse a detection sensor payload into structured fields.

    Returns:
        dict with keys:
          raw_text, sensor_name,
          event_kind ('trip'|'cleared'|'state'|'unknown'),
          dwell_ms, burst_ms, active_ms (int|None), state (int|None)
    """
    empty = {
        "raw_text": None,
        "sensor_name": None,
        "event_kind": "unknown",
        "dwell_ms": None,
        "burst_ms": None,
        "active_ms": None,
        "state": None,
    }
    raw_text = decode_detection_text(raw_payload)
    if not raw_text:
        return empty

    trip = _TRIP_RE.match(raw_text)
    if trip:
        name = (trip.group("head") or "").strip() or raw_text
        ms_raw = trip.group("ms")
        ms = int(ms_raw) if ms_raw is not None else None
        # Older firmware used dwell_ms=; newer uses burst_ms= on trip.
        has_burst_token = "burst_ms=" in raw_text.lower()
        return {
            "raw_text": raw_text,
            "sensor_name": name,
            "event_kind": "trip",
            "dwell_ms": None if has_burst_token else ms,
            "burst_ms": ms if has_burst_token else None,
            "active_ms": None,
            "state": None,
        }

    cleared = _CLEARED_RE.match(raw_text)
    if cleared:
        name = (cleared.group("head") or "").strip() or raw_text
        active_raw = cleared.group("active")
        burst_raw = cleared.group("burst")
        return {
            "raw_text": raw_text,
            "sensor_name": name,
            "event_kind": "cleared",
            "dwell_ms": None,
            "burst_ms": int(burst_raw) if burst_raw is not None else None,
            "active_ms": int(active_raw) if active_raw is not None else None,
            "state": None,
        }

    state = _STATE_RE.match(raw_text)
    if state:
        name = (state.group("head") or "").strip() or raw_text
        return {
            "raw_text": raw_text,
            "sensor_name": name,
            "event_kind": "state",
            "dwell_ms": None,
            "burst_ms": None,
            "active_ms": None,
            "state": int(state.group("state")),
        }

    # Fallback: strip timing tokens so catalogs stay stable
    cleaned = _MS_TOKEN_RE.sub("", raw_text).strip() or raw_text
    return {
        "raw_text": raw_text,
        "sensor_name": cleaned,
        "event_kind": "unknown",
        "dwell_ms": None,
        "burst_ms": None,
        "active_ms": None,
        "state": None,
    }


def normalize_detection_sensor_name(value: str | None) -> str:
    """Normalize a sensor name for subscription matching.

    Accepts either a configured name ("driveway") or legacy full text
    ("driveway detected" / "driveway cleared …").
    """
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = _MS_TOKEN_RE.sub("", text).strip()
    text = _DETECTED_SUFFIX_RE.sub("", text).strip()
    text = _CLEARED_SUFFIX_RE.sub("", text).strip()
    return text or str(value).strip()


def format_dwell_label(dwell_ms: int | None) -> str | None:
    """Human-readable duration, e.g. '2.0s' or '250ms'."""
    if dwell_ms is None:
        return None
    try:
        ms = int(dwell_ms)
    except (TypeError, ValueError):
        return None
    if ms < 0:
        return None
    if ms < 1000:
        return f"{ms}ms"
    secs = ms / 1000.0
    if abs(secs - round(secs)) < 0.05:
        return f"{int(round(secs))}s"
    return f"{secs:.1f}s"
