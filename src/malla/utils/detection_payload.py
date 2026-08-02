"""Parse DETECTION_SENSOR_APP text payloads, including dwell reporting.

Firmware formats (DetectionSensorModule):
  "{name} detected"
  "{name} detected dwell_ms=<ms>"   # when minimum_detect_secs > 0
  "{name} state: <0|1>"             # heartbeat / state broadcast
"""

from __future__ import annotations

import re
from typing import Any

_DWELL_RE = re.compile(
    r"^(?P<head>.*?)\s+detected(?:\s+dwell_ms=(?P<dwell>\d+))?\s*$",
    re.IGNORECASE,
)
_STATE_RE = re.compile(
    r"^(?P<head>.*?)\s+state:\s*(?P<state>0|1)\s*$",
    re.IGNORECASE,
)
_DWELL_TOKEN_RE = re.compile(r"\s+dwell_ms=\d+\s*$", re.IGNORECASE)
_DETECTED_SUFFIX_RE = re.compile(r"\s+detected\s*$", re.IGNORECASE)


def decode_detection_text(raw_payload: Any) -> str | None:
    """Decode bytes/str payload to text, or None if empty/unusable."""
    if raw_payload is None:
        return None
    try:
        if isinstance(raw_payload, bytes):
            text = raw_payload.decode("utf-8", errors="replace")
        else:
            text = str(raw_payload)
    except Exception:
        return None
    text = text.strip()
    return text or None


def parse_detection_payload(raw_payload: Any) -> dict[str, Any]:
    """Parse a detection sensor payload into structured fields.

    Returns:
        dict with keys:
          raw_text, sensor_name, event_kind ('trip'|'state'|'unknown'),
          dwell_ms (int|None), state (int|None)
    """
    raw_text = decode_detection_text(raw_payload)
    if not raw_text:
        return {
            "raw_text": None,
            "sensor_name": None,
            "event_kind": "unknown",
            "dwell_ms": None,
            "state": None,
        }

    trip = _DWELL_RE.match(raw_text)
    if trip:
        name = (trip.group("head") or "").strip() or raw_text
        dwell_raw = trip.group("dwell")
        return {
            "raw_text": raw_text,
            "sensor_name": name,
            "event_kind": "trip",
            "dwell_ms": int(dwell_raw) if dwell_raw is not None else None,
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
            "state": int(state.group("state")),
        }

    # Fallback: strip a trailing dwell token if present so catalogs stay stable
    cleaned = _DWELL_TOKEN_RE.sub("", raw_text).strip() or raw_text
    return {
        "raw_text": raw_text,
        "sensor_name": cleaned,
        "event_kind": "unknown",
        "dwell_ms": None,
        "state": None,
    }


def normalize_detection_sensor_name(value: str | None) -> str:
    """Normalize a sensor name for subscription matching.

    Accepts either a configured name ("driveway") or legacy full text
    ("driveway detected" / "driveway detected dwell_ms=2000").
    """
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = _DWELL_TOKEN_RE.sub("", text).strip()
    text = _DETECTED_SUFFIX_RE.sub("", text).strip()
    return text or str(value).strip()


def format_dwell_label(dwell_ms: int | None) -> str | None:
    """Human-readable dwell duration, e.g. '2.0s' or '250ms'."""
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
