"""Helpers for DetectionSensorConfig including custom firmware dwell/burst fields.

Firmware PR (RootPathFinder/firmware-meshtastic#5) adds:
  - minimum_detect_secs (field 9): pin must stay continuously active
  - burst_gap_secs (field 10): coalesce Doppler OUT gaps into one burst
  - minimum_alert_secs (field 11): wall-clock persistence before mesh alert

Firmware PR (RootPathFinder/firmware-meshtastic#6) adds:
  - send_clear (field 12): optional cleared timing report after an alerted burst

Stock meshtastic Python bindings may not expose these fields yet.
"""

from __future__ import annotations

from typing import Any

from .protobuf_wire import (
    get_message_bool,
    get_message_uint32,
    set_message_bool_field,
    set_message_uint32_field,
)

# nanopb / protobuf field numbers on DetectionSensorConfig
MINIMUM_DETECT_SECS_FIELD = 9
BURST_GAP_SECS_FIELD = 10
MINIMUM_ALERT_SECS_FIELD = 11
SEND_CLEAR_FIELD = 12

_CUSTOM_UINT32_FIELDS: tuple[tuple[str, int], ...] = (
    ("minimum_detect_secs", MINIMUM_DETECT_SECS_FIELD),
    ("burst_gap_secs", BURST_GAP_SECS_FIELD),
    ("minimum_alert_secs", MINIMUM_ALERT_SECS_FIELD),
)

_CUSTOM_BOOL_FIELDS: tuple[tuple[str, int], ...] = (
    ("send_clear", SEND_CLEAR_FIELD),
)


def detection_sensor_to_dict(detection_sensor: Any) -> dict[str, Any]:
    """Convert a DetectionSensorConfig protobuf message to a JSON-friendly dict."""
    custom: dict[str, Any] = {}
    for name, field_no in _CUSTOM_UINT32_FIELDS:
        value = get_message_uint32(detection_sensor, field_no, name)
        custom[name] = int(value or 0)
    for name, field_no in _CUSTOM_BOOL_FIELDS:
        value = get_message_bool(detection_sensor, field_no, name)
        custom[name] = bool(value)

    return {
        "enabled": bool(detection_sensor.enabled),
        "minimum_broadcast_secs": int(detection_sensor.minimum_broadcast_secs),
        "state_broadcast_secs": int(detection_sensor.state_broadcast_secs),
        "send_bell": bool(detection_sensor.send_bell),
        "name": str(detection_sensor.name or ""),
        "monitor_pin": int(detection_sensor.monitor_pin),
        "detection_trigger_type": int(detection_sensor.detection_trigger_type),
        "use_pullup": bool(detection_sensor.use_pullup),
        **custom,
    }


def apply_detection_sensor_module_data(module_config: Any, module_data: dict[str, Any]) -> None:
    """Apply flat admin module_data onto ``module_config.detection_sensor``."""
    ds = module_config.detection_sensor
    custom_uint32 = dict(_CUSTOM_UINT32_FIELDS)
    custom_bool = dict(_CUSTOM_BOOL_FIELDS)
    for key, value in module_data.items():
        if key in custom_uint32:
            set_message_uint32_field(ds, custom_uint32[key], int(value), key)
            continue
        if key in custom_bool:
            set_message_bool_field(ds, custom_bool[key], bool(value), key)
            continue
        if hasattr(ds, key):
            setattr(ds, key, value)
