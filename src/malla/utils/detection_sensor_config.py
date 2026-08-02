"""Helpers for DetectionSensorConfig including custom firmware dwell field.

Firmware PR (minimum_detect_secs, field 9) requires pin dwell before a trip is
accepted. Stock meshtastic Python bindings may not expose the field yet.
"""

from __future__ import annotations

from typing import Any

from .protobuf_wire import get_message_uint32, set_message_uint32_field

# nanopb / protobuf field number on DetectionSensorConfig
MINIMUM_DETECT_SECS_FIELD = 9


def detection_sensor_to_dict(detection_sensor: Any) -> dict[str, Any]:
    """Convert a DetectionSensorConfig protobuf message to a JSON-friendly dict."""
    minimum_detect_secs = get_message_uint32(
        detection_sensor,
        MINIMUM_DETECT_SECS_FIELD,
        "minimum_detect_secs",
    )
    return {
        "enabled": bool(detection_sensor.enabled),
        "minimum_broadcast_secs": int(detection_sensor.minimum_broadcast_secs),
        "state_broadcast_secs": int(detection_sensor.state_broadcast_secs),
        "send_bell": bool(detection_sensor.send_bell),
        "name": str(detection_sensor.name or ""),
        "monitor_pin": int(detection_sensor.monitor_pin),
        "detection_trigger_type": int(detection_sensor.detection_trigger_type),
        "use_pullup": bool(detection_sensor.use_pullup),
        "minimum_detect_secs": int(minimum_detect_secs or 0),
    }


def apply_detection_sensor_module_data(module_config: Any, module_data: dict[str, Any]) -> None:
    """Apply flat admin module_data onto ``module_config.detection_sensor``."""
    ds = module_config.detection_sensor
    for key, value in module_data.items():
        if key == "minimum_detect_secs":
            set_message_uint32_field(
                ds,
                MINIMUM_DETECT_SECS_FIELD,
                int(value),
                "minimum_detect_secs",
            )
            continue
        if hasattr(ds, key):
            setattr(ds, key, value)
