"""Tests for Detection Sensor dwell (minimum_detect_secs) admin support."""

from __future__ import annotations

import pytest
from meshtastic.protobuf import module_config_pb2

from malla.services.config_metadata import get_module_config_schema
from malla.utils.detection_sensor_config import (
    MINIMUM_DETECT_SECS_FIELD,
    apply_detection_sensor_module_data,
    detection_sensor_to_dict,
)
from malla.utils.protobuf_wire import (
    encode_uint32_field,
    read_uint32_field,
    strip_field,
)


@pytest.mark.unit
def test_schema_includes_minimum_detect_secs():
    schema = get_module_config_schema("detectionsensor")
    names = [f["name"] for f in schema]
    assert "minimum_detect_secs" in names
    field = next(f for f in schema if f["name"] == "minimum_detect_secs")
    assert field["type"] == "number"
    assert field["unit"] == "seconds"
    assert field["min"] == 0


@pytest.mark.unit
def test_wire_encode_decode_uint32_field():
    raw = encode_uint32_field(MINIMUM_DETECT_SECS_FIELD, 2)
    assert read_uint32_field(raw, MINIMUM_DETECT_SECS_FIELD) == 2
    stripped = strip_field(raw + encode_uint32_field(1, 1), MINIMUM_DETECT_SECS_FIELD)
    assert read_uint32_field(stripped, MINIMUM_DETECT_SECS_FIELD) is None
    assert read_uint32_field(stripped, 1) == 1


@pytest.mark.unit
def test_apply_and_read_minimum_detect_secs_unknown_field():
    mc = module_config_pb2.ModuleConfig()
    apply_detection_sensor_module_data(
        mc,
        {
            "enabled": True,
            "minimum_broadcast_secs": 60,
            "state_broadcast_secs": 0,
            "name": "driveway",
            "monitor_pin": 21,
            "detection_trigger_type": 1,
            "use_pullup": False,
            "minimum_detect_secs": 2,
        },
    )
    # Field must be present on the wire even if stock bindings lack the attr.
    raw = mc.detection_sensor.SerializeToString()
    assert read_uint32_field(raw, MINIMUM_DETECT_SECS_FIELD) == 2

    # Round-trip through ModuleConfig serialization (as AdminMessage would).
    mc2 = module_config_pb2.ModuleConfig()
    mc2.ParseFromString(mc.SerializeToString())
    parsed = detection_sensor_to_dict(mc2.detection_sensor)
    assert parsed["enabled"] is True
    assert parsed["minimum_broadcast_secs"] == 60
    assert parsed["name"] == "driveway"
    assert parsed["minimum_detect_secs"] == 2


@pytest.mark.unit
def test_detection_sensor_to_dict_defaults_dwell_zero():
    ds = module_config_pb2.ModuleConfig.DetectionSensorConfig(
        enabled=True,
        minimum_broadcast_secs=30,
    )
    parsed = detection_sensor_to_dict(ds)
    assert parsed["minimum_detect_secs"] == 0
    assert parsed["enabled"] is True
