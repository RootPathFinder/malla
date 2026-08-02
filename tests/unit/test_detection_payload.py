"""Tests for DETECTION_SENSOR_APP payload parsing (dwell/burst/cleared)."""

from __future__ import annotations

import pytest

from malla.utils.detection_payload import (
    format_dwell_label,
    normalize_detection_sensor_name,
    parse_detection_payload,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            b"driveway detected",
            {
                "raw_text": "driveway detected",
                "sensor_name": "driveway",
                "event_kind": "trip",
                "dwell_ms": None,
                "burst_ms": None,
                "active_ms": None,
                "state": None,
            },
        ),
        (
            "driveway detected dwell_ms=2048",
            {
                "raw_text": "driveway detected dwell_ms=2048",
                "sensor_name": "driveway",
                "event_kind": "trip",
                "dwell_ms": 2048,
                "burst_ms": None,
                "active_ms": None,
                "state": None,
            },
        ),
        (
            "driveway detected burst_ms=8120",
            {
                "raw_text": "driveway detected burst_ms=8120",
                "sensor_name": "driveway",
                "event_kind": "trip",
                "dwell_ms": None,
                "burst_ms": 8120,
                "active_ms": None,
                "state": None,
            },
        ),
        (
            "driveway cleared active_ms=9400 burst_ms=8120",
            {
                "raw_text": "driveway cleared active_ms=9400 burst_ms=8120",
                "sensor_name": "driveway",
                "event_kind": "cleared",
                "dwell_ms": None,
                "burst_ms": 8120,
                "active_ms": 9400,
                "state": None,
            },
        ),
        (
            "gate state: 1",
            {
                "raw_text": "gate state: 1",
                "sensor_name": "gate",
                "event_kind": "state",
                "dwell_ms": None,
                "burst_ms": None,
                "active_ms": None,
                "state": 1,
            },
        ),
        (
            "custom text",
            {
                "raw_text": "custom text",
                "sensor_name": "custom text",
                "event_kind": "unknown",
                "dwell_ms": None,
                "burst_ms": None,
                "active_ms": None,
                "state": None,
            },
        ),
        (
            None,
            {
                "raw_text": None,
                "sensor_name": None,
                "event_kind": "unknown",
                "dwell_ms": None,
                "burst_ms": None,
                "active_ms": None,
                "state": None,
            },
        ),
    ],
)
def test_parse_detection_payload(raw, expected):
    assert parse_detection_payload(raw) == expected


@pytest.mark.unit
def test_parse_strips_trailing_nul_bytes():
    # Firmware C-string payloads often include a trailing NUL, which used to
    # break the `… detected` match and show a junk character in the UI.
    parsed = parse_detection_payload(b"Driveway detected\x00")
    assert parsed["event_kind"] == "trip"
    assert parsed["sensor_name"] == "Driveway"
    assert parsed["raw_text"] == "Driveway detected"

    parsed_dwell = parse_detection_payload(b"Driveway detected dwell_ms=2048\x00\x00")
    assert parsed_dwell["sensor_name"] == "Driveway"
    assert parsed_dwell["dwell_ms"] == 2048

    parsed_clear = parse_detection_payload(
        b"Driveway cleared active_ms=1200 burst_ms=900\x00"
    )
    assert parsed_clear["event_kind"] == "cleared"
    assert parsed_clear["sensor_name"] == "Driveway"
    assert parsed_clear["active_ms"] == 1200
    assert parsed_clear["burst_ms"] == 900


@pytest.mark.unit
def test_normalize_detection_sensor_name():
    assert normalize_detection_sensor_name("driveway") == "driveway"
    assert normalize_detection_sensor_name("driveway detected") == "driveway"
    assert (
        normalize_detection_sensor_name("driveway detected dwell_ms=2000") == "driveway"
    )
    assert (
        normalize_detection_sensor_name("driveway detected burst_ms=8000") == "driveway"
    )
    assert (
        normalize_detection_sensor_name("driveway cleared active_ms=9 burst_ms=8")
        == "driveway"
    )
    assert normalize_detection_sensor_name("*") == "*"


@pytest.mark.unit
def test_format_dwell_label():
    assert format_dwell_label(250) == "250ms"
    assert format_dwell_label(2000) == "2s"
    assert format_dwell_label(2500) == "2.5s"
    assert format_dwell_label(None) is None
