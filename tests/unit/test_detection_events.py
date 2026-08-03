"""Tests for joining trip + cleared detection packets into one event."""

from __future__ import annotations

import pytest

from malla.utils.detection_events import join_detection_events, join_detection_readings


def _ev(
    *,
    id: int,
    kind: str,
    ts: float,
    name: str = "Driveway",
    node: int = 0xD4000003,
    active_ms: int | None = None,
    burst_ms: int | None = None,
) -> dict:
    return {
        "id": id,
        "event_kind": kind,
        "timestamp": ts,
        "timestamp_iso": f"ts-{ts}",
        "from_node_id": node,
        "detection_name": name,
        "detection_text": f"{name} {kind}",
        "active_ms": active_ms,
        "burst_ms": burst_ms,
        "dwell_ms": None,
        "state": None,
    }


@pytest.mark.unit
def test_joins_trip_and_cleared_into_one_complete_event():
    trip = _ev(id=1, kind="trip", ts=1000.0)
    cleared = _ev(id=2, kind="cleared", ts=1005.0, active_ms=2728, burst_ms=5728)
    # Newest-first input like the API
    out = join_detection_events([cleared, trip])
    assert len(out) == 1
    ev = out[0]
    assert ev["event_kind"] == "complete"
    assert ev["active_ms"] == 2728
    assert ev["burst_ms"] == 5728
    assert ev["trip_id"] == 1
    assert ev["clear_id"] == 2
    assert ev["started_at"] == 1000.0
    assert ev["timestamp"] == 1005.0


@pytest.mark.unit
def test_keeps_unpaired_trip_as_in_progress():
    trip = _ev(id=10, kind="trip", ts=50.0)
    out = join_detection_events([trip])
    assert len(out) == 1
    assert out[0]["event_kind"] == "trip"
    assert out[0]["id"] == 10


@pytest.mark.unit
def test_does_not_pair_across_sensors_or_nodes():
    trip_a = _ev(id=1, kind="trip", ts=1.0, name="Driveway", node=1)
    clear_b = _ev(id=2, kind="cleared", ts=2.0, name="Gate", node=1, active_ms=1, burst_ms=1)
    clear_a = _ev(id=3, kind="cleared", ts=3.0, name="Driveway", node=1, active_ms=9, burst_ms=9)
    out = join_detection_events([clear_a, clear_b, trip_a])
    kinds = [(e["event_kind"], e.get("detection_name"), e.get("active_ms")) for e in out]
    assert ("complete", "Driveway", 9) in kinds
    assert ("complete", "Gate", 1) in kinds  # orphan clear still one finished row
    assert len(out) == 2


@pytest.mark.unit
def test_passes_through_state_heartbeats():
    trip = _ev(id=1, kind="trip", ts=1.0)
    state = _ev(id=2, kind="state", ts=2.0)
    state["state"] = 1
    cleared = _ev(id=3, kind="cleared", ts=3.0, active_ms=100, burst_ms=200)
    out = join_detection_events([cleared, state, trip])
    assert [e["event_kind"] for e in out] == ["complete", "state"]


@pytest.mark.unit
def test_rejects_pairing_when_gap_too_large():
    trip = _ev(id=1, kind="trip", ts=1.0)
    cleared = _ev(id=2, kind="cleared", ts=1000.0, active_ms=1, burst_ms=1)
    out = join_detection_events([cleared, trip], max_pair_secs=120.0)
    kinds = {e["event_kind"] for e in out}
    assert "trip" in kinds
    assert "complete" in kinds  # orphan clear
    assert len(out) == 2


@pytest.mark.unit
def test_join_detection_readings_merges_pairs_keeps_other_types():
    trip = {
        "id": 1,
        "timestamp": 1000.0,
        "from_node_id": 1,
        "sensor_type": "detection",
        "data": {
            "detection_name": "Driveway",
            "event_kind": "trip",
            "burst_ms": 2000,
            "active_ms": None,
            "dwell_ms": None,
            "state": None,
        },
    }
    cleared = {
        "id": 2,
        "timestamp": 1005.0,
        "from_node_id": 1,
        "sensor_type": "detection",
        "data": {
            "detection_name": "Driveway",
            "event_kind": "cleared",
            "burst_ms": 5000,
            "active_ms": 2700,
            "dwell_ms": None,
            "state": None,
        },
    }
    env = {
        "id": 3,
        "timestamp": 1006.0,
        "from_node_id": 2,
        "sensor_type": "environment",
        "data": {"temperature": 20.0},
    }
    out = join_detection_readings([env, cleared, trip])
    assert len(out) == 2
    assert out[0]["sensor_type"] == "environment"
    det = out[1]
    assert det["sensor_type"] == "detection"
    assert det["data"]["event_kind"] == "complete"
    assert det["data"]["active_ms"] == 2700
    assert det["data"]["burst_ms"] == 5000
    assert det["data"]["trip_id"] == 1
    assert det["data"]["clear_id"] == 2
