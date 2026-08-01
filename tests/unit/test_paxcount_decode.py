"""Unit tests for Paxcounter MAC/BSSID sightings decode."""

from __future__ import annotations

import pytest

from malla.utils.paxcount_decode import (
    aggregate_mac_hits,
    ble_distance_band,
    build_id_directory,
    build_sighting_history,
    decode_paxcount_payload,
    estimate_ble_distance_m,
    format_ble_distance,
    format_fingerprint,
    format_mac,
    normalize_profile_id,
    sighting_matches_profile,
    stable_sighting_id,
)
from malla.vendor.meshtastic import paxcount_pb2


def _encode_sample(
    *,
    wifi: int = 3,
    ble: int = 1,
    uptime: int = 99,
    sightings: list[tuple] | None = None,
    sighting_count: int | None = None,
    chunk_index: int = 0,
    chunk_total: int = 1,
) -> bytes:
    msg = paxcount_pb2.Paxcount(
        wifi=wifi,
        ble=ble,
        uptime=uptime,
        chunk_index=chunk_index,
        chunk_total=chunk_total,
    )
    for item in sightings or []:
        if len(item) == 3:
            mac, kind, rssi = item
            fingerprint = b""
        else:
            mac, kind, rssi, fingerprint = item
        s = msg.sightings.add()
        s.mac = mac
        s.kind = kind
        s.rssi = rssi
        if fingerprint:
            s.fingerprint = fingerprint
    msg.sighting_count = (
        len(msg.sightings) if sighting_count is None else sighting_count
    )
    return msg.SerializeToString()


@pytest.mark.unit
def test_format_mac_bytes_and_string():
    assert format_mac(bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF])) == "aa:bb:cc:dd:ee:ff"
    assert format_mac("AABBCCDDEEFF") == "aa:bb:cc:dd:ee:ff"
    assert format_mac("aa:bb:cc:dd:ee:ff") == "aa:bb:cc:dd:ee:ff"


@pytest.mark.unit
def test_format_fingerprint_and_stable_id():
    assert format_fingerprint(bytes([0xAB, 0xCD])) == "abcd"
    assert format_fingerprint("fp:abcd") == "abcd"
    assert format_fingerprint(b"") is None
    assert stable_sighting_id("aa:bb:cc:dd:ee:ff", "abcd") == "fp:abcd"
    assert stable_sighting_id("aa:bb:cc:dd:ee:ff", None) == "aa:bb:cc:dd:ee:ff"


@pytest.mark.unit
def test_normalize_profile_id_mac_and_fingerprint():
    assert normalize_profile_id("AA:BB:CC:DD:EE:FF") == "aa:bb:cc:dd:ee:ff"
    assert normalize_profile_id("fp:ABCD") == "fp:abcd"
    assert normalize_profile_id("abcd") == "fp:abcd"
    assert normalize_profile_id("aabbccddeeff") == "aa:bb:cc:dd:ee:ff"
    assert normalize_profile_id("not-a-mac") is None


@pytest.mark.unit
def test_decode_legacy_counts_only():
    raw = _encode_sample(wifi=12, ble=4, uptime=3600, sightings=[])
    decoded = decode_paxcount_payload(raw)
    assert decoded is not None
    assert decoded["wifi"] == 12
    assert decoded["ble"] == 4
    assert decoded["uptime"] == 3600
    assert decoded["sightings"] == []
    assert decoded["sighting_count"] == 0


@pytest.mark.unit
def test_decode_sightings_client_ap_and_ble():
    raw = _encode_sample(
        wifi=2,
        ble=1,
        sightings=[
            (bytes([1, 2, 3, 4, 5, 6]), paxcount_pb2.PaxSighting.WIFI_CLIENT, -60),
            (bytes([10, 11, 12, 13, 14, 15]), paxcount_pb2.PaxSighting.WIFI_AP, -45),
            (bytes([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]), paxcount_pb2.PaxSighting.BLE, -72),
        ],
        sighting_count=3,
        chunk_index=0,
        chunk_total=1,
    )
    decoded = decode_paxcount_payload(raw)
    assert decoded is not None
    assert decoded["wifi"] == 2
    assert decoded["ble"] == 1
    assert decoded["sighting_count"] == 3
    assert len(decoded["sightings"]) == 3
    assert decoded["sightings"][0]["mac"] == "01:02:03:04:05:06"
    assert decoded["sightings"][0]["kind"] == "wifi_client"
    assert decoded["sightings"][0]["rssi"] == -60
    assert decoded["sightings"][1]["kind"] == "wifi_ap"
    assert decoded["sightings"][1]["mac"] == "0a:0b:0c:0d:0e:0f"
    assert decoded["sightings"][2]["kind"] == "ble"
    assert decoded["sightings"][2]["mac"] == "de:ad:be:ef:00:01"
    assert decoded["sightings"][2]["rssi"] == -72
    assert decoded["sightings"][2]["fingerprint"] is None
    assert decoded["sightings"][2]["stable_id"] == "de:ad:be:ef:00:01"


@pytest.mark.unit
def test_decode_ble_apple_android_with_fingerprint():
    raw = _encode_sample(
        wifi=0,
        ble=2,
        sightings=[
            (
                bytes([0x11, 0x22, 0x33, 0x44, 0x55, 0x66]),
                paxcount_pb2.PaxSighting.BLE_APPLE,
                -68,
                bytes([0xDE, 0xAD, 0xBE, 0xEF]),
            ),
            (
                bytes([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]),
                paxcount_pb2.PaxSighting.BLE_ANDROID,
                -75,
                bytes([0x12, 0x34]),
            ),
        ],
    )
    decoded = decode_paxcount_payload(raw)
    assert decoded is not None
    assert decoded["sightings"][0]["kind"] == "ble_apple"
    assert decoded["sightings"][0]["fingerprint"] == "deadbeef"
    assert decoded["sightings"][0]["stable_id"] == "fp:deadbeef"
    assert decoded["sightings"][0]["distance_m"] is not None
    assert decoded["sightings"][0]["distance_label"]
    assert decoded["sightings"][1]["kind"] == "ble_android"
    assert decoded["sightings"][1]["fingerprint"] == "1234"
    assert decoded["sightings"][1]["stable_id"] == "fp:1234"
    # Stronger RSSI → nearer estimate
    assert decoded["sightings"][0]["distance_m"] < decoded["sightings"][1]["distance_m"]


@pytest.mark.unit
def test_estimate_ble_distance_from_rssi():
    assert estimate_ble_distance_m(None) is None
    assert estimate_ble_distance_m(10) is None
    near = estimate_ble_distance_m(-45)
    mid = estimate_ble_distance_m(-65)
    far = estimate_ble_distance_m(-85)
    assert near is not None and mid is not None and far is not None
    assert near < mid < far
    # ~1 m at calibrated measured power
    one_m = estimate_ble_distance_m(-59)
    assert one_m is not None
    assert 0.8 <= one_m <= 1.2
    assert ble_distance_band(0.5) == "immediate"
    assert ble_distance_band(2.0) == "near"
    assert ble_distance_band(6.0) == "mid"
    assert ble_distance_band(20.0) == "far"
    assert format_ble_distance(1.2) == "~1.2 m"
    assert format_ble_distance(15.0) == "~15 m"


@pytest.mark.unit
def test_decode_empty_or_invalid_returns_none():
    assert decode_paxcount_payload(None) is None
    assert decode_paxcount_payload(b"") is None


@pytest.mark.unit
def test_aggregate_mac_hits_counts_unique():
    readings = [
        {
            "timestamp": 1000.0,
            "from_node_hex": "!aabbccdd",
            "sightings": [
                {"mac": "01:02:03:04:05:06", "kind": "wifi_client", "rssi": -70},
                {"mac": "aa:bb:cc:dd:ee:ff", "kind": "wifi_ap", "rssi": -50},
                {"mac": "de:ad:be:ef:00:01", "kind": "ble", "rssi": -80},
            ],
        },
        {
            "timestamp": 2000.0,
            "from_node_hex": "!aabbccdd",
            "sightings": [
                {"mac": "01:02:03:04:05:06", "kind": "wifi_client", "rssi": -55},
                {"mac": "de:ad:be:ef:00:01", "kind": "ble", "rssi": -65},
            ],
        },
    ]
    hits = aggregate_mac_hits(readings)
    assert len(hits) == 3
    assert hits[0]["mac"] == "01:02:03:04:05:06"
    assert hits[0]["hits"] == 2
    assert hits[0]["best_rssi"] == -55
    assert hits[0]["last_seen"] == 2000.0
    ble = next(h for h in hits if h["kind"] == "ble")
    assert ble["mac"] == "de:ad:be:ef:00:01"
    assert ble["hits"] == 2
    assert ble["best_rssi"] == -65
    ap = next(h for h in hits if h["kind"] == "wifi_ap")
    assert ap["mac"] == "aa:bb:cc:dd:ee:ff"
    assert ap["hits"] == 1


@pytest.mark.unit
def test_build_id_directory_groups_rotating_macs_by_fingerprint():
    hits = aggregate_mac_hits(
        [
            {
                "timestamp": 1000.0,
                "from_node_hex": "!aabbccdd",
                "sightings": [
                    {
                        "mac": "11:22:33:44:55:66",
                        "kind": "ble_apple",
                        "rssi": -70,
                        "fingerprint": "abcd",
                        "stable_id": "fp:abcd",
                    },
                ],
            },
            {
                "timestamp": 2000.0,
                "from_node_hex": "!aabbccdd",
                "sightings": [
                    {
                        "mac": "aa:bb:cc:dd:ee:ff",
                        "kind": "ble_apple",
                        "rssi": -60,
                        "fingerprint": "abcd",
                        "stable_id": "fp:abcd",
                    },
                ],
            },
        ]
    )
    profiles = {
        "fp:abcd": {
            "mac": "fp:abcd",
            "nickname": "Dave iPhone",
            "notes": "rotating",
            "updated_at": 1.0,
        }
    }
    directory = build_id_directory(hits, profiles)
    assert len(directory) == 1
    row = directory[0]
    assert row["id"] == "fp:abcd"
    assert row["fingerprint"] == "abcd"
    assert row["hits"] == 2
    assert set(row["macs"]) == {"11:22:33:44:55:66", "aa:bb:cc:dd:ee:ff"}
    assert row["nickname"] == "Dave iPhone"
    assert row["has_profile"] is True
    assert row["best_rssi"] == -60


@pytest.mark.unit
def test_sighting_matches_profile_fingerprint_and_mac():
    assert sighting_matches_profile(
        {"mac": "11:22:33:44:55:66", "fingerprint": "abcd", "stable_id": "fp:abcd"},
        "fp:abcd",
    )
    assert sighting_matches_profile(
        {"mac": "aa:bb:cc:dd:ee:ff", "fingerprint": "abcd"},
        "fp:abcd",
    )
    assert sighting_matches_profile(
        {"mac": "aa:bb:cc:dd:ee:ff", "stable_id": "aa:bb:cc:dd:ee:ff"},
        "AA:BB:CC:DD:EE:FF",
    )
    assert not sighting_matches_profile(
        {"mac": "aa:bb:cc:dd:ee:ff", "fingerprint": "1234"},
        "fp:abcd",
    )


@pytest.mark.unit
def test_build_sighting_history_rssi_and_presence_buckets():
    now = 1_700_000_000.0
    samples = [
        {"timestamp": now - 3500, "rssi": -80, "kind": "ble_apple", "mac": "11:22:33:44:55:66", "fingerprint": "abcd"},
        {"timestamp": now - 3400, "rssi": -60, "kind": "ble_apple", "mac": "aa:bb:cc:dd:ee:ff", "fingerprint": "abcd"},
        {"timestamp": now - 100, "rssi": -55, "kind": "ble_apple", "mac": "11:22:33:44:55:66", "fingerprint": "abcd"},
    ]
    history = build_sighting_history(
        samples,
        hours=1,
        bucket_minutes=15,
        now=now,
        present_within_seconds=900,
    )
    assert history["summary"]["hit_count"] == 3
    assert history["summary"]["best_rssi"] == -55
    assert history["summary"]["avg_rssi"] == round((-80 + -60 + -55) / 3, 1)
    assert history["summary"]["present_now"] is True
    assert history["summary"]["present_buckets"] >= 2
    assert len(history["presence"]) == 4  # 60 / 15
    assert len(history["samples"]) == 3
    # Samples sorted ascending
    assert history["samples"][0]["timestamp"] <= history["samples"][-1]["timestamp"]
    assert history["summary"]["nearest_m"] is not None
    assert history["summary"]["nearest_label"]
    assert history["samples"][-1]["distance_m"] is not None
