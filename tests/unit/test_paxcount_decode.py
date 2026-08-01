"""Unit tests for Paxcounter MAC/BSSID sightings decode."""

from __future__ import annotations

import pytest

from malla.utils.paxcount_decode import (
    aggregate_mac_hits,
    decode_paxcount_payload,
    format_mac,
)
from malla.vendor.meshtastic import paxcount_pb2


def _encode_sample(
    *,
    wifi: int = 3,
    ble: int = 1,
    uptime: int = 99,
    sightings: list[tuple[bytes, int, int]] | None = None,
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
    for mac, kind, rssi in sightings or []:
        s = msg.sightings.add()
        s.mac = mac
        s.kind = kind
        s.rssi = rssi
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
def test_decode_sightings_client_and_ap():
    raw = _encode_sample(
        wifi=2,
        ble=0,
        sightings=[
            (bytes([1, 2, 3, 4, 5, 6]), paxcount_pb2.PaxSighting.WIFI_CLIENT, -60),
            (bytes([10, 11, 12, 13, 14, 15]), paxcount_pb2.PaxSighting.WIFI_AP, -45),
        ],
        sighting_count=2,
        chunk_index=0,
        chunk_total=1,
    )
    decoded = decode_paxcount_payload(raw)
    assert decoded is not None
    assert decoded["wifi"] == 2
    assert decoded["sighting_count"] == 2
    assert len(decoded["sightings"]) == 2
    assert decoded["sightings"][0]["mac"] == "01:02:03:04:05:06"
    assert decoded["sightings"][0]["kind"] == "wifi_client"
    assert decoded["sightings"][0]["rssi"] == -60
    assert decoded["sightings"][1]["kind"] == "wifi_ap"
    assert decoded["sightings"][1]["mac"] == "0a:0b:0c:0d:0e:0f"


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
            ],
        },
        {
            "timestamp": 2000.0,
            "from_node_hex": "!aabbccdd",
            "sightings": [
                {"mac": "01:02:03:04:05:06", "kind": "wifi_client", "rssi": -55},
            ],
        },
    ]
    hits = aggregate_mac_hits(readings)
    assert len(hits) == 2
    assert hits[0]["mac"] == "01:02:03:04:05:06"
    assert hits[0]["hits"] == 2
    assert hits[0]["best_rssi"] == -55
    assert hits[0]["last_seen"] == 2000.0
    assert hits[1]["mac"] == "aa:bb:cc:dd:ee:ff"
    assert hits[1]["hits"] == 1
    assert hits[1]["kind"] == "wifi_ap"
