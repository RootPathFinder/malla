"""Unit tests for PAX ID profile repository and directory helpers."""

from __future__ import annotations

import os
import tempfile

import pytest

from malla.config import AppConfig
from malla.database.pax_profile_repository import (
    PaxProfileRepository,
    init_pax_profile_tables,
)
from malla.utils.paxcount_decode import aggregate_mac_hits, build_id_directory


@pytest.fixture()
def _temp_db(monkeypatch):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    monkeypatch.setenv("MALLA_DATABASE_FILE", tmp.name)
    cfg = AppConfig(database_file=tmp.name)
    monkeypatch.setattr("malla.database.connection.get_config", lambda: cfg)
    init_pax_profile_tables()
    yield tmp.name
    try:
        os.unlink(tmp.name)
    except FileNotFoundError:
        pass


@pytest.mark.unit
class TestPaxProfileRepository:
    def test_upsert_get_and_list(self, _temp_db):
        result = PaxProfileRepository.upsert_profile(
            "AA:BB:CC:DD:EE:FF",
            nickname="Dave phone",
            notes="iPhone",
        )
        assert result["success"] is True
        assert result["profile"]["mac"] == "aa:bb:cc:dd:ee:ff"
        assert result["profile"]["nickname"] == "Dave phone"

        got = PaxProfileRepository.get_profile("aabbccddeeff")
        assert got is not None
        assert got["nickname"] == "Dave phone"

        listed = PaxProfileRepository.list_profiles(q="dave")
        assert len(listed) == 1
        assert listed[0]["mac"] == "aa:bb:cc:dd:ee:ff"

    def test_empty_nickname_deletes_profile(self, _temp_db):
        PaxProfileRepository.upsert_profile("01:02:03:04:05:06", nickname="Temp")
        result = PaxProfileRepository.upsert_profile(
            "01:02:03:04:05:06", nickname="", notes=""
        )
        assert result["success"] is True
        assert result["deleted"] is True
        assert PaxProfileRepository.get_profile("01:02:03:04:05:06") is None

    def test_delete_profile(self, _temp_db):
        PaxProfileRepository.upsert_profile("de:ad:be:ef:00:01", nickname="BLE tag")
        assert PaxProfileRepository.delete_profile("deadbeef0001") is True
        assert PaxProfileRepository.get_profile("de:ad:be:ef:00:01") is None

    def test_get_profiles_by_macs(self, _temp_db):
        PaxProfileRepository.upsert_profile("11:22:33:44:55:66", nickname="A")
        PaxProfileRepository.upsert_profile("aa:bb:cc:dd:ee:ff", nickname="B")
        mapping = PaxProfileRepository.get_profiles_by_macs(
            ["11:22:33:44:55:66", "00:00:00:00:00:00"]
        )
        assert "11:22:33:44:55:66" in mapping
        assert "aa:bb:cc:dd:ee:ff" not in mapping
        assert mapping["11:22:33:44:55:66"]["nickname"] == "A"


@pytest.mark.unit
class TestIdDirectory:
    def test_build_id_directory_merges_kinds_and_profiles(self):
        hits = aggregate_mac_hits(
            [
                {
                    "timestamp": 1000.0,
                    "from_node_hex": "!aabbccdd",
                    "sightings": [
                        {"mac": "01:02:03:04:05:06", "kind": "wifi_client", "rssi": -70},
                        {"mac": "01:02:03:04:05:06", "kind": "ble", "rssi": -60},
                    ],
                },
                {
                    "timestamp": 2000.0,
                    "from_node_hex": "!aabbccdd",
                    "sightings": [
                        {"mac": "01:02:03:04:05:06", "kind": "wifi_client", "rssi": -50},
                    ],
                },
            ]
        )
        profiles = {
            "01:02:03:04:05:06": {
                "mac": "01:02:03:04:05:06",
                "nickname": "Shared radio",
                "notes": "wifi+ble",
                "updated_at": 1.0,
            }
        }
        directory = build_id_directory(hits, profiles)
        assert len(directory) == 1
        row = directory[0]
        assert row["mac"] == "01:02:03:04:05:06"
        assert row["hits"] == 3
        assert set(row["kinds"]) == {"wifi_client", "ble"}
        assert row["kind_hits"]["wifi_client"] == 2
        assert row["kind_hits"]["ble"] == 1
        assert row["best_rssi"] == -50
        assert row["first_seen"] == 1000.0
        assert row["last_seen"] == 2000.0
        assert row["nickname"] == "Shared radio"
        assert row["has_profile"] is True
