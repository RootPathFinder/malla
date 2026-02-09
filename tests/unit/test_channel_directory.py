"""
Unit tests for the Channel Directory Repository.
"""

import os
import tempfile

import pytest

from malla.config import AppConfig
from malla.database.channel_directory_repository import (
    ChannelDirectoryRepository,
)


@pytest.fixture(autouse=True)
def _reset_table_flag():
    """Reset the module-level table-created flag between tests."""
    import malla.database.channel_directory_repository as mod

    mod._TABLE_CREATED = False
    yield
    mod._TABLE_CREATED = False


@pytest.fixture()
def _temp_db(monkeypatch):
    """Create a temporary database and point the config at it."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()

    cfg = AppConfig(database_file=tmp.name)
    monkeypatch.setattr("malla.database.connection.get_config", lambda: cfg)

    yield tmp.name

    try:
        os.unlink(tmp.name)
    except FileNotFoundError:
        pass


class TestChannelDirectoryRepository:
    """Tests for ChannelDirectoryRepository CRUD operations."""

    @pytest.mark.unit
    def test_add_channel_basic(self, _temp_db):
        result = ChannelDirectoryRepository.add_channel(
            channel_name="Dispatches",
            psk="AQ==",
            description="EMS/Fire dispatches",
        )
        assert result["success"] is True
        assert result["channel"]["channel_name"] == "Dispatches"
        assert result["channel"]["psk"] == "AQ=="
        assert result["channel"]["description"] == "EMS/Fire dispatches"
        assert result["channel"]["active"] is True

    @pytest.mark.unit
    def test_add_channel_with_registrant(self, _temp_db):
        result = ChannelDirectoryRepository.add_channel(
            channel_name="Weather",
            psk="AQ==",
            description="Weather alerts",
            registered_by_node_id=0x12345678,
            registered_by_name="TestNode",
        )
        assert result["success"] is True
        assert result["channel"]["registered_by_node_id"] == 0x12345678
        assert result["channel"]["registered_by_name"] == "TestNode"

    @pytest.mark.unit
    def test_add_duplicate_channel_fails(self, _temp_db):
        ChannelDirectoryRepository.add_channel(channel_name="Dispatches", psk="AQ==")
        result = ChannelDirectoryRepository.add_channel(
            channel_name="Dispatches", psk="BQ=="
        )
        assert result["success"] is False
        assert "already exists" in result["error"]

    @pytest.mark.unit
    def test_add_duplicate_case_insensitive(self, _temp_db):
        ChannelDirectoryRepository.add_channel(channel_name="Dispatches", psk="AQ==")
        result = ChannelDirectoryRepository.add_channel(
            channel_name="dispatches", psk="BQ=="
        )
        assert result["success"] is False

    @pytest.mark.unit
    def test_get_all_channels_empty(self, _temp_db):
        channels = ChannelDirectoryRepository.get_all_channels()
        assert channels == []

    @pytest.mark.unit
    def test_get_all_channels(self, _temp_db):
        ChannelDirectoryRepository.add_channel("Alpha", "AQ==", "Alpha channel")
        ChannelDirectoryRepository.add_channel("Beta", "BQ==", "Beta channel")

        channels = ChannelDirectoryRepository.get_all_channels()
        assert len(channels) == 2
        names = [ch["channel_name"] for ch in channels]
        assert "Alpha" in names
        assert "Beta" in names

    @pytest.mark.unit
    def test_get_all_channels_active_only(self, _temp_db):
        ChannelDirectoryRepository.add_channel("Active", "AQ==")
        ChannelDirectoryRepository.add_channel("Inactive", "BQ==")
        ChannelDirectoryRepository.update_channel("Inactive", active=False)

        active = ChannelDirectoryRepository.get_all_channels(active_only=True)
        assert len(active) == 1
        assert active[0]["channel_name"] == "Active"

        all_channels = ChannelDirectoryRepository.get_all_channels(active_only=False)
        assert len(all_channels) == 2

    @pytest.mark.unit
    def test_get_channel(self, _temp_db):
        ChannelDirectoryRepository.add_channel(
            "Dispatches", "AQ==", "EMS/Fire dispatches"
        )
        ch = ChannelDirectoryRepository.get_channel("Dispatches")

        assert ch is not None
        assert ch["channel_name"] == "Dispatches"
        assert ch["psk"] == "AQ=="
        assert ch["description"] == "EMS/Fire dispatches"

    @pytest.mark.unit
    def test_get_channel_case_insensitive(self, _temp_db):
        ChannelDirectoryRepository.add_channel("Dispatches", "AQ==")
        ch = ChannelDirectoryRepository.get_channel("dispatches")
        assert ch is not None
        assert ch["channel_name"] == "Dispatches"

    @pytest.mark.unit
    def test_get_channel_not_found(self, _temp_db):
        ch = ChannelDirectoryRepository.get_channel("NoSuchChannel")
        assert ch is None

    @pytest.mark.unit
    def test_update_channel_psk(self, _temp_db):
        ChannelDirectoryRepository.add_channel("Test", "AQ==")
        result = ChannelDirectoryRepository.update_channel("Test", psk="NewKey==")
        assert result["success"] is True

        ch = ChannelDirectoryRepository.get_channel("Test")
        assert ch["psk"] == "NewKey=="

    @pytest.mark.unit
    def test_update_channel_description(self, _temp_db):
        ChannelDirectoryRepository.add_channel("Test", "AQ==", "Old desc")
        ChannelDirectoryRepository.update_channel("Test", description="New desc")

        ch = ChannelDirectoryRepository.get_channel("Test")
        assert ch["description"] == "New desc"

    @pytest.mark.unit
    def test_update_channel_not_found(self, _temp_db):
        result = ChannelDirectoryRepository.update_channel("Missing", psk="AQ==")
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.unit
    def test_remove_channel(self, _temp_db):
        ChannelDirectoryRepository.add_channel("Temp", "AQ==")
        result = ChannelDirectoryRepository.remove_channel("Temp")
        assert result["success"] is True

        ch = ChannelDirectoryRepository.get_channel("Temp")
        assert ch is None

    @pytest.mark.unit
    def test_remove_channel_by_registrant(self, _temp_db):
        ChannelDirectoryRepository.add_channel(
            "MyCh", "AQ==", registered_by_node_id=0xAABBCCDD
        )
        # Wrong node – should fail
        result = ChannelDirectoryRepository.remove_channel(
            "MyCh", requester_node_id=0x11111111
        )
        assert result["success"] is False

        # Right node – should succeed
        result = ChannelDirectoryRepository.remove_channel(
            "MyCh", requester_node_id=0xAABBCCDD
        )
        assert result["success"] is True

    @pytest.mark.unit
    def test_remove_channel_not_found(self, _temp_db):
        result = ChannelDirectoryRepository.remove_channel("Ghost")
        assert result["success"] is False

    @pytest.mark.unit
    def test_get_channel_count(self, _temp_db):
        assert ChannelDirectoryRepository.get_channel_count() == 0

        ChannelDirectoryRepository.add_channel("A", "AQ==")
        ChannelDirectoryRepository.add_channel("B", "BQ==")
        assert ChannelDirectoryRepository.get_channel_count() == 2

        ChannelDirectoryRepository.update_channel("B", active=False)
        assert ChannelDirectoryRepository.get_channel_count(active_only=True) == 1
        assert ChannelDirectoryRepository.get_channel_count(active_only=False) == 2

    @pytest.mark.unit
    def test_channel_default_psk(self, _temp_db):
        result = ChannelDirectoryRepository.add_channel("NoPSK")
        assert result["success"] is True
        assert result["channel"]["psk"] == "AQ=="

    @pytest.mark.unit
    def test_channel_whitespace_stripped(self, _temp_db):
        result = ChannelDirectoryRepository.add_channel(
            "  Spaces  ", "  AQ==  ", "  desc  "
        )
        assert result["success"] is True
        assert result["channel"]["channel_name"] == "Spaces"
        assert result["channel"]["psk"] == "AQ=="

        ch = ChannelDirectoryRepository.get_channel("Spaces")
        assert ch is not None
