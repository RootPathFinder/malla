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


@pytest.fixture()
def _temp_db(monkeypatch):
    """Create a temporary database and point the config at it.

    Uses environment variable override for complete database isolation
    during parallel test execution.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()

    # Set environment variable - this takes priority in get_db_connection()
    monkeypatch.setenv("MALLA_DATABASE_FILE", tmp.name)

    # Also patch config for any code paths that check it directly
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


class TestChannelUrlGeneration:
    """Tests for Meshtastic channel URL generation."""

    @pytest.mark.unit
    def test_generate_channel_url_basic(self):
        from malla.utils.channel_url import generate_channel_url

        url = generate_channel_url("TestChan", "AQ==")
        assert url is not None
        assert url.startswith("https://meshtastic.org/e/#")
        assert len(url) > len("https://meshtastic.org/e/#")

    @pytest.mark.unit
    def test_generate_channel_url_default_psk(self):
        from malla.utils.channel_url import generate_channel_url

        url = generate_channel_url("MyChan")
        assert url is not None
        assert "meshtastic.org/e/#" in url

    @pytest.mark.unit
    def test_generate_channel_url_different_channels_differ(self):
        from malla.utils.channel_url import generate_channel_url

        url1 = generate_channel_url("Alpha", "AQ==")
        url2 = generate_channel_url("Beta", "AQ==")
        assert url1 != url2

    @pytest.mark.unit
    def test_generate_channel_url_different_psk_differ(self):
        from malla.utils.channel_url import generate_channel_url

        url1 = generate_channel_url("Same", "AQ==")
        url2 = generate_channel_url("Same", "BQ==")
        assert url1 != url2

    @pytest.mark.unit
    def test_generate_channel_url_preserves_primary_channel(self):
        """URL encodes the shared channel at index 1 (secondary) with
        the default primary channel at index 0 so tapping the URL on
        iOS/Android does not wipe existing channels."""
        import base64

        from meshtastic.protobuf import apponly_pb2

        from malla.utils.channel_url import generate_channel_url

        url = generate_channel_url("TestChan", "AQ==")
        assert url is not None

        # Decode the protobuf from the URL fragment
        fragment = url.split("#", 1)[1]
        # Re-pad base64url
        padded = fragment + "=" * (-len(fragment) % 4)
        proto_bytes = base64.urlsafe_b64decode(padded)

        channel_set = apponly_pb2.ChannelSet()
        channel_set.ParseFromString(proto_bytes)

        # Must have 2 settings entries: primary (index 0) + shared (index 1)
        assert len(channel_set.settings) == 2

        # settings[0] should be the default primary channel (empty name,
        # default PSK 0x01)
        primary = channel_set.settings[0]
        assert primary.name == ""
        assert primary.psk == b"\x01"

        # settings[1] should be the shared channel
        secondary = channel_set.settings[1]
        assert secondary.name == "TestChan"
        assert secondary.psk == base64.b64decode("AQ==")

        # lora_config should not be present
        assert not channel_set.HasField("lora_config")

    @pytest.mark.unit
    def test_generate_channel_url_custom_slot(self):
        """Specifying channel_index=3 puts the channel at settings[3]
        with placeholder entries at settings[0]-[2]."""
        import base64

        from meshtastic.protobuf import apponly_pb2

        from malla.utils.channel_url import generate_channel_url

        url = generate_channel_url("MyChan", "AQ==", channel_index=3)
        assert url is not None

        fragment = url.split("#", 1)[1]
        padded = fragment + "=" * (-len(fragment) % 4)
        proto_bytes = base64.urlsafe_b64decode(padded)

        channel_set = apponly_pb2.ChannelSet()
        channel_set.ParseFromString(proto_bytes)

        # 4 entries: slots 0, 1, 2 (placeholders) + slot 3 (target)
        assert len(channel_set.settings) == 4

        # Placeholders should have default PSK and no name
        for i in range(3):
            assert channel_set.settings[i].name == ""
            assert channel_set.settings[i].psk == b"\x01"

        # Slot 3 is the actual channel
        assert channel_set.settings[3].name == "MyChan"
        assert channel_set.settings[3].psk == base64.b64decode("AQ==")

    @pytest.mark.unit
    def test_generate_channel_url_slot_zero_replaces_primary(self):
        """channel_index=0 puts the channel directly at slot 0 with
        no extra placeholder entries."""
        import base64

        from meshtastic.protobuf import apponly_pb2

        from malla.utils.channel_url import generate_channel_url

        url = generate_channel_url("Primary", "AQ==", channel_index=0)
        assert url is not None

        fragment = url.split("#", 1)[1]
        padded = fragment + "=" * (-len(fragment) % 4)
        proto_bytes = base64.urlsafe_b64decode(padded)

        channel_set = apponly_pb2.ChannelSet()
        channel_set.ParseFromString(proto_bytes)

        assert len(channel_set.settings) == 1
        assert channel_set.settings[0].name == "Primary"

    @pytest.mark.unit
    def test_generate_channel_url_invalid_slot_returns_none(self):
        """Out of range channel_index (8, -1) returns None."""
        from malla.utils.channel_url import generate_channel_url

        assert generate_channel_url("X", "AQ==", channel_index=8) is None
        assert generate_channel_url("X", "AQ==", channel_index=-1) is None
