"""Unit tests for mesh bot settings persistence and quieter channel ads."""

import os
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.malla.config import AppConfig
from src.malla.database.bot_settings_repository import BotSettingsRepository
from src.malla.services.bot_service import BotService


@pytest.fixture()
def temp_db(monkeypatch):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    monkeypatch.setenv("MALLA_DATABASE_FILE", tmp.name)
    cfg = AppConfig(database_file=tmp.name)
    monkeypatch.setattr("malla.database.connection.get_config", lambda: cfg)
    monkeypatch.setattr(
        "src.malla.database.connection.get_config", lambda: cfg
    )
    yield tmp.name
    try:
        os.unlink(tmp.name)
    except FileNotFoundError:
        pass


@pytest.fixture()
def bot_service(temp_db) -> BotService:
    BotService._instance = None
    service = BotService()
    service._enabled = True
    return service


class TestBotSettingsRepository:
    @pytest.mark.unit
    def test_set_and_get_roundtrip(self, temp_db):
        BotSettingsRepository.set_many(
            {
                "command_prefix": "?",
                "daily_digest_hour": 7,
                "disabled_commands": ["uptime", "time", "busy"],
                "wait_for_jobs": False,
            }
        )
        all_settings = BotSettingsRepository.get_all()
        assert all_settings["command_prefix"] == "?"
        assert all_settings["daily_digest_hour"] == 7
        assert all_settings["disabled_commands"] == ["uptime", "time", "busy"]
        assert all_settings["wait_for_jobs"] is False
        assert BotSettingsRepository.get("daily_digest_hour") == 7


class TestBotSettingsPersistence:
    @pytest.mark.unit
    def test_settings_survive_service_reinit(self, temp_db, bot_service: BotService):
        bot_service._command_prefix = "?"
        bot_service._daily_digest_hour = 6
        bot_service._traceroute_format = "hops"
        bot_service._welcome_new_nodes_enabled = False
        bot_service._disabled_commands = {"uptime", "time", "busy"}
        bot_service._last_broadcast_time = 12345.0
        bot_service._last_daily_digest_date = "2026-07-19"
        bot_service._save_persisted_settings()

        BotService._instance = None
        restored = BotService()

        assert restored._command_prefix == "?"
        assert restored._daily_digest_hour == 6
        assert restored._traceroute_format == "hops"
        assert restored._welcome_new_nodes_enabled is False
        assert restored._disabled_commands == {"uptime", "time", "busy"}
        assert restored._last_broadcast_time == 12345.0
        assert restored._last_daily_digest_date == "2026-07-19"

    @pytest.mark.unit
    def test_fresh_boot_seeds_broadcast_timestamp(self, temp_db):
        before = time.time()
        BotService._instance = None
        service = BotService()
        after = time.time()

        assert before <= service._last_broadcast_time <= after
        stored = BotSettingsRepository.get("last_broadcast_time")
        assert stored == service._last_broadcast_time

    @pytest.mark.unit
    def test_disable_command_persists(self, temp_db, bot_service: BotService):
        assert bot_service.disable_command("busy")
        BotService._instance = None
        restored = BotService()
        assert "busy" in restored._disabled_commands
        assert not restored.is_command_enabled("busy")


class TestQuietChannelDirectory:
    @pytest.mark.unit
    def test_broadcast_omits_psks(self, bot_service: BotService):
        channels = [
            {"channel_name": "Spaces", "psk": "secret1", "description": "chat"},
            {"channel_name": "Weather", "psk": "secret2", "description": ""},
        ]
        bot_service.queue_message = MagicMock()

        with patch(
            "src.malla.database.channel_directory_repository."
            "ChannelDirectoryRepository.get_all_channels",
            return_value=channels,
        ):
            bot_service._broadcast_channel_directory()

        bot_service.queue_message.assert_called_once()
        text = bot_service.queue_message.call_args.kwargs["text"]
        assert "secret1" not in text
        assert "secret2" not in text
        assert "Spaces" in text
        assert "Weather" in text
        assert "chanurl" in text

    @pytest.mark.unit
    def test_channelinfo_hides_key_on_public(self, bot_service: BotService):
        ch = {
            "channel_name": "Spaces",
            "psk": "supersecret",
            "description": "local chat",
            "registered_by_name": "Alice",
            "created_at": time.time() - 120,
        }
        with patch(
            "src.malla.database.channel_directory_repository."
            "ChannelDirectoryRepository.get_channel",
            return_value=ch,
        ):
            public = bot_service._cmd_channelinfo(
                SimpleNamespace(
                    args=["Spaces"],
                    is_dm=False,
                    command="channelinfo",
                )
            )
            private = bot_service._cmd_channelinfo(
                SimpleNamespace(
                    args=["Spaces"],
                    is_dm=True,
                    command="channelinfo",
                )
            )

        assert "supersecret" not in public
        assert "DM for key" in public
        assert "Key: supersecret" in private
