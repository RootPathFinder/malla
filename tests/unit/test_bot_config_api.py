"""Unit tests for mesh bot configuration serialization and updates."""

import pytest

from src.malla.routes.bot_routes import _bot_config_dict
from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    return BotService()


class TestBotConfigDict:
    @pytest.mark.unit
    def test_bot_config_dict_includes_digest_and_broadcast_settings(
        self, bot_service: BotService
    ):
        bot_service._daily_digest_enabled = False
        bot_service._daily_digest_hour = 7
        bot_service._channel_broadcast_enabled = False
        bot_service._broadcast_interval_hours = 6
        bot_service._command_prefix = "?"
        bot_service._min_send_interval = 3.5
        bot_service._traceroute_format = "hops"
        bot_service._welcome_new_nodes_enabled = False

        config = _bot_config_dict(bot_service)

        assert config["daily_digest_enabled"] is False
        assert config["daily_digest_hour"] == 7
        assert config["channel_broadcast_enabled"] is False
        assert config["broadcast_interval_hours"] == 6
        assert config["command_prefix"] == "?"
        assert config["min_send_interval"] == 3.5
        assert config["traceroute_format"] == "hops"
        assert config["traceroute_formats"] == [
            "names",
            "longnames",
            "chain",
            "hops",
        ]
        assert config["welcome_new_nodes_enabled"] is False
        assert "listen_channels" in config
        assert "respond_channel_index" in config
        assert "wait_for_jobs" in config
