"""Unit tests for mesh bot configuration serialization and updates."""

from unittest.mock import patch

import pytest

from src.malla.routes.bot_routes import _bot_config_dict
from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    yield service
    BotService._instance = None


class TestBotConfigDict:
    @pytest.mark.unit
    def test_bot_config_dict_includes_digest_and_broadcast_settings(
        self, bot_service: BotService
    ):
        bot_service._daily_digest_enabled = False
        bot_service._daily_digest_hour = 7
        bot_service._daily_digest_timezone = "America/New_York"
        bot_service._channel_broadcast_enabled = False
        bot_service._broadcast_interval_hours = 6
        bot_service._command_prefix = "?"
        bot_service._min_send_interval = 3.5
        bot_service._traceroute_format = "hops"
        bot_service._welcome_new_nodes_enabled = False
        bot_service._nws_alert_enabled = True
        bot_service._nws_alert_zip = "98101"
        bot_service._nws_alert_interval_minutes = 15
        bot_service._daily_wx_enabled = True
        bot_service._daily_wx_hour = 6
        bot_service._daily_wx_timezone = "America/Chicago"
        bot_service._daily_wx_zip = "60601"

        config = _bot_config_dict(bot_service)

        assert config["daily_digest_enabled"] is False
        assert config["daily_digest_hour"] == 7
        assert config["daily_digest_timezone"] == "America/New_York"
        assert "America/New_York" in config["daily_digest_timezones"]
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
        assert config["nws_alert_enabled"] is True
        assert config["nws_alert_zip"] == "98101"
        assert config["nws_alert_interval_minutes"] == 15
        assert config["daily_wx_enabled"] is True
        assert config["daily_wx_hour"] == 6
        assert config["daily_wx_timezone"] == "America/Chicago"
        assert config["daily_wx_zip"] == "60601"
        assert "listen_channels" in config
        assert "respond_channel_index" in config
        assert "wait_for_jobs" in config


class TestBotConfigApiTracerouteFormat:
    @pytest.mark.unit
    def test_put_traceroute_format_persists_and_status_returns_it(
        self, client, bot_service: BotService
    ):
        with patch(
            "src.malla.routes.bot_routes.get_bot_service", return_value=bot_service
        ):
            with patch.object(bot_service, "_save_persisted_settings") as save:
                put = client.put(
                    "/api/bot/config",
                    json={"traceroute_format": "longnames"},
                )
                assert put.status_code == 200
                body = put.get_json()
                assert body["success"] is True
                assert body["config"]["traceroute_format"] == "longnames"
                save.assert_called_once()

            assert bot_service._traceroute_format == "longnames"

            status = client.get("/api/bot/status")
            assert status.status_code == 200
            assert status.get_json()["traceroute_format"] == "longnames"


class TestBotConfigApiDailyWx:
    @pytest.mark.unit
    def test_put_daily_wx_config_persists(self, client, bot_service: BotService):
        with patch(
            "src.malla.routes.bot_routes.get_bot_service", return_value=bot_service
        ):
            with patch.object(bot_service, "_save_persisted_settings") as save:
                put = client.put(
                    "/api/bot/config",
                    json={
                        "daily_wx_zip": "90210",
                        "daily_wx_hour": 6,
                        "daily_wx_timezone": "America/Los_Angeles",
                        "daily_wx_enabled": True,
                    },
                )
                assert put.status_code == 200
                body = put.get_json()
                assert body["success"] is True
                assert body["config"]["daily_wx_enabled"] is True
                assert body["config"]["daily_wx_hour"] == 6
                assert body["config"]["daily_wx_timezone"] == "America/Los_Angeles"
                assert body["config"]["daily_wx_zip"] == "90210"
                save.assert_called_once()

            assert bot_service._daily_wx_enabled is True
            assert bot_service._daily_wx_zip == "90210"

    @pytest.mark.unit
    def test_enable_daily_wx_without_zip_fails(self, client, bot_service: BotService):
        bot_service._daily_wx_zip = ""
        bot_service._nws_alert_zip = ""
        with patch(
            "src.malla.routes.bot_routes.get_bot_service", return_value=bot_service
        ):
            with patch.object(bot_service, "_save_persisted_settings"):
                put = client.put(
                    "/api/bot/config",
                    json={"daily_wx_enabled": True},
                )
        assert put.status_code == 400
        assert "zip" in put.get_json()["error"].lower()
        assert bot_service._daily_wx_enabled is False
