"""Unit tests for multi-hour bot notice scheduling and manual triggers."""

from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    service._enabled = True
    return service


class TestParseHours:
    @pytest.mark.unit
    def test_parse_hours_variants(self):
        assert BotService._parse_hours("8") == [8]
        assert BotService._parse_hours("8,12,18") == [8, 12, 18]
        assert BotService._parse_hours("18, 8, 8") == [8, 18]
        assert BotService._parse_hours([7, 19]) == [7, 19]
        assert BotService._parse_hours(6) == [6]
        assert BotService._parse_hours("") == []
        assert BotService._parse_hours("25") is None
        assert BotService._parse_hours("8,x") is None


class TestMultiHourDigest:
    @pytest.mark.unit
    def test_sends_each_configured_hour_once(self, bot_service: BotService):
        bot_service._daily_digest_enabled = True
        bot_service._daily_digest_hours = [8, 18]
        bot_service._daily_digest_timezone = "UTC"
        bot_service._daily_digest_sent_slots = set()

        morning = datetime(2026, 7, 24, 8, 5, tzinfo=ZoneInfo("UTC"))
        afternoon = datetime(2026, 7, 24, 18, 5, tzinfo=ZoneInfo("UTC"))

        with patch.object(
            bot_service, "_build_daily_digest", return_value="📡 Net"
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(bot_service, "_digest_now", return_value=morning):
                    bot_service._maybe_send_daily_digest()
                    bot_service._maybe_send_daily_digest()
                assert queue_message.call_count == 1
                assert "2026-07-24:08" in bot_service._daily_digest_sent_slots

                with patch.object(bot_service, "_digest_now", return_value=afternoon):
                    bot_service._maybe_send_daily_digest()
                assert queue_message.call_count == 2
                assert "2026-07-24:18" in bot_service._daily_digest_sent_slots

    @pytest.mark.unit
    def test_manual_digest_does_not_consume_slot(self, bot_service: BotService):
        bot_service._daily_digest_sent_slots = set()
        with patch.object(
            bot_service, "_build_daily_digest", return_value="📡 Net"
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                result = bot_service.send_daily_digest_now()
        assert result["success"] is True
        queue_message.assert_called_once()
        assert bot_service._daily_digest_sent_slots == set()


class TestMultiHourWx:
    @pytest.mark.unit
    def test_sends_second_hour_same_day(self, bot_service: BotService):
        bot_service._daily_wx_enabled = True
        bot_service._daily_wx_hours = [7, 17]
        bot_service._daily_wx_timezone = "UTC"
        bot_service._daily_wx_zip = "90210"
        bot_service._daily_wx_sent_slots = {"2026-07-24:07"}

        evening = datetime(2026, 7, 24, 17, 0, tzinfo=ZoneInfo("UTC"))
        with patch.object(
            bot_service,
            "_build_daily_wx_forecast",
            return_value=["🌤️ FCST"],
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(bot_service, "_daily_wx_now", return_value=evening):
                    bot_service._maybe_send_daily_wx()

        queue_message.assert_called_once()
        assert "2026-07-24:17" in bot_service._daily_wx_sent_slots


class TestSendNoticeNow:
    @pytest.mark.unit
    def test_unknown_notice(self, bot_service: BotService):
        result = bot_service.send_notice_now("nope")
        assert result["success"] is False
        assert "Unknown" in result["error"]

    @pytest.mark.unit
    def test_nws_manual_requires_zip(self, bot_service: BotService):
        bot_service._nws_alert_zip = ""
        result = bot_service.send_notice_now("nws_alerts")
        assert result["success"] is False
        assert "zip" in result["error"].lower()
