"""Unit tests for the mesh bot daily weather forecast broadcast."""

import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    service._enabled = True
    return service


def _sample_daily() -> dict:
    return {
        "time": ["2026-07-24", "2026-07-25", "2026-07-26"],
        "weather_code": [0, 61, 2],
        "temperature_2m_max": [75.1, 68.2, 72.0],
        "temperature_2m_min": [55.0, 52.1, 54.0],
        "precipitation_sum": [0.0, 0.35, 0.0],
        "precipitation_probability_max": [10, 80, 20],
    }


class TestDailyWxFormatting:
    @pytest.mark.unit
    def test_format_fits_one_lora_message(self, bot_service: BotService):
        messages = bot_service._format_wx_forecast_messages(
            "90210",
            {"name": "Beverly Hills"},
            _sample_daily(),
        )
        assert len(messages) == 1
        text = messages[0]
        assert "🌤️ FCST 90210" in text
        assert "Beverly Hills" in text or "Beverly" in text
        assert "Today Clear 75/55" in text
        assert "Sat Rain 68/52" in text
        assert "80%" in text
        assert '0.4"' in text or '0.3"' in text
        assert "Sun PCloudy 72/54" in text
        assert len(text.encode("utf-8")) <= 220

    @pytest.mark.unit
    def test_format_splits_when_over_budget(self, bot_service: BotService):
        # Force a tiny budget so days spill into a 2nd message.
        messages = bot_service._format_wx_forecast_messages(
            "90210",
            {"name": "Beverly Hills"},
            _sample_daily(),
            max_bytes=55,
            max_messages=2,
        )
        assert 1 <= len(messages) <= 2
        assert all(len(m.encode("utf-8")) <= 55 for m in messages)
        joined = "\n".join(messages)
        assert "Today" in joined or "Clear" in joined

    @pytest.mark.unit
    def test_day_line_omits_low_precip(self, bot_service: BotService):
        line = bot_service._format_wx_day_line(
            "2026-07-24",
            0,
            70,
            50,
            0.01,
            10,
            today_label="Today",
        )
        assert line == "Today Clear 70/50"
        assert "%" not in line


class TestDailyWxScheduling:
    @pytest.mark.unit
    def test_maybe_send_daily_wx_once_per_day(self, bot_service: BotService):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        bot_service._daily_wx_enabled = True
        bot_service._daily_wx_hour = 7
        bot_service._daily_wx_timezone = "America/New_York"
        bot_service._daily_wx_zip = "90210"
        bot_service._last_daily_wx_date = None
        fixed_morning = datetime(
            2026, 7, 24, 8, 0, 0, tzinfo=ZoneInfo("America/New_York")
        )

        with patch.object(
            bot_service,
            "_build_daily_wx_forecast",
            return_value=["🌤️ FCST 90210\nToday Clear 75/55"],
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(
                    bot_service, "_daily_wx_now", return_value=fixed_morning
                ):
                    bot_service._maybe_send_daily_wx()
                    bot_service._maybe_send_daily_wx()

        assert queue_message.call_count == 1
        assert bot_service._last_daily_wx_date == "2026-07-24"

    @pytest.mark.unit
    def test_wx_hour_uses_configured_timezone_not_utc(self, bot_service: BotService):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        bot_service._daily_wx_enabled = True
        bot_service._daily_wx_hour = 7
        bot_service._daily_wx_timezone = "America/New_York"
        bot_service._daily_wx_zip = "90210"
        bot_service._last_daily_wx_date = None

        # 07:00 UTC == 03:00 America/New_York in July — too early
        utc_morning = datetime(2026, 7, 24, 7, 0, 0, tzinfo=ZoneInfo("UTC"))
        with patch.object(
            bot_service,
            "_daily_wx_now",
            return_value=utc_morning.astimezone(ZoneInfo("America/New_York")),
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(
                    bot_service,
                    "_build_daily_wx_forecast",
                    return_value=["🌤️ FCST"],
                ):
                    bot_service._maybe_send_daily_wx()

        queue_message.assert_not_called()

        # 11:00 UTC == 07:00 America/New_York — should fire
        utc_fire = datetime(2026, 7, 24, 11, 0, 0, tzinfo=ZoneInfo("UTC"))
        with patch.object(
            bot_service,
            "_daily_wx_now",
            return_value=utc_fire.astimezone(ZoneInfo("America/New_York")),
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(
                    bot_service,
                    "_build_daily_wx_forecast",
                    return_value=["🌤️ FCST"],
                ):
                    bot_service._maybe_send_daily_wx()

        queue_message.assert_called_once()

    @pytest.mark.unit
    def test_disabled_or_missing_zip_skips(self, bot_service: BotService):
        bot_service._daily_wx_enabled = False
        bot_service._daily_wx_zip = "90210"
        with patch.object(bot_service, "queue_message") as queue_message:
            bot_service._maybe_send_daily_wx()
        queue_message.assert_not_called()

        bot_service._daily_wx_enabled = True
        bot_service._daily_wx_zip = ""
        bot_service._nws_alert_zip = ""
        with patch.object(bot_service, "queue_message") as queue_message:
            bot_service._maybe_send_daily_wx()
        queue_message.assert_not_called()

    @pytest.mark.unit
    def test_queues_two_messages_when_forecast_splits(self, bot_service: BotService):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        bot_service._daily_wx_enabled = True
        bot_service._daily_wx_hour = 7
        bot_service._daily_wx_timezone = "UTC"
        bot_service._daily_wx_zip = "90210"
        bot_service._last_daily_wx_date = None
        fixed = datetime(2026, 7, 24, 8, 0, 0, tzinfo=ZoneInfo("UTC"))

        with patch.object(
            bot_service,
            "_build_daily_wx_forecast",
            return_value=["msg1", "msg2"],
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(bot_service, "_daily_wx_now", return_value=fixed):
                    bot_service._maybe_send_daily_wx()

        assert queue_message.call_count == 2

    @pytest.mark.unit
    def test_resolve_zip_falls_back_to_nws(self, bot_service: BotService):
        bot_service._daily_wx_zip = ""
        bot_service._nws_alert_zip = "98101"
        assert bot_service._resolve_daily_wx_zip() == "98101"
        assert bot_service._resolve_daily_wx_zip("90210-1234") == "90210"


class TestFcstCommand:
    @pytest.mark.unit
    def test_fcst_registered(self, bot_service: BotService):
        assert "fcst" in bot_service._commands

    @pytest.mark.unit
    def test_cmd_fcst_returns_built_forecast(self, bot_service: BotService):
        ctx = SimpleNamespace(
            command="fcst",
            args=["90210"],
            raw_message="!fcst 90210",
            sender_id=1,
            sender_name="Tester",
            channel_index=1,
            channel_name="LongFast",
            received_at=time.time(),
            packet={},
            is_dm=False,
        )
        with patch.object(
            bot_service,
            "_build_daily_wx_forecast",
            return_value=["🌤️ FCST 90210\nToday Clear 75/55"],
        ):
            result = bot_service._cmd_fcst(ctx)

        assert "FCST 90210" in result
        assert "Today Clear" in result

    @pytest.mark.unit
    def test_cmd_fcst_usage_without_zip(self, bot_service: BotService):
        bot_service._daily_wx_zip = ""
        bot_service._nws_alert_zip = ""
        ctx = SimpleNamespace(
            command="fcst",
            args=[],
            raw_message="!fcst",
            sender_id=1,
            sender_name="Tester",
            channel_index=1,
            channel_name="LongFast",
            received_at=time.time(),
            packet={},
            is_dm=False,
        )
        text = bot_service._cmd_fcst(ctx)
        assert "Usage:" in text
