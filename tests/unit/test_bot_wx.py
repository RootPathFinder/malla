"""Unit tests for mesh bot !wx zip weather command."""

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


def _ctx(**overrides):
    base = {
        "command": "wx",
        "args": [],
        "raw_message": "!wx",
        "sender_id": 0x12345678,
        "sender_name": "Tester",
        "channel_index": 1,
        "channel_name": "LongFast",
        "received_at": time.time(),
        "packet": {},
        "is_dm": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class TestWxCommand:
    @pytest.mark.unit
    def test_wx_registered_not_weather(self, bot_service: BotService):
        assert "wx" in bot_service._commands
        assert "weather" not in bot_service._commands

    @pytest.mark.unit
    def test_wx_usage_without_args(self, bot_service: BotService):
        text = bot_service._cmd_wx(_ctx(args=[]))
        assert "Usage:" in text
        assert "wx <zip>" in text

    @pytest.mark.unit
    def test_wx_zip_report(self, bot_service: BotService):
        place = {
            "name": "Beverly Hills",
            "admin1": "California",
            "country_code": "US",
            "latitude": 34.07,
            "longitude": -118.4,
        }
        current = {
            "temperature_2m": 75.1,
            "relative_humidity_2m": 55,
            "weather_code": 0,
            "wind_speed_10m": 8.8,
            "precipitation": 0.0,
        }
        with (
            patch.object(bot_service, "_lookup_zip_location", return_value=place),
            patch.object(bot_service, "_fetch_wx_report", return_value=current),
        ):
            text = bot_service._cmd_wx(_ctx(args=["90210"], raw_message="!wx 90210"))

        assert "90210" in text
        assert "Beverly Hills" in text
        assert "75°F" in text
        assert "Clear" in text
        assert "Wind 9mph" in text
        assert "RH 55%" in text

    @pytest.mark.unit
    def test_wx_zip_not_found(self, bot_service: BotService):
        with patch.object(bot_service, "_lookup_zip_location", return_value=None):
            text = bot_service._cmd_wx(_ctx(args=["00000"]))
        assert "not found" in text.lower()

    @pytest.mark.unit
    def test_wx_weather_unavailable(self, bot_service: BotService):
        place = {
            "name": "Town",
            "admin1": None,
            "country_code": "US",
            "latitude": 1.0,
            "longitude": 2.0,
        }
        with (
            patch.object(bot_service, "_lookup_zip_location", return_value=place),
            patch.object(bot_service, "_fetch_wx_report", return_value=None),
        ):
            text = bot_service._cmd_wx(_ctx(args=["12345"]))
        assert "unavailable" in text.lower()

    @pytest.mark.unit
    def test_lookup_us_zip_uses_country_code(self, bot_service: BotService):
        captured: dict[str, str] = {}

        def fake_get(url: str, timeout: float = 3.0):
            captured["url"] = url
            return {
                "results": [
                    {
                        "name": "Beverly Hills",
                        "admin1": "California",
                        "country_code": "US",
                        "latitude": 34.07,
                        "longitude": -118.4,
                    }
                ]
            }

        with patch.object(bot_service, "_http_get_json", side_effect=fake_get):
            place = bot_service._lookup_zip_location("90210-1234")

        assert place is not None
        assert place["name"] == "Beverly Hills"
        assert "countryCode=US" in captured["url"]
        assert "name=90210" in captured["url"]

    @pytest.mark.unit
    def test_format_includes_rain_when_present(self, bot_service: BotService):
        message = bot_service._format_wx_report(
            "10001",
            {"name": "New York"},
            {
                "temperature_2m": 48.0,
                "relative_humidity_2m": 80,
                "weather_code": 61,
                "wind_speed_10m": 12.0,
                "precipitation": 1.2,
            },
        )
        assert "Rain 1.2mm" in message
        assert "Rain" in message
        assert bot_service._wmo_weather_label(61) == "Rain"

    @pytest.mark.unit
    def test_wx_sensors_alias_still_works(self, bot_service: BotService):
        with patch.object(
            bot_service, "_cmd_wx_sensors", return_value="🌡️ Mesh sensors:"
        ) as sensors:
            text = bot_service._cmd_wx(_ctx(args=["sensors"]))
        sensors.assert_called_once()
        assert text.startswith("🌡️")
