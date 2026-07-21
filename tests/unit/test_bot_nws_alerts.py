"""Unit tests for mesh bot NWS weather alerts."""

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


def _sample_alert(**overrides):
    alert = {
        "id": "urn:oid:test-1",
        "event": "Heat Advisory",
        "severity": "Moderate",
        "urgency": "Expected",
        "certainty": "Likely",
        "headline": "Heat Advisory issued...",
        "area": "Los Angeles County; Ventura County",
        "expires": "2026-07-22T20:00:00-07:00",
        "ends": "2026-07-22T20:00:00-07:00",
    }
    alert.update(overrides)
    return alert


class TestNwsZipNormalize:
    @pytest.mark.unit
    def test_normalize_zip(self, bot_service: BotService):
        assert bot_service._normalize_nws_zip("90210") == "90210"
        assert bot_service._normalize_nws_zip("90210-1234") == "90210"
        assert bot_service._normalize_nws_zip(" 98101 ") == "98101"
        assert bot_service._normalize_nws_zip("M5V 2T6") == ""
        assert bot_service._normalize_nws_zip("") == ""


class TestNwsFormat:
    @pytest.mark.unit
    def test_format_single_alert(self, bot_service: BotService):
        text = bot_service._format_nws_alert("90210", _sample_alert())
        assert "NWS 90210" in text
        assert "Heat Advisory" in text
        assert "Until" in text
        assert "Los Angeles County" in text
        assert len(text.encode("utf-8")) <= 220

    @pytest.mark.unit
    def test_format_summary_empty(self, bot_service: BotService):
        text = bot_service._format_nws_alerts_summary("98101", [])
        assert "98101" in text
        assert "no active" in text.lower()

    @pytest.mark.unit
    def test_format_summary_lists_events(self, bot_service: BotService):
        alerts = [
            _sample_alert(id="a1", event="Tornado Warning"),
            _sample_alert(id="a2", event="Flash Flood Watch"),
        ]
        text = bot_service._format_nws_alerts_summary("98101", alerts)
        assert "Tornado Warning" in text
        assert "Flash Flood Watch" in text
        assert "(2)" in text


class TestNwsFetchParsing:
    @pytest.mark.unit
    def test_fetch_parses_and_skips_tests(self, bot_service: BotService):
        payload = {
            "features": [
                {
                    "id": "https://api.weather.gov/alerts/1",
                    "properties": {
                        "id": "urn:oid:1",
                        "event": "Tornado Warning",
                        "status": "Actual",
                        "severity": "Extreme",
                        "areaDesc": "King County",
                        "expires": "2026-07-21T21:00:00-07:00",
                        "ends": "2026-07-21T21:00:00-07:00",
                    },
                },
                {
                    "id": "https://api.weather.gov/alerts/2",
                    "properties": {
                        "id": "urn:oid:2",
                        "event": "Test Message",
                        "status": "Actual",
                        "severity": "Minor",
                    },
                },
                {
                    "id": "https://api.weather.gov/alerts/3",
                    "properties": {
                        "id": "urn:oid:3",
                        "event": "Heat Advisory",
                        "status": "Exercise",
                        "severity": "Moderate",
                    },
                },
            ]
        }
        with patch.object(bot_service, "_http_get_json", return_value=payload):
            alerts = bot_service._fetch_nws_alerts(47.6, -122.3)

        assert alerts is not None
        assert len(alerts) == 1
        assert alerts[0]["event"] == "Tornado Warning"
        assert alerts[0]["id"] == "urn:oid:1"


class TestWxAlertsCommand:
    @pytest.mark.unit
    def test_wx_alerts_uses_arg_zip(self, bot_service: BotService):
        with patch.object(
            bot_service,
            "_get_nws_alerts_for_zip",
            return_value=[_sample_alert()],
        ) as fetch:
            text = bot_service._cmd_wx(_ctx(args=["alerts", "90210"]))
        fetch.assert_called_once_with("90210")
        assert "Heat Advisory" in text

    @pytest.mark.unit
    def test_wx_alerts_uses_configured_zip(self, bot_service: BotService):
        bot_service._nws_alert_zip = "98101"
        with patch.object(
            bot_service, "_get_nws_alerts_for_zip", return_value=[]
        ) as fetch:
            text = bot_service._cmd_wx(_ctx(args=["alerts"]))
        fetch.assert_called_once_with("98101")
        assert "no active" in text.lower()

    @pytest.mark.unit
    def test_wx_alerts_requires_zip(self, bot_service: BotService):
        bot_service._nws_alert_zip = ""
        text = bot_service._cmd_wx(_ctx(args=["alerts"]))
        assert "Usage:" in text


class TestNwsPeriodicBroadcast:
    @pytest.mark.unit
    def test_first_poll_baselines_without_broadcast(self, bot_service: BotService):
        bot_service._nws_alert_enabled = True
        bot_service._nws_alert_zip = "90210"
        bot_service._nws_alert_interval_minutes = 5
        bot_service._last_nws_alert_check = 0.0
        bot_service._nws_alert_ids_seeded = False

        queued: list[str] = []

        def capture_queue(text, **kwargs):
            queued.append(text)
            return True

        with (
            patch.object(
                bot_service,
                "_get_nws_alerts_for_zip",
                return_value=[_sample_alert()],
            ),
            patch.object(bot_service, "queue_message", side_effect=capture_queue),
            patch.object(bot_service, "_persist_setting"),
            patch.object(bot_service, "_persist_nws_known_alert_ids"),
        ):
            bot_service._maybe_send_nws_alerts()

        assert queued == []
        assert bot_service._nws_alert_ids_seeded is True
        assert "urn:oid:test-1" in bot_service._nws_known_alert_ids

    @pytest.mark.unit
    def test_new_alert_is_broadcast(self, bot_service: BotService):
        bot_service._nws_alert_enabled = True
        bot_service._nws_alert_zip = "90210"
        bot_service._nws_alert_interval_minutes = 5
        bot_service._last_nws_alert_check = 0.0
        bot_service._nws_alert_ids_seeded = True
        bot_service._nws_known_alert_ids = {"urn:oid:old"}

        queued: list[str] = []

        def capture_queue(text, **kwargs):
            queued.append(text)
            return True

        new_alert = _sample_alert(id="urn:oid:new", event="Tornado Warning")
        with (
            patch.object(
                bot_service, "_get_nws_alerts_for_zip", return_value=[new_alert]
            ),
            patch.object(bot_service, "queue_message", side_effect=capture_queue),
            patch.object(bot_service, "_persist_setting"),
            patch.object(bot_service, "_persist_nws_known_alert_ids"),
            patch.object(bot_service, "_log_activity"),
        ):
            bot_service._maybe_send_nws_alerts()

        assert len(queued) == 1
        assert "Tornado Warning" in queued[0]
        assert "urn:oid:new" in bot_service._nws_known_alert_ids

    @pytest.mark.unit
    def test_disabled_or_missing_zip_skips(self, bot_service: BotService):
        bot_service._nws_alert_enabled = False
        bot_service._nws_alert_zip = "90210"
        bot_service._last_nws_alert_check = 0.0
        with patch.object(bot_service, "_get_nws_alerts_for_zip") as fetch:
            bot_service._maybe_send_nws_alerts()
        fetch.assert_not_called()

        bot_service._nws_alert_enabled = True
        bot_service._nws_alert_zip = ""
        with patch.object(bot_service, "_get_nws_alerts_for_zip") as fetch:
            bot_service._maybe_send_nws_alerts()
        fetch.assert_not_called()
