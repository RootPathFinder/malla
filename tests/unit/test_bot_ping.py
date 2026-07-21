"""Unit tests for mesh bot !ping RF-aware replies."""

import time
from types import SimpleNamespace

import pytest

from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    service._enabled = True
    yield service
    BotService._instance = None


def _ctx(packet: dict | None = None, **overrides):
    base = {
        "command": "ping",
        "args": [],
        "raw_message": "!ping",
        "sender_id": 0x12345678,
        "sender_name": "Tester",
        "channel_index": 1,
        "channel_name": "LongFast",
        "received_at": time.time(),
        "packet": packet or {},
        "is_dm": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class TestPingCommand:
    @pytest.mark.unit
    def test_ping_with_full_rf_stats_camel_case(self, bot_service: BotService):
        reply = bot_service._cmd_ping(
            _ctx(
                {
                    "hopStart": 3,
                    "hopLimit": 1,
                    "rxSnr": 8.5,
                    "rxRssi": -85,
                }
            )
        )
        assert reply == "Pong!\nhops: 2\nsnr: 8.5 dB\nrssi: -85 dBm"

    @pytest.mark.unit
    def test_ping_with_snake_case_fields(self, bot_service: BotService):
        reply = bot_service._cmd_ping(
            _ctx(
                {
                    "hop_start": 5,
                    "hop_limit": 5,
                    "rx_snr": 12.0,
                    "rx_rssi": -70,
                }
            )
        )
        assert reply == "Pong!\nhops: 0 (direct)\nsnr: 12 dB\nrssi: -70 dBm"

    @pytest.mark.unit
    def test_ping_hops_away_fallback(self, bot_service: BotService):
        reply = bot_service._cmd_ping(_ctx({"hopsAway": 4, "rxSnr": -1.5}))
        assert "hops: 4" in reply
        assert "snr: -1.5 dB" in reply

    @pytest.mark.unit
    def test_ping_via_mqtt(self, bot_service: BotService):
        reply = bot_service._cmd_ping(
            _ctx({"hopStart": 3, "hopLimit": 2, "viaMqtt": True})
        )
        assert reply == "Pong!\nhops: 1\nvia: MQTT"

    @pytest.mark.unit
    def test_ping_no_rf_metadata(self, bot_service: BotService):
        reply = bot_service._cmd_ping(_ctx({}))
        assert reply == "Pong!\nbot online"

    @pytest.mark.unit
    def test_extract_prefers_hop_start_over_hops_away(self, bot_service: BotService):
        stats = bot_service._extract_ping_rf_stats(
            {"hopStart": 7, "hopLimit": 5, "hopsAway": 99}
        )
        assert stats["hops"] == 2
