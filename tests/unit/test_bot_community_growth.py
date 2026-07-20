"""Unit tests for mesh bot community-growth features."""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
        "command": "help",
        "args": [],
        "raw_message": "!help",
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


class TestStarterHelp:
    @pytest.mark.unit
    def test_channel_help_is_starter_card(self, bot_service: BotService):
        text = bot_service._cmd_help(_ctx(is_dm=False))
        assert "Mesh bot" in text
        assert "!net" in text
        assert "!channels" in text
        assert "DM !help for all cmds" in text
        assert "Cmds:" not in text

    @pytest.mark.unit
    def test_dm_help_lists_commands(self, bot_service: BotService):
        text = bot_service._cmd_help(_ctx(is_dm=True))
        assert text.startswith("Cmds:")
        assert "!net" in text
        assert "!start" in text

    @pytest.mark.unit
    def test_start_command_matches_starter(self, bot_service: BotService):
        assert bot_service._cmd_start(_ctx(command="start")) == (
            bot_service._starter_help_text()
        )

    @pytest.mark.unit
    def test_uptime_and_time_disabled_by_default(self, bot_service: BotService):
        assert "uptime" in bot_service._disabled_commands
        assert "time" in bot_service._disabled_commands
        assert bot_service.is_command_enabled("net")
        assert not bot_service.is_command_enabled("uptime")


class TestDmTipsAndNeighbors:
    @pytest.mark.unit
    def test_dm_tip_only_on_public_replies(self, bot_service: BotService):
        public = bot_service._with_dm_tip(_ctx(is_dm=False), "📊 stats")
        private = bot_service._with_dm_tip(_ctx(is_dm=True), "📊 stats")
        assert public.endswith("DM !help for more")
        assert private == "📊 stats"

    @pytest.mark.unit
    def test_neighbors_uses_names(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "node_id": 0x11111111,
                "short_name": "Hill",
                "long_name": "Hill Top",
                "avg_snr": -8.2,
                "cnt": 3,
            }
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            result = bot_service._cmd_neighbors(_ctx(command="neighbors"))

        assert "Near you:" in result
        assert "Hill" in result
        assert "-8dB" in result
        assert "DM !help for more" in result


class TestWelcomeAndDigestCta:
    @pytest.mark.unit
    def test_digest_adds_cta_when_new_nodes_present(self, bot_service: BotService):
        when = time.strptime("2026-07-20 08:00", "%Y-%m-%d %H:%M")
        message = bot_service._format_daily_digest(
            vitals={
                "active_nodes_24h": 10,
                "packets_24h": 100,
                "avg_snr": -5.0,
                "packets_trend": 0,
                "signal_trend": 0,
            },
            nodes_delta=0,
            offline_routers=[],
            lowbat_count=0,
            top_names=[],
            new_nodes={"count": 2, "names": ["Fox", "Ada"]},
            longest_tr=None,
            when=when,
        )
        assert "New: 2 (Fox, Ada)" in message
        assert "Say hi! !channels" in message

    @pytest.mark.unit
    def test_welcome_new_node_queues_message(self, bot_service: BotService):
        bot_service._welcome_new_nodes_enabled = True
        bot_service._last_welcome_check = time.time() - 1000
        bot_service._last_welcome_broadcast_time = 0.0

        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "node_id": 42,
                "short_name": "Fox",
                "long_name": "Fox Node",
                "first_seen": time.time() - 60,
            }
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                bot_service._maybe_welcome_new_nodes()

        queue_message.assert_called_once()
        text = queue_message.call_args.kwargs["text"]
        assert "Welcome Fox" in text
        assert "!start" in text
        assert 42 in bot_service._welcomed_node_ids
