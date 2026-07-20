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
    def test_neighbors_prefers_long_names_from_neighborinfo(
        self, bot_service: BotService
    ):
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value = cursor

        rf_rows = [
            {
                "node_id": 0x11111111,
                "short_name": "Hill",
                "long_name": "Hill Top Roof",
                "role": "ROUTER",
                "avg_snr": -8.2,
                "cnt": None,
                "last_ts": time.time() - 120,
            }
        ]

        with (
            patch(
                "src.malla.database.connection.get_db_connection", return_value=conn
            ),
            patch.object(
                bot_service,
                "_load_neighborinfo_neighbors",
                return_value=(rf_rows, time.time() - 120),
            ),
        ):
            result = bot_service._cmd_neighbors(_ctx(command="neighbors"))

        assert "RF neighbors" in result
        assert "Hill Top Roof" in result
        assert "-8dB" in result
        assert "rtr" in result
        assert "DM !help for more" in result

    @pytest.mark.unit
    def test_neighbors_falls_back_to_relay_with_long_names(
        self, bot_service: BotService
    ):
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value = cursor

        relay_rows = [
            {
                "node_id": 0x22222222,
                "short_name": "Ada",
                "long_name": "Ada Lake Node",
                "role": "CLIENT",
                "avg_snr": 7.4,
                "cnt": 12,
                "last_ts": time.time() - 600,
            }
        ]

        with (
            patch(
                "src.malla.database.connection.get_db_connection", return_value=conn
            ),
            patch.object(
                bot_service, "_load_neighborinfo_neighbors", return_value=([], None)
            ),
            patch.object(
                bot_service,
                "_load_inferred_neighbors",
                return_value=(relay_rows, "relay"),
            ),
        ):
            result = bot_service._cmd_neighbors(_ctx(command="neighbors"))

        assert "relay neighbors" in result
        assert "Ada Lake Node" in result
        assert "7dB" in result
        assert "12pk" in result

    @pytest.mark.unit
    def test_neighbor_display_name_includes_short_when_useful(
        self, bot_service: BotService
    ):
        label = bot_service._neighbor_display_name(
            0xABC, short_name="Fox", long_name="North Ridge", max_len=30
        )
        assert label == "North Ridge (Fox)"

        same = bot_service._neighbor_display_name(
            0xABC, short_name="Hill", long_name="Hill Top", max_len=30
        )
        assert same == "Hill Top"
        assert "(Hill)" not in same


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
