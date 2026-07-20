"""Unit tests for the mesh bot daily network digest."""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    """Return a fresh bot service instance for each test."""
    BotService._instance = None
    service = BotService()
    service._enabled = True
    return service


class TestDailyDigestFormatting:
    @pytest.mark.unit
    def test_format_includes_alerts_and_trends(self, bot_service: BotService):
        when = time.strptime("2026-07-20 08:00", "%Y-%m-%d %H:%M")
        message = bot_service._format_daily_digest(
            vitals={
                "active_nodes_24h": 42,
                "packets_24h": 1800,
                "avg_snr": -7.2,
                "packets_trend": -12.0,
                "signal_trend": 0.4,
            },
            nodes_delta=3,
            offline_routers=["HillTop", "Bridge"],
            lowbat_count=2,
            top_names=["Alpha", "Bravo", "Charlie"],
            when=when,
        )

        assert "📡 Net 7/20" in message
        assert "Nodes: 42 (↑3)" in message
        assert "Pkts: 1.8k (↓12%)" in message
        assert "SNR: -7.2dB (↑0.4)" in message
        assert "Lowbat: 2" in message
        assert "Off routers: 2" in message
        assert "Routers: HillTop, Bridge" in message
        assert "Top: Alpha, Bravo, Charlie" in message

    @pytest.mark.unit
    def test_format_omits_stale_alert_lines_when_empty(self, bot_service: BotService):
        when = time.strptime("2026-07-20 08:00", "%Y-%m-%d %H:%M")
        message = bot_service._format_daily_digest(
            vitals={
                "active_nodes_24h": 10,
                "packets_24h": 100,
                "avg_snr": -5.0,
                "packets_trend": 0.2,
                "signal_trend": 0.0,
            },
            nodes_delta=0,
            offline_routers=[],
            lowbat_count=0,
            top_names=["Alpha"],
            when=when,
        )

        assert "Lowbat" not in message
        assert "Off routers" not in message
        assert "Routers:" not in message
        assert "Top: Alpha" in message


class TestDailyDigestFilters:
    @pytest.mark.unit
    def test_offline_router_query_uses_recent_window_only(
        self, bot_service: BotService
    ):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "node_id": 1,
                "short_name": "HillTop",
                "long_name": "Hill Top Repeater",
                "last_seen": time.time() - 3600,
            }
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            names = bot_service._get_recently_offline_routers()

        assert names == ["HillTop"]
        sql = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        assert "HAVING last_seen BETWEEN ? AND ?" in sql
        assert "n.role IN" in sql
        # oldest bound first, then newest bound
        oldest, newest = params[-2], params[-1]
        assert oldest < newest
        assert newest - oldest == pytest.approx(
            (bot_service._digest_offline_max_hours - bot_service._digest_offline_min_hours)
            * 3600,
            rel=0.01,
        )

    @pytest.mark.unit
    def test_lowbat_count_only_uses_recent_telemetry(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchone.return_value = {"cnt": 2}
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            count = bot_service._get_recent_lowbat_count()

        assert count == 2
        sql = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        assert "timestamp > ?" in sql
        assert params[1] == bot_service._digest_lowbat_threshold
        # since timestamp should be roughly now - 24h
        assert params[0] == pytest.approx(
            time.time() - bot_service._digest_lowbat_hours * 3600, abs=5
        )


class TestDailyDigestScheduling:
    @pytest.mark.unit
    def test_maybe_send_daily_digest_once_per_day(self, bot_service: BotService):
        bot_service._daily_digest_hour = 8
        bot_service._enabled = True
        fixed_morning = time.struct_time((2026, 7, 20, 9, 0, 0, 0, 201, 0))

        with patch.object(
            bot_service, "_build_daily_digest", return_value="📡 Net 7/20\nNodes: 1"
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch(
                    "src.malla.services.bot_service.time.localtime",
                    return_value=fixed_morning,
                ):
                    bot_service._maybe_send_daily_digest()
                    bot_service._maybe_send_daily_digest()

        assert queue_message.call_count == 1
        assert bot_service._last_daily_digest_date == "2026-07-20"

    @pytest.mark.unit
    def test_cmd_net_returns_built_digest(self, bot_service: BotService):
        ctx = SimpleNamespace(
            command="net",
            args=[],
            raw_message="!net",
            sender_id=1,
            sender_name="Tester",
            channel_index=1,
            channel_name="LongFast",
            received_at=time.time(),
            packet={},
            is_dm=False,
        )
        with patch.object(
            bot_service, "_build_daily_digest", return_value="📡 Net 7/20\nNodes: 5"
        ):
            result = bot_service._cmd_net(ctx)

        assert result == "📡 Net 7/20\nNodes: 5"
