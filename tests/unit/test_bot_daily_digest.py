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
            new_nodes={"count": 7, "names": ["Newbie", "Fresh", "Rookie"]},
            longest_tr={
                "hops": 5,
                "from_name": "Alpha",
                "to_name": "Zulu",
            },
            when=when,
        )

        assert "📡 Net 7/20" in message
        assert "Nodes: 42 (↑3)" in message
        assert "Pkts: 1.8k (↓12%)" in message
        assert "SNR: -7.2dB (↑0.4)" in message
        assert "Lowbat: 2" in message
        assert "Off routers: 2" in message
        assert "Routers: HillTop, Bridge" in message
        assert "New: 7 (Newbie, Fresh, Rookie…)" in message
        assert "Say hi! !net" in message
        assert "!channels" not in message
        assert "Long TR: 5 hops Alpha→Zulu" in message
        # Top talkers are optional and dropped first when the payload is tight
        assert len(message.encode("utf-8")) <= 220

    @pytest.mark.unit
    def test_format_includes_solar_at_risk(self, bot_service: BotService):
        when = time.strptime("2026-07-20 08:00", "%Y-%m-%d %H:%M")
        message = bot_service._format_daily_digest(
            vitals={
                "active_nodes_24h": 12,
                "packets_24h": 400,
                "avg_snr": -6.0,
                "packets_trend": 0.0,
                "signal_trend": 0.0,
                "solar_at_risk_nodes": 2,
            },
            nodes_delta=0,
            offline_routers=[],
            lowbat_count=0,
            top_names=[],
            new_nodes={"count": 0, "names": []},
            longest_tr=None,
            when=when,
            solar_at_risk_count=2,
        )
        assert "Solar⚠: 2" in message
        assert "Lowbat" not in message

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
            new_nodes={"count": 0, "names": []},
            longest_tr=None,
            when=when,
        )

        assert "Lowbat" not in message
        assert "Off routers" not in message
        assert "Routers:" not in message
        assert "New:" not in message
        assert "Long TR:" not in message
        assert "Top: Alpha" in message

    @pytest.mark.unit
    def test_format_new_nodes_line_variants(self, bot_service: BotService):
        assert bot_service._format_new_nodes_line(0, []) is None
        assert bot_service._format_new_nodes_line(2, ["A", "B"]) == "New: 2 (A, B)"
        assert (
            bot_service._format_new_nodes_line(7, ["A", "B", "C"])
            == "New: 7 (A, B, C…)"
        )


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


class TestDailyDigestExtras:
    @pytest.mark.unit
    def test_new_nodes_query_uses_first_seen_24h_window(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchone.return_value = {"cnt": 7}
        cursor.fetchall.return_value = [
            {
                "node_id": 11,
                "short_name": "Newbie",
                "long_name": "New Node",
                "first_seen": time.time() - 1000,
            }
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            result = bot_service._get_new_nodes_24h(name_limit=3)

        assert result == {"count": 7, "names": ["Newbie"]}
        assert cursor.execute.call_count == 2
        count_sql = cursor.execute.call_args_list[0][0][0]
        names_sql = cursor.execute.call_args_list[1][0][0]
        names_params = cursor.execute.call_args_list[1][0][1]
        assert "COUNT(*)" in count_sql
        assert "first_seen > ?" in names_sql
        assert names_params[0] == pytest.approx(time.time() - 24 * 3600, abs=5)
        assert names_params[1] == 3

    @pytest.mark.unit
    def test_longest_traceroute_picks_max_hops(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "from_node_id": 0x11111111,
                "to_node_id": 0x22222222,
                "hop_start": 3,
                "hop_limit": 1,
                "raw_payload": b"unused",
                "from_short": "Alpha",
                "from_long": None,
                "to_short": "Beta",
                "to_long": None,
            },
            {
                "from_node_id": 0x33333333,
                "to_node_id": 0x44444444,
                "hop_start": 7,
                "hop_limit": 2,
                "raw_payload": b"unused",
                "from_short": "Gamma",
                "from_long": None,
                "to_short": "Delta",
                "to_long": None,
            },
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            with patch(
                "src.malla.utils.traceroute_utils.parse_traceroute_payload",
                side_effect=[
                    {
                        "route_nodes": [1],
                        "snr_towards": [-5.0, -6.0],
                        "route_back": [],
                        "snr_back": [],
                    },
                    {
                        "route_nodes": [1, 2, 3],
                        "snr_towards": [-5.0, -6.0, -7.0, -8.0],
                        "route_back": [],
                        "snr_back": [],
                    },
                ],
            ):
                best = bot_service._get_longest_traceroute_24h()

        assert best is not None
        assert best["hops"] == 5  # hop_start - hop_limit for second packet
        assert best["from_name"] == "Gamma"
        assert best["to_name"] == "Delta"


class TestDailyDigestScheduling:
    @pytest.mark.unit
    def test_maybe_send_daily_digest_once_per_day(self, bot_service: BotService):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        bot_service._daily_digest_hour = 8
        bot_service._daily_digest_timezone = "America/New_York"
        bot_service._enabled = True
        bot_service._last_daily_digest_date = None
        fixed_morning = datetime(
            2026, 7, 20, 9, 0, 0, tzinfo=ZoneInfo("America/New_York")
        )

        with patch.object(
            bot_service, "_build_daily_digest", return_value="📡 Net 7/20\nNodes: 1"
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(bot_service, "_digest_now", return_value=fixed_morning):
                    bot_service._maybe_send_daily_digest()
                    bot_service._maybe_send_daily_digest()

        assert queue_message.call_count == 1
        assert bot_service._last_daily_digest_date == "2026-07-20"

    @pytest.mark.unit
    def test_digest_hour_uses_configured_timezone_not_utc(self, bot_service: BotService):
        """Hour 8 in America/New_York must not fire at 08:00 UTC (04:00 EDT)."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        bot_service._daily_digest_hour = 8
        bot_service._daily_digest_timezone = "America/New_York"
        bot_service._enabled = True
        bot_service._last_daily_digest_date = None

        # 08:00 UTC == 04:00 America/New_York in July — too early
        utc_morning = datetime(2026, 7, 20, 8, 0, 0, tzinfo=ZoneInfo("UTC"))
        with patch.object(
            bot_service,
            "_digest_now",
            return_value=utc_morning.astimezone(ZoneInfo("America/New_York")),
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(
                    bot_service, "_build_daily_digest", return_value="📡 Net"
                ):
                    bot_service._maybe_send_daily_digest()

        queue_message.assert_not_called()

        # 12:00 UTC == 08:00 America/New_York — should fire
        utc_noon = datetime(2026, 7, 20, 12, 0, 0, tzinfo=ZoneInfo("UTC"))
        with patch.object(
            bot_service,
            "_digest_now",
            return_value=utc_noon.astimezone(ZoneInfo("America/New_York")),
        ):
            with patch.object(bot_service, "queue_message") as queue_message:
                with patch.object(
                    bot_service, "_build_daily_digest", return_value="📡 Net"
                ):
                    bot_service._maybe_send_daily_digest()

        queue_message.assert_called_once()

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
