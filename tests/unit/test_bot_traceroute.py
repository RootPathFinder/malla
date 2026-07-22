"""Unit tests for mesh bot traceroute parsing and formatting."""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from meshtastic import mesh_pb2

from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    """Return a fresh bot service instance for each test."""
    BotService._instance = None
    service = BotService()
    service._enabled = True
    return service


def _ctx(**overrides):
    base = {
        "command": "traceroute",
        "args": [],
        "raw_message": "!traceroute",
        "sender_id": 0x12345678,
        "sender_name": "Alpha",
        "channel_index": 1,
        "channel_name": "LongFast",
        "received_at": time.time(),
        "packet": {},
        "is_dm": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _build_route_discovery_payload(
    route: list[int],
    snr_towards: list[float],
    route_back: list[int] | None = None,
    snr_back: list[int] | None = None,
) -> bytes:
    route_discovery = mesh_pb2.RouteDiscovery()
    route_discovery.route.extend(route)
    route_discovery.snr_towards.extend(int(snr * 4) for snr in snr_towards)
    if route_back is not None:
        route_discovery.route_back.extend(route_back)
    if snr_back is not None:
        route_discovery.snr_back.extend(int(snr * 4) for snr in snr_back)
    return route_discovery.SerializeToString()


def _name_map() -> dict[int, str]:
    return {
        0x12345678: "Alpha",
        0x11111111: "Hill",
        0x87654321: "You",
    }


class TestBotTracerouteParsing:
    @pytest.mark.unit
    def test_parse_traceroute_from_raw_payload(self, bot_service: BotService):
        payload = _build_route_discovery_payload(
            route=[0x11111111],
            snr_towards=[-5.0, -8.0],
            route_back=[0x11111111],
            snr_back=[-7.0, -6.0],
        )
        packet = {"decoded": {"payload": payload}}

        route, route_back, snr_towards, snr_back = (
            bot_service._parse_traceroute_route_data(packet, packet["decoded"])
        )

        assert route == [0x11111111]
        assert route_back == [0x11111111]
        assert snr_towards == [-5.0, -8.0]
        assert snr_back == [-7.0, -6.0]

    @pytest.mark.unit
    def test_parse_traceroute_from_snake_case_dict(self, bot_service: BotService):
        decoded = {
            "traceroute": {
                "route": [0x22222222],
                "route_back": [],
                "snr_towards": [-20, -16],
                "snr_back": [],
            }
        }

        route, route_back, snr_towards, snr_back = (
            bot_service._parse_traceroute_route_data({}, decoded)
        )

        assert route == [0x22222222]
        assert route_back == []
        assert snr_towards == [-5.0, -4.0]
        assert snr_back == []


class TestBotTracerouteMatching:
    @pytest.mark.unit
    def test_match_pending_traceroute_from_target(self, bot_service: BotService):
        dest_id = 0x87654321
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "Requester",
            1,
            time.time(),
        )

        matched = bot_service._match_pending_traceroute(
            from_id=dest_id,
            to_id=0x12345678,
            request_id=42,
            route=[0x11111111],
            route_back=[],
            snr_towards=[-5.0, -8.0],
            snr_back=[],
        )

        assert matched == dest_id

    @pytest.mark.unit
    def test_match_pending_traceroute_response_to_local_node(
        self, bot_service: BotService
    ):
        dest_id = 0x87654321
        local_id = 0x12345678
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "Requester",
            1,
            time.time(),
        )

        with patch.object(bot_service, "_get_local_node_id", return_value=local_id):
            matched = bot_service._match_pending_traceroute(
                from_id=dest_id,
                to_id=local_id,
                request_id=42,
                route=[0x11111111],
                route_back=[0x11111111],
                snr_towards=[-5.0, -8.0],
                snr_back=[-7.0, -6.0],
            )

        assert matched == dest_id


class TestBotTracerouteFormatting:
    @pytest.mark.unit
    def test_default_format_is_names(self, bot_service: BotService):
        assert bot_service._traceroute_format == "names"

    @pytest.mark.unit
    def test_format_names_style_uses_bulk_labels(self, bot_service: BotService):
        with patch.object(
            bot_service, "_fetch_node_labels", return_value=_name_map()
        ) as fetch:
            result = bot_service._format_traceroute_result(
                route=[0x11111111],
                route_back=[0x11111111],
                snr_towards=[-5.0, -8.0],
                snr_back=[-7.0, -6.0],
                source_id=0x12345678,
                dest_id=0x87654321,
                style="names",
            )

        fetch.assert_called_once()
        assert fetch.call_args.kwargs.get("prefer_long") in (None, False)
        assert "TR to You (2 hops)" in result
        assert "Alpha → Hill(-5) → You(-8)" in result
        assert "← You → Hill(-7) → Alpha(-6)" in result

    @pytest.mark.unit
    def test_format_longnames_style_prefers_long_names(self, bot_service: BotService):
        long_map = {
            0x12345678: "Alpha Node",
            0x11111111: "Hill Top",
            0x87654321: "Your Node",
        }
        with patch.object(
            bot_service, "_fetch_node_labels", return_value=long_map
        ) as fetch:
            result = bot_service._format_traceroute_result(
                route=[0x11111111],
                route_back=[0x11111111],
                snr_towards=[-5.0, -8.0],
                snr_back=[-7.0, -6.0],
                source_id=0x12345678,
                dest_id=0x87654321,
                style="longnames",
            )

        fetch.assert_called_once()
        assert fetch.call_args.kwargs["prefer_long"] is True
        assert fetch.call_args.kwargs["max_len"] == 12
        assert "TR to Your Node (2 hops)" in result
        assert "Alpha Node → Hill Top(-5) → Your Node(-8)" in result
        assert "← Your Node → Hill Top(-7) → Alpha Node(-6)" in result

    @pytest.mark.unit
    def test_format_chain_style_includes_names(self, bot_service: BotService):
        with patch.object(bot_service, "_fetch_node_labels", return_value=_name_map()):
            result = bot_service._format_traceroute_result(
                route=[0x11111111],
                route_back=[0x11111111],
                snr_towards=[-5.0, -8.0],
                snr_back=[-7.0, -6.0],
                source_id=0x12345678,
                dest_id=0x87654321,
                style="chain",
            )

        assert "TR → Alpha > Hill(-5) > You(-8)" in result
        assert "← You > Hill(-7) > Alpha(-6)" in result

    @pytest.mark.unit
    def test_format_hops_style_includes_names(self, bot_service: BotService):
        with patch.object(bot_service, "_fetch_node_labels", return_value=_name_map()):
            result = bot_service._format_traceroute_result(
                route=[0x11111111],
                route_back=[0x11111111],
                snr_towards=[-5.0, -8.0],
                snr_back=[-7.0, -6.0],
                source_id=0x12345678,
                dest_id=0x87654321,
                style="hops",
            )

        assert "TR to You (2 hops)" in result
        assert "1 Alpha→Hill -5" in result
        assert "2 Hill→You -8" in result
        assert "← You→Hill -7 →Alpha -6" in result

    @pytest.mark.unit
    def test_format_falls_back_to_hex_when_unnamed(self, bot_service: BotService):
        with patch.object(
            bot_service,
            "_fetch_node_labels",
            return_value={
                0x12345678: "5678",
                0x87654321: "4321",
            },
        ):
            result = bot_service._format_traceroute_result(
                route=[],
                route_back=[],
                snr_towards=[-4.0],
                snr_back=[],
                source_id=0x12345678,
                dest_id=0x87654321,
                style="names",
            )

        assert "TR to 4321 (1 hop)" in result
        assert "5678 → 4321(-4)" in result

    @pytest.mark.unit
    def test_fetch_node_labels_reads_node_info_shape(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "node_id": 0x12345678,
                "short_name": "Alpha",
                "long_name": "Alpha Node",
            },
            {
                "node_id": 0x87654321,
                "short_name": None,
                "long_name": "VeryLongDestinationName",
            },
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            labels = bot_service._fetch_node_labels(
                [0x12345678, 0x87654321, 0x11111111],
                max_len=6,
                overrides={0x11111111: "HillTop"},
            )

        assert labels[0x12345678] == "Alpha"
        assert labels[0x87654321] == "VeryLo"
        assert labels[0x11111111] == "HillTo"

    @pytest.mark.unit
    def test_fetch_node_labels_prefer_long(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "node_id": 0x12345678,
                "short_name": "Alpha",
                "long_name": "Alpha Node",
            },
            {
                "node_id": 0x87654321,
                "short_name": "You",
                "long_name": "Your Node",
            },
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            labels = bot_service._fetch_node_labels(
                [0x12345678, 0x87654321],
                max_len=12,
                overrides={0x87654321: "You"},
                prefer_long=True,
            )

        assert labels[0x12345678] == "Alpha Node"
        # DB long name wins over short override when prefer_long=True
        assert labels[0x87654321] == "Your Node"

    @pytest.mark.unit
    def test_get_node_name_reads_node_info_directly(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "short_name": "You",
            "long_name": "Your Node",
        }
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            assert bot_service._get_node_name(0x87654321) == "Your Node"

    @pytest.mark.unit
    def test_handle_traceroute_packet_sends_named_response(
        self, bot_service: BotService
    ):
        dest_id = 0x87654321
        local_id = 0x12345678
        bot_service._traceroute_format = "names"
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "You",
            1,
            time.time(),
        )

        payload = _build_route_discovery_payload(
            route=[0x11111111],
            snr_towards=[-5.0, -8.0],
            route_back=[0x11111111],
            snr_back=[-7.0, -6.0],
        )
        packet = {
            "from": dest_id,
            "to": local_id,
            "requestId": 99,
            "decoded": {
                "portnum": "TRACEROUTE_APP",
                "payload": payload,
            },
        }

        with patch.object(bot_service, "_get_local_node_id", return_value=local_id):
            with patch.object(
                bot_service,
                "_fetch_node_labels",
                return_value=_name_map(),
            ):
                with patch.object(bot_service, "queue_message") as queue_message:
                    bot_service._handle_traceroute_packet(packet)

        queue_message.assert_called_once()
        response = queue_message.call_args.kwargs["text"]
        assert "TR to You (2 hops)" in response
        assert "Alpha → Hill(-5) → You(-8)" in response
        assert dest_id not in bot_service._pending_traceroutes


class TestBotTracerouteDestination:
    @pytest.mark.unit
    def test_resolve_defaults_to_sender(self, bot_service: BotService):
        dest_id, label = bot_service._resolve_traceroute_destination(_ctx())
        assert dest_id == 0x12345678
        assert label == "Alpha"

    @pytest.mark.unit
    def test_resolve_prefers_exact_short_name(self, bot_service: BotService):
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "node_id": 0xA1B2C3D4,
            "short_name": "Hill",
            "long_name": "Hill Top",
        }
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            dest_id, label = bot_service._resolve_traceroute_destination(
                _ctx(args=["Hill"], raw_message="!traceroute Hill")
            )

        assert dest_id == 0xA1B2C3D4
        assert "Hill" in label
        cursor.execute.assert_called()
        sql = cursor.execute.call_args.args[0]
        assert "LOWER(short_name) = ?" in sql

    @pytest.mark.unit
    def test_resolve_unknown_short_name_returns_none(self, bot_service: BotService):
        cursor = MagicMock()
        # Exact short_name miss, then neighbors-style lookup miss
        cursor.fetchone.side_effect = [None, None]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.database.connection.get_db_connection", return_value=conn
        ):
            assert (
                bot_service._resolve_traceroute_destination(
                    _ctx(args=["ZZZZ"], raw_message="!traceroute ZZZZ")
                )
                is None
            )

    @pytest.mark.unit
    def test_cmd_traceroute_uses_resolved_destination(self, bot_service: BotService):
        publisher = MagicMock()
        publisher.is_connected = True
        publisher._interface = MagicMock()

        with patch(
            "src.malla.services.tcp_publisher.get_tcp_publisher",
            return_value=publisher,
        ):
            with patch.object(
                bot_service,
                "_resolve_traceroute_destination",
                return_value=(0xA1B2C3D4, "Hill"),
            ):
                with patch.object(
                    bot_service,
                    "_execute_traceroute",
                    return_value="🔍 TR to Hill...",
                ) as execute:
                    result = bot_service._cmd_traceroute(
                        _ctx(args=["Hill"], raw_message="!traceroute Hill")
                    )

        assert result == "🔍 TR to Hill..."
        execute.assert_called_once()
        assert execute.call_args.args[0] == 0xA1B2C3D4
        assert execute.call_args.args[1] == "Hill"
        assert execute.call_args.args[2] == 1
        assert execute.call_args.args[4] == 0x12345678

    @pytest.mark.unit
    def test_cmd_traceroute_reports_unknown_node(self, bot_service: BotService):
        publisher = MagicMock()
        publisher.is_connected = True
        publisher._interface = MagicMock()

        with patch(
            "src.malla.services.tcp_publisher.get_tcp_publisher",
            return_value=publisher,
        ):
            with patch.object(
                bot_service,
                "_resolve_traceroute_destination",
                return_value=None,
            ):
                result = bot_service._cmd_traceroute(
                    _ctx(args=["Nope"], raw_message="!traceroute Nope")
                )

        assert "not found" in result
        assert "Nope" in result
