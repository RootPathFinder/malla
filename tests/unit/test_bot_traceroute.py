"""Unit tests for mesh bot traceroute parsing and formatting."""

import time
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

    @pytest.mark.unit
    def test_match_ignores_overheard_route_mention(self, bot_service: BotService):
        """Target listed mid-route on someone else's traceroute must not match."""
        dest_id = 0x87654321
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "Requester",
            1,
            time.time(),
        )

        matched = bot_service._match_pending_traceroute(
            from_id=0xAABBCCDD,
            to_id=0x12345678,
            request_id=7,
            route=[dest_id, 0x11111111],
            route_back=[],
            snr_towards=[-5.0, -6.0, -7.0],
            snr_back=[],
        )
        assert matched is None


class TestBotTracerouteCompleteness:
    @pytest.mark.unit
    def test_forward_complete_direct_and_multihop(self, bot_service: BotService):
        assert bot_service._traceroute_forward_complete([], [-4.0]) is True
        assert bot_service._traceroute_forward_complete([0x11111111], [-5.0, -8.0]) is True
        assert bot_service._traceroute_forward_complete([0x11111111], [-5.0]) is False
        assert bot_service._traceroute_forward_complete([0x11111111], []) is False
        assert bot_service._traceroute_forward_complete([], []) is False

    @pytest.mark.unit
    def test_timeout_scales_with_hop_limit(self, bot_service: BotService):
        bot_service._traceroute_hop_limit = 7
        bot_service._traceroute_timeout_base = 90.0
        bot_service._traceroute_timeout_per_hop = 25.0
        assert bot_service._get_traceroute_timeout() == 200.0

        bot_service._traceroute_hop_limit = 1
        assert bot_service._get_traceroute_timeout() == 90.0

    @pytest.mark.unit
    def test_incomplete_multihop_keeps_pending(self, bot_service: BotService):
        dest_id = 0x87654321
        local_id = 0x12345678
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "You",
            1,
            time.time(),
        )
        # Missing final SNR → incomplete for 1 intermediate hop
        payload = _build_route_discovery_payload(
            route=[0x11111111],
            snr_towards=[-5.0],
        )
        packet = {
            "from": dest_id,
            "to": local_id,
            "requestId": 99,
            "decoded": {"portnum": "TRACEROUTE_APP", "payload": payload},
        }

        with patch.object(bot_service, "_get_local_node_id", return_value=local_id):
            with patch.object(bot_service, "queue_message") as queue_message:
                bot_service._handle_traceroute_packet(packet)

        queue_message.assert_not_called()
        assert dest_id in bot_service._pending_traceroutes

    @pytest.mark.unit
    def test_complete_multihop_clears_pending(self, bot_service: BotService):
        dest_id = 0x87654321
        local_id = 0x12345678
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
            "decoded": {"portnum": "TRACEROUTE_APP", "payload": payload},
        }

        with patch.object(bot_service, "_get_local_node_id", return_value=local_id):
            with patch.object(
                bot_service, "_fetch_node_labels", return_value=_name_map()
            ):
                with patch.object(bot_service, "queue_message") as queue_message:
                    bot_service._handle_traceroute_packet(packet)

        queue_message.assert_called_once()
        assert dest_id not in bot_service._pending_traceroutes

    @pytest.mark.unit
    def test_send_hard_failure_notifies_and_clears(self, bot_service: BotService):
        dest_id = 0x87654321
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "You",
            1,
            time.time(),
        )
        publisher = MagicMock()
        publisher._interface = MagicMock()
        publisher._interface.sendTraceRoute.side_effect = RuntimeError("radio busy")

        with patch.object(bot_service, "queue_message") as queue_message:
            bot_service._send_traceroute_packet(
                dest_id, publisher, channel_index=1, sender_name="You"
            )

        assert dest_id not in bot_service._pending_traceroutes
        queue_message.assert_called_once()
        assert "failed" in queue_message.call_args.kwargs["text"].lower()

    @pytest.mark.unit
    def test_library_wait_timeout_keeps_pending(self, bot_service: BotService):
        dest_id = 0x87654321
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "You",
            1,
            time.time(),
        )
        publisher = MagicMock()
        publisher._interface = MagicMock()
        publisher._interface.sendTraceRoute.side_effect = RuntimeError(
            "Timed out waiting for traceroute"
        )

        with patch.object(bot_service, "queue_message") as queue_message:
            bot_service._send_traceroute_packet(
                dest_id, publisher, channel_index=1, sender_name="You"
            )

        assert dest_id in bot_service._pending_traceroutes
        queue_message.assert_not_called()
        publisher._interface.sendTraceRoute.assert_called_once_with(
            dest=dest_id, hopLimit=7, channelIndex=1
        )


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
