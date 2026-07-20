"""Unit tests for mesh bot traceroute parsing and formatting."""

import time
from unittest.mock import patch

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
    def test_format_chain_style(self, bot_service: BotService):
        result = bot_service._format_traceroute_result(
            route=[0x11111111],
            route_back=[0x11111111],
            snr_towards=[-5.0, -8.0],
            snr_back=[-7.0, -6.0],
            source_id=0x12345678,
            dest_id=0x87654321,
            style="chain",
        )

        assert "TR → 5678 > 1111(-5.0) > 4321(-8.0)" in result
        assert "← 4321 > 1111(-7.0) > 5678(-6.0)" in result

    @pytest.mark.unit
    def test_format_hops_style(self, bot_service: BotService):
        result = bot_service._format_traceroute_result(
            route=[0x11111111],
            route_back=[0x11111111],
            snr_towards=[-5.0, -8.0],
            snr_back=[-7.0, -6.0],
            source_id=0x12345678,
            dest_id=0x87654321,
            style="hops",
        )

        assert "TR to 4321 (2 hops)" in result
        assert "1 5678→1111 -5.0" in result
        assert "2 1111→4321 -8.0" in result
        assert "← 4321→1111 -7.0 →5678 -6.0" in result

    @pytest.mark.unit
    def test_format_names_style(self, bot_service: BotService):
        def fake_details(node_id: int):
            return {
                0x12345678: {"short_name": "Alpha", "long_name": "Alpha Node"},
                0x11111111: {"short_name": "Hill", "long_name": "Hill Top"},
                0x87654321: {"short_name": "You", "long_name": "Your Node"},
            }.get(node_id)

        with patch(
            "src.malla.database.repositories.NodeRepository.get_node_details",
            side_effect=fake_details,
        ):
            result = bot_service._format_traceroute_result(
                route=[0x11111111],
                route_back=[0x11111111],
                snr_towards=[-5.0, -8.0],
                snr_back=[-7.0, -6.0],
                source_id=0x12345678,
                dest_id=0x87654321,
                style="names",
            )

        assert "TR Alpha → Hill(-5.0) → You(-8.0)" in result
        assert "← You → Hill(-7.0) → Alpha(-6.0)" in result

    @pytest.mark.unit
    def test_format_chain_direct_hop(self, bot_service: BotService):
        result = bot_service._format_traceroute_result(
            route=[],
            route_back=[],
            snr_towards=[-4.0],
            snr_back=[],
            source_id=0x12345678,
            dest_id=0x87654321,
            style="chain",
        )

        assert result == "TR → 5678 > 4321(-4.0)"

    @pytest.mark.unit
    def test_handle_traceroute_packet_sends_formatted_response(
        self, bot_service: BotService
    ):
        dest_id = 0x87654321
        local_id = 0x12345678
        bot_service._traceroute_format = "chain"
        bot_service._pending_traceroutes[dest_id] = (
            dest_id,
            "Requester",
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
            with patch.object(bot_service, "queue_message") as queue_message:
                bot_service._handle_traceroute_packet(packet)

        queue_message.assert_called_once()
        response = queue_message.call_args.kwargs["text"]
        assert response.startswith("TR →")
        assert "5678 > 1111(-5.0) > 4321(-8.0)" in response
        assert dest_id not in bot_service._pending_traceroutes
