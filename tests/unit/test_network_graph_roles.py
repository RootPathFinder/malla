"""Unit tests for network graph node role/name enrichment."""

from unittest.mock import MagicMock, patch

import pytest

from src.malla.services.traceroute_service import TracerouteService


@pytest.mark.unit
def test_network_graph_nodes_include_role_and_names():
    packet = {
        "id": 1,
        "timestamp": 1_700_000_000,
        "raw_payload": b"\x00",
    }

    hop = MagicMock()
    hop.from_node_id = 0x11111111
    hop.to_node_id = 0x22222222
    hop.from_node_name = "Fallback From"
    hop.to_node_name = "Fallback To"
    hop.snr = -5.0

    tr_packet = MagicMock()
    tr_packet.get_rf_hops.return_value = [hop]

    meta_rows = [
        {
            "node_id": 0x11111111,
            "short_name": "Hill",
            "long_name": "Hill Top Roof",
            "role": "ROUTER",
            "hw_model": "TBEAM",
        },
        {
            "node_id": 0x22222222,
            "short_name": "Ada",
            "long_name": "Ada Lake",
            "role": "CLIENT",
            "hw_model": "HELTEC_V3",
        },
    ]

    cursor = MagicMock()
    cursor.fetchall.return_value = meta_rows
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with (
        patch(
            "src.malla.database.repositories.TracerouteRepository.get_traceroute_packets",
            return_value={"packets": [packet]},
        ),
        patch(
            "src.malla.services.traceroute_service.TraceroutePacket",
            return_value=tr_packet,
        ),
        patch(
            "src.malla.database.repositories.LocationRepository.get_node_locations",
            return_value=[],
        ),
        patch(
            "src.malla.database.get_db_connection",
            return_value=conn,
        ),
    ):
        result = TracerouteService.get_network_graph_data(hours=1, min_snr=-200)

    nodes = {int(n["id"]): n for n in result["nodes"]}
    assert 0x11111111 in nodes
    assert nodes[0x11111111]["role"] == "ROUTER"
    assert nodes[0x11111111]["short_name"] == "Hill"
    assert nodes[0x11111111]["long_name"] == "Hill Top Roof"
    assert nodes[0x11111111]["name"] == "Hill Top Roof"
    assert nodes[0x22222222]["role"] == "CLIENT"
    assert nodes[0x22222222]["name"] == "Ada Lake"
