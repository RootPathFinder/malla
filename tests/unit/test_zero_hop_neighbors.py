"""Unit tests for zero-hop neighbor aggregation used on node detail."""

from unittest.mock import MagicMock, patch

import pytest

from src.malla.services.neighbor_service import NeighborService


class TestZeroHopNeighbors:
    @pytest.mark.unit
    def test_merge_neighborinfo_and_observed(self):
        ni = {
            "has_data": True,
            "last_report": 1_700_000_000.0,
            "neighbors": [
                {
                    "node_id": 0x11111111,
                    "snr": 12.0,
                    "last_rx_time": 1_700_000_100.0,
                    "node_name": "Alpha",
                    "hex_id": "!11111111",
                    "quality": "excellent",
                }
            ],
        }
        observed = {
            0x11111111: {
                "node_id": 0x11111111,
                "packet_count": 8,
                "rssi_avg": -80.0,
                "snr_avg": 9.5,
                "last_seen": 1_700_000_200.0,
                "heard_from": True,
                "heard_by": True,
            },
            0x22222222: {
                "node_id": 0x22222222,
                "packet_count": 3,
                "rssi_avg": -95.0,
                "snr_avg": 2.0,
                "last_seen": 1_700_000_050.0,
                "heard_from": False,
                "heard_by": True,
            },
        }

        with (
            patch.object(NeighborService, "get_node_neighbors", return_value=ni),
            patch.object(
                NeighborService, "_apply_neighborinfo_both_ways", return_value=None
            ),
            patch.object(
                NeighborService, "_load_observed_zero_hop_peers", return_value=observed
            ),
            patch.object(
                NeighborService, "_load_traceroute_rf_peers", return_value={}
            ),
            patch.object(NeighborService, "_apply_peer_distances", return_value=None),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                return_value={
                    0xABCDEF01: "Self",
                    0x11111111: "Alpha",
                    0x22222222: "Beta",
                },
            ),
        ):
            result = NeighborService.get_zero_hop_neighbors(0xABCDEF01, hours=None)

        assert result["has_data"] is True
        assert result["neighbor_count"] == 2
        assert result["summary"]["neighbor_count"] == 2
        assert result["summary"]["both_ways"] == 1
        assert result["summary"]["one_way"] == 1
        assert result["summary"]["best_snr"] == 12.0
        by_id = {n["node_id"]: n for n in result["neighbors"]}

        alpha = by_id[0x11111111]
        assert alpha["source_label"] == "Reported + Observed"
        assert alpha["is_bidirectional"] is True
        assert alpha["rssi"] == -80.0
        assert alpha["snr"] == 12.0
        assert alpha["quality"] == "excellent"
        assert alpha["packet_count"] == 8
        assert alpha["direct_receptions_direction"] == "received"

        beta = by_id[0x22222222]
        assert beta["source_label"] == "Observed"
        assert beta["heard_by"] is True
        assert beta["heard_from"] is False
        assert beta["is_bidirectional"] is False
        assert beta["node_name"] == "Beta"
        assert beta["direct_receptions_direction"] == "transmitted"

        assert result["neighbors"][0]["node_id"] == 0x11111111

    @pytest.mark.unit
    def test_apply_peer_distances_attaches_positions_and_center(self):
        by_id = {
            0x11111111: {
                "node_id": 0x11111111,
                "distance_km": None,
                "distance_display": None,
            }
        }
        locations = [
            {"node_id": 0xABCDEF01, "latitude": 42.65, "longitude": -73.75},
            {"node_id": 0x11111111, "latitude": 42.66, "longitude": -73.74},
        ]
        with patch(
            "src.malla.database.repositories.LocationRepository.get_node_locations",
            return_value=locations,
        ):
            center = NeighborService._apply_peer_distances(0xABCDEF01, by_id)

        assert center == {"latitude": 42.65, "longitude": -73.75}
        peer = by_id[0x11111111]
        assert peer["latitude"] == 42.66
        assert peer["longitude"] == -73.74
        assert peer["distance_km"] is not None
        assert peer["distance_display"]

    @pytest.mark.unit
    def test_traceroute_only_peer_labeled(self):
        with (
            patch.object(
                NeighborService,
                "get_node_neighbors",
                return_value={"has_data": False, "neighbors": []},
            ),
            patch.object(
                NeighborService, "_apply_neighborinfo_both_ways", return_value=None
            ),
            patch.object(
                NeighborService, "_load_observed_zero_hop_peers", return_value={}
            ),
            patch.object(
                NeighborService,
                "_load_traceroute_rf_peers",
                return_value={
                    0x33333333: {
                        "node_id": 0x33333333,
                        "snr_avg": 4.0,
                        "packet_count": 2,
                        "last_seen": 1_700_000_000.0,
                    }
                },
            ),
            patch.object(NeighborService, "_apply_peer_distances", return_value=None),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                return_value={0x1: "Solo", 0x33333333: "Charlie"},
            ),
        ):
            result = NeighborService.get_zero_hop_neighbors(0x1, hours=24)

        assert result["neighbor_count"] == 1
        peer = result["neighbors"][0]
        assert peer["sources"] == ["traceroute"]
        assert peer["source_label"] == "Traceroute RF"
        assert peer["node_name"] == "Charlie"

    @pytest.mark.unit
    def test_confirmed_both_ways_from_topology(self):
        by_id = {
            0x11111111: {
                "node_id": 0x11111111,
                "hex_id": "!11111111",
                "node_name": "Alpha",
                "snr": 5.0,
                "rssi": None,
                "quality": "good",
                "packet_count": None,
                "last_seen": 100.0,
                "sources": ["neighborinfo"],
                "heard_from": False,
                "heard_by": False,
                "is_bidirectional": False,
                "confirmed_both_ways": False,
                "distance_km": None,
                "distance_display": None,
            }
        }
        topo = {
            "edges": [
                {
                    "node_a": 0xABCDEF01,
                    "node_b": 0x11111111,
                    "confirmed_both_ways": True,
                    "avg_snr": 11.0,
                    "last_seen": 200.0,
                    "node_a_name": "Self",
                    "node_b_name": "Alpha",
                }
            ]
        }
        with patch.object(NeighborService, "get_mesh_topology", return_value=topo):
            NeighborService._apply_neighborinfo_both_ways(
                0xABCDEF01, by_id, hours=24
            )

        assert by_id[0x11111111]["confirmed_both_ways"] is True
        assert by_id[0x11111111]["is_bidirectional"] is True
        assert by_id[0x11111111]["snr"] == 11.0

    @pytest.mark.unit
    def test_apply_peer_distances(self):
        by_id = {
            0x22222222: {
                "node_id": 0x22222222,
                "distance_km": None,
                "distance_display": None,
            }
        }
        locations = [
            {"node_id": 0x11111111, "latitude": 40.0, "longitude": -74.0},
            {"node_id": 0x22222222, "latitude": 40.001, "longitude": -74.0},
        ]
        with patch(
            "src.malla.database.repositories.LocationRepository.get_node_locations",
            return_value=locations,
        ):
            NeighborService._apply_peer_distances(0x11111111, by_id)

        assert by_id[0x22222222]["distance_km"] is not None
        assert by_id[0x22222222]["distance_display"] is not None
        assert "m" in by_id[0x22222222]["distance_display"]

    @pytest.mark.unit
    def test_empty_when_no_sources(self):
        with (
            patch.object(
                NeighborService,
                "get_node_neighbors",
                return_value={"has_data": False, "neighbors": []},
            ),
            patch.object(
                NeighborService, "_apply_neighborinfo_both_ways", return_value=None
            ),
            patch.object(
                NeighborService, "_load_observed_zero_hop_peers", return_value={}
            ),
            patch.object(
                NeighborService, "_load_traceroute_rf_peers", return_value={}
            ),
            patch.object(NeighborService, "_apply_peer_distances", return_value=None),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                return_value={0x1: "Solo"},
            ),
        ):
            result = NeighborService.get_zero_hop_neighbors(0x1)

        assert result["has_data"] is False
        assert result["neighbors"] == []
        assert result["neighbor_count"] == 0

    @pytest.mark.unit
    def test_classify_snr_quality(self):
        assert NeighborService._classify_snr_quality(12) == "excellent"
        assert NeighborService._classify_snr_quality(6) == "good"
        assert NeighborService._classify_snr_quality(1) == "fair"
        assert NeighborService._classify_snr_quality(-3) == "poor"
        assert NeighborService._classify_snr_quality(None) == "unknown"

    @pytest.mark.unit
    def test_source_label_combinations(self):
        assert (
            NeighborService._source_label(["neighborinfo", "observed"])
            == "Reported + Observed"
        )
        assert (
            NeighborService._source_label(["observed", "traceroute"])
            == "Observed · Traceroute RF"
        )
        assert NeighborService._source_label(["traceroute"]) == "Traceroute RF"

    @pytest.mark.unit
    def test_load_observed_merges_directions(self):
        heard_rows = [
            {
                "peer_id": 0xAAAABBBB,
                "packet_count": 2,
                "rssi_avg": -70.0,
                "snr_avg": 8.0,
                "last_seen": 100.0,
            }
        ]
        heard_by_rows = [
            {
                "gateway_id": "!aaaabbbb",
                "packet_count": 5,
                "rssi_avg": -75.0,
                "snr_avg": 10.0,
                "last_seen": 200.0,
            },
            {
                "gateway_id": "!cccccccc",
                "packet_count": 1,
                "rssi_avg": -90.0,
                "snr_avg": 1.0,
                "last_seen": 150.0,
            },
        ]

        cursor = MagicMock()
        cursor.fetchall.side_effect = [heard_rows, heard_by_rows]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch(
            "src.malla.services.neighbor_service.get_db_connection", return_value=conn
        ):
            peers = NeighborService._load_observed_zero_hop_peers(0x12345678, limit=10)

        assert 0xAAAABBBB in peers
        assert peers[0xAAAABBBB]["heard_from"] is True
        assert peers[0xAAAABBBB]["heard_by"] is True
        assert peers[0xAAAABBBB]["packet_count"] == 7
        assert peers[0xAAAABBBB]["snr_avg"] == 10.0
        assert peers[0xAAAABBBB]["last_seen"] == 200.0

        assert 0xCCCCCCCC in peers
        assert peers[0xCCCCCCCC]["heard_from"] is False
        assert peers[0xCCCCCCCC]["heard_by"] is True
