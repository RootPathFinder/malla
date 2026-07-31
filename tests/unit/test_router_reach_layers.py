"""Unit tests for multi-router Reach layers used on Mesh Topology."""

from unittest.mock import patch

import pytest

from src.malla.services.neighbor_service import NeighborService


def _clear_reach_cache():
    NeighborService._topology_cache.clear()


def _locs_for(ids, catalog):
    id_set = {int(i) for i in ids}
    return [loc for loc in catalog if int(loc["node_id"]) in id_set]


class TestRoleNormalization:
    @pytest.mark.unit
    def test_normalize_role_names_and_ids(self):
        assert NeighborService._normalize_device_role("router_client") == "ROUTER_CLIENT"
        assert NeighborService._normalize_device_role(2) == "ROUTER"
        assert NeighborService._normalize_device_role("4") == "REPEATER"
        assert NeighborService._is_reach_layer_role("CLIENT_BASE")
        assert not NeighborService._is_reach_layer_role("CLIENT")


class TestRouterReachLayers:
    @pytest.mark.unit
    def test_builds_layers_for_located_routers(self):
        _clear_reach_cache()
        candidates = [
            {
                "node_id": 0x11111111,
                "long_name": "HillTop",
                "short_name": "HILL",
                "role": "ROUTER",
                "last_seen": 1_700_000_000.0,
            },
            {
                "node_id": 0x22222222,
                "long_name": "Bridge",
                "short_name": "BRDG",
                "role": "ROUTER_CLIENT",
                "last_seen": 1_700_000_100.0,
            },
            {
                "node_id": 0x33333333,
                "long_name": "NoGPS",
                "short_name": "NGPS",
                "role": "REPEATER",
                "last_seen": 1_700_000_200.0,
            },
        ]
        location_catalog = [
            {
                "node_id": 0x11111111,
                "latitude": 34.1,
                "longitude": -118.2,
                "role": "ROUTER",
            },
            {
                "node_id": 0x22222222,
                "latitude": 34.2,
                "longitude": -118.3,
                "role": "ROUTER_CLIENT",
            },
            {
                "node_id": 0xABCDEF01,
                "latitude": 34.12,
                "longitude": -118.22,
                "role": "CLIENT",
            },
        ]

        def fake_locations(filters=None):
            if filters and filters.get("node_ids"):
                return _locs_for(filters["node_ids"], location_catalog)
            return []

        topology = {
            "nodes": [],
            "edges": [
                {
                    "node_a": 0x11111111,
                    "node_b": 0xABCDEF01,
                    "snr_a_to_b": 8.0,
                    "snr_b_to_a": 7.5,
                    "confirmed_both_ways": True,
                    "avg_snr": 7.75,
                    "last_seen": 1_700_000_050.0,
                }
            ],
        }

        with (
            patch.object(
                NeighborService, "_list_router_candidates", return_value=candidates
            ),
            patch.object(
                NeighborService, "_list_positioned_reach_role_ids", return_value=[]
            ),
            patch(
                "src.malla.database.repositories.LocationRepository.get_node_locations",
                side_effect=fake_locations,
            ),
            patch.object(
                NeighborService, "get_mesh_topology", return_value=topology
            ),
            patch.object(
                NeighborService,
                "_load_observed_zero_hop_peers_batch",
                return_value={0x11111111: {}, 0x22222222: {}},
            ),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                side_effect=lambda ids: {
                    0x11111111: "HillTop",
                    0x22222222: "Bridge",
                    0xABCDEF01: "ClientA",
                },
            ),
        ):
            result = NeighborService.get_router_reach_layers(hours=24, max_routers=10)

        assert result["hours"] == 24
        assert result["statistics"]["router_candidates"] == 3
        assert result["statistics"]["routers_with_location"] == 2
        assert result["statistics"]["routers_mapped"] == 2
        assert result["statistics"]["mapped_reach_links"] == 1

        by_id = {r["node_id"]: r for r in result["routers"]}
        assert 0x33333333 not in by_id
        hill = by_id[0x11111111]
        assert hill["node_name"] == "HillTop"
        assert hill["color"].startswith("#")
        assert hill["mapped_neighbor_count"] == 1
        assert hill["neighbors"][0]["is_bidirectional"] is True
        assert hill["neighbors"][0]["latitude"] == 34.12

    @pytest.mark.unit
    def test_positioned_role_ids_find_router_outside_candidate_window(self):
        """GPS routers must not be dropped just because last_seen ranking omitted them."""
        _clear_reach_cache()
        locations = [
            {
                "node_id": 0xABCDEF01,
                "latitude": 40.0,
                "longitude": -105.0,
                "role": "ROUTER",
                "display_name": "OldRouter",
                "timestamp": 1_600_000_000.0,
            }
        ]
        with (
            patch.object(NeighborService, "_list_router_candidates", return_value=[]),
            patch.object(
                NeighborService,
                "_list_positioned_reach_role_ids",
                return_value=[0xABCDEF01],
            ),
            patch(
                "src.malla.database.repositories.LocationRepository.get_node_locations",
                return_value=locations,
            ),
            patch.object(
                NeighborService, "get_mesh_topology", return_value={"nodes": [], "edges": []}
            ),
            patch.object(
                NeighborService,
                "_load_observed_zero_hop_peers_batch",
                return_value={0xABCDEF01: {}},
            ),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                return_value={0xABCDEF01: "OldRouter"},
            ),
        ):
            result = NeighborService.get_router_reach_layers(max_routers=10)

        assert len(result["routers"]) == 1
        assert result["routers"][0]["node_id"] == 0xABCDEF01
        assert result["routers"][0]["role"] == "ROUTER"

    @pytest.mark.unit
    def test_topology_fallback_for_is_router_nodes(self):
        _clear_reach_cache()
        locations_calls = []

        def fake_locations(filters=None):
            locations_calls.append(filters)
            if filters and filters.get("node_ids"):
                return [
                    {
                        "node_id": 0x10101010,
                        "latitude": 41.0,
                        "longitude": -74.0,
                        "role": None,
                    }
                ]
            return []

        with (
            patch.object(NeighborService, "_list_router_candidates", return_value=[]),
            patch.object(
                NeighborService, "_list_positioned_reach_role_ids", return_value=[]
            ),
            patch(
                "src.malla.database.repositories.LocationRepository.get_node_locations",
                side_effect=fake_locations,
            ),
            patch.object(
                NeighborService,
                "get_mesh_topology",
                return_value={
                    "nodes": [
                        {
                            "node_id": 0x10101010,
                            "name": "TopoRouter",
                            "hex_id": "!10101010",
                            "role": "CLIENT",
                            "is_router": True,
                            "last_seen": 1_700_000_000.0,
                        }
                    ],
                    "edges": [],
                },
            ),
            patch.object(
                NeighborService,
                "_load_observed_zero_hop_peers_batch",
                return_value={0x10101010: {}},
            ),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                return_value={0x10101010: "TopoRouter"},
            ),
        ):
            result = NeighborService.get_router_reach_layers(max_routers=10)

        assert len(result["routers"]) == 1
        assert result["routers"][0]["node_id"] == 0x10101010
        assert any(c and c.get("node_ids") for c in locations_calls)

    @pytest.mark.unit
    def test_empty_when_no_routers(self):
        _clear_reach_cache()
        with (
            patch.object(NeighborService, "_list_router_candidates", return_value=[]),
            patch.object(
                NeighborService, "_list_positioned_reach_role_ids", return_value=[]
            ),
            patch(
                "src.malla.database.repositories.LocationRepository.get_node_locations",
                return_value=[],
            ),
            patch.object(
                NeighborService,
                "get_mesh_topology",
                return_value={"nodes": [], "edges": []},
            ),
        ):
            result = NeighborService.get_router_reach_layers()
        assert result["routers"] == []
        assert result["statistics"]["routers_mapped"] == 0
        assert result["statistics"]["locations_total"] == 0

    @pytest.mark.unit
    def test_respects_max_routers(self):
        _clear_reach_cache()
        candidates = [
            {
                "node_id": i,
                "long_name": f"R{i}",
                "short_name": f"R{i}",
                "role": "ROUTER",
                "last_seen": 1_700_000_000.0 + i,
            }
            for i in range(1, 8)
        ]
        locations = [
            {
                "node_id": i,
                "latitude": 34.0 + i * 0.01,
                "longitude": -118.0,
                "role": "ROUTER",
            }
            for i in range(1, 8)
        ]

        with (
            patch.object(
                NeighborService, "_list_router_candidates", return_value=candidates
            ),
            patch.object(
                NeighborService, "_list_positioned_reach_role_ids", return_value=[]
            ),
            patch(
                "src.malla.database.repositories.LocationRepository.get_node_locations",
                return_value=locations,
            ),
            patch.object(
                NeighborService,
                "get_mesh_topology",
                return_value={"nodes": [], "edges": []},
            ),
            patch.object(
                NeighborService,
                "_load_observed_zero_hop_peers_batch",
                return_value={i: {} for i in range(1, 8)},
            ),
            patch(
                "src.malla.services.neighbor_service.get_bulk_node_names",
                side_effect=lambda ids: {i: f"R{i}" for i in ids},
            ),
        ):
            result = NeighborService.get_router_reach_layers(max_routers=3)

        assert len(result["routers"]) == 3
        assert result["statistics"]["routers_with_location"] == 7

    @pytest.mark.unit
    def test_uses_cache_within_ttl(self):
        _clear_reach_cache()
        with (
            patch.object(NeighborService, "_list_router_candidates", return_value=[]),
            patch.object(
                NeighborService, "_list_positioned_reach_role_ids", return_value=[]
            ),
            patch(
                "src.malla.database.repositories.LocationRepository.get_node_locations",
                return_value=[],
            ),
            patch.object(
                NeighborService,
                "get_mesh_topology",
                return_value={"nodes": [], "edges": []},
            ) as topo,
        ):
            first = NeighborService.get_router_reach_layers(hours=24, max_routers=5)
            second = NeighborService.get_router_reach_layers(hours=24, max_routers=5)
        assert first is second
        assert topo.call_count == 1


class TestRouterReachApi:
    @pytest.mark.unit
    def test_api_router_reach_returns_payload(self, client):
        payload = {
            "hours": 24,
            "routers": [
                {
                    "node_id": 1,
                    "hex_id": "!00000001",
                    "node_name": "Hill",
                    "role": "ROUTER",
                    "color": "#0d6efd",
                    "latitude": 1.0,
                    "longitude": 2.0,
                    "neighbor_count": 0,
                    "mapped_neighbor_count": 0,
                    "summary": {},
                    "neighbors": [],
                }
            ],
            "statistics": {
                "router_candidates": 1,
                "routers_with_location": 1,
                "routers_mapped": 1,
                "total_reach_links": 0,
                "mapped_reach_links": 0,
            },
        }
        with patch(
            "src.malla.routes.mesh_routes.NeighborService.get_router_reach_layers",
            return_value=payload,
        ) as mocked:
            resp = client.get("/mesh/api/router-reach?hours=24&max_routers=10")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["routers"][0]["node_name"] == "Hill"
        mocked.assert_called_once_with(hours=24, max_routers=10)


class TestRouterReachUi:
    @pytest.mark.unit
    def test_topology_page_has_router_reach_tab(self):
        from pathlib import Path

        html = (
            Path(__file__).resolve().parents[2]
            / "src/malla/templates/mesh_topology.html"
        ).read_text(encoding="utf-8")
        assert 'id="router-reach-tab"' in html
        assert 'id="router-reach-map"' in html
        assert "/mesh/api/router-reach" in html
        assert "loadRouterReach" in html
        assert "leaflet" in html.lower()
        assert "with GPS" in html
        # Node-detail Reach palette
        assert "#0d6efd" in html
        assert "#198754" in html
        assert "#fd7e14" in html
        assert "confirmed_both_ways" in html
