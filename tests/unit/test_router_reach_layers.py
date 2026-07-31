"""Unit tests for multi-router Reach layers used on Mesh Topology."""

from unittest.mock import patch

import pytest

from src.malla.services.neighbor_service import NeighborService


def _clear_reach_cache():
    NeighborService._topology_cache.clear()


def _fake_reach(node_id, *, limit=50, hours=168):
    return {
        "node_id": node_id,
        "node_name": f"Node-{node_id}",
        "neighbor_count": 0,
        "center": None,
        "summary": {"neighbor_count": 0, "with_location": 0},
        "neighbors": [],
    }


class TestRoleNormalization:
    @pytest.mark.unit
    def test_normalize_role_names_and_ids(self):
        assert NeighborService._normalize_device_role("router_client") == "ROUTER_CLIENT"
        assert NeighborService._normalize_device_role(2) == "ROUTER"
        assert NeighborService._normalize_device_role("4") == "REPEATER"
        assert NeighborService._is_reach_layer_role("CLIENT_BASE")
        assert not NeighborService._is_reach_layer_role("CLIENT")


class TestRouterCandidateDiscovery:
    @pytest.mark.unit
    def test_list_router_candidates_uses_last_updated_column(
        self, temp_database, monkeypatch
    ):
        """node_info has last_updated, not last_seen — discovery must not crash empty."""
        monkeypatch.setenv("MALLA_DATABASE_FILE", temp_database)
        _clear_reach_cache()
        candidates = NeighborService._list_router_candidates(limit=50)
        assert candidates, "Expected router-role nodes from fixture DB"
        assert all("node_id" in c and "role" in c for c in candidates)
        assert any(
            NeighborService._is_reach_layer_role(c.get("role")) for c in candidates
        )

        positioned = NeighborService._list_positioned_reach_role_ids(limit=50)
        assert isinstance(positioned, list)

        located, stats = NeighborService._resolve_located_routers(
            max_routers=10, topology={"nodes": [], "edges": []}
        )
        assert stats["router_candidates"] > 0
        assert stats["routers_with_location"] >= 1 or stats["locations_total"] >= 1
        assert isinstance(located, list)


class TestRouterReachLayers:
    @pytest.mark.unit
    def test_builds_layers_via_zero_hop_neighbors(self):
        """Router Reach must reuse node-detail get_zero_hop_neighbors."""
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
        locations = [
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
        ]

        def fake_reach(node_id, *, limit=50, hours=168):
            if node_id == 0x11111111:
                return {
                    "node_id": node_id,
                    "node_name": "HillTop",
                    "neighbor_count": 1,
                    "center": {"latitude": 34.1, "longitude": -118.2},
                    "summary": {
                        "max_distance_km": 2.5,
                        "neighbor_count": 1,
                        "with_location": 1,
                        "both_ways": 1,
                        "one_way": 0,
                    },
                    "neighbors": [
                        {
                            "node_id": 0xABCDEF01,
                            "hex_id": "!abcdef01",
                            "node_name": "ClientA",
                            "snr": 8.0,
                            "quality": "good",
                            "confirmed_both_ways": True,
                            "is_bidirectional": True,
                            "distance_km": 2.5,
                            "distance_display": "2.5 km",
                            "latitude": 34.12,
                            "longitude": -118.22,
                            "source_label": "NeighborInfo + Observed",
                            "sources": ["neighborinfo", "observed"],
                        }
                    ],
                }
            return {
                "node_id": node_id,
                "node_name": "Bridge",
                "neighbor_count": 0,
                "center": {"latitude": 34.2, "longitude": -118.3},
                "summary": {
                    "max_distance_km": None,
                    "neighbor_count": 0,
                    "with_location": 0,
                },
                "neighbors": [],
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
                return_value=locations,
            ),
            patch.object(
                NeighborService,
                "get_mesh_topology",
                return_value={"nodes": [], "edges": []},
            ),
            patch(
                "src.malla.services.traceroute_service.TracerouteService.get_network_graph_data",
                return_value={"links": []},
            ),
            patch.object(
                NeighborService, "get_zero_hop_neighbors", side_effect=fake_reach
            ) as zh,
        ):
            result = NeighborService.get_router_reach_layers(hours=24, max_routers=10)

        assert result["hours"] == 24
        assert result["statistics"]["router_candidates"] == 3
        assert result["statistics"]["routers_with_location"] == 2
        assert result["statistics"]["routers_mapped"] == 2
        assert result["statistics"]["mapped_reach_links"] == 1

        called_ids = sorted(c.args[0] for c in zh.call_args_list)
        assert called_ids == [0x11111111, 0x22222222]

        by_id = {r["node_id"]: r for r in result["routers"]}
        assert 0x33333333 not in by_id
        hill = by_id[0x11111111]
        assert hill["node_name"] == "HillTop"
        assert hill["color"].startswith("#")
        assert hill["mapped_neighbor_count"] == 1
        assert hill["neighbors"][0]["is_bidirectional"] is True
        assert hill["neighbors"][0]["latitude"] == 34.12
        assert hill["neighbors"][0]["source_label"] == "NeighborInfo + Observed"

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
                NeighborService,
                "get_mesh_topology",
                return_value={"nodes": [], "edges": []},
            ),
            patch(
                "src.malla.services.traceroute_service.TracerouteService.get_network_graph_data",
                return_value={"links": []},
            ),
            patch.object(
                NeighborService, "get_zero_hop_neighbors", side_effect=_fake_reach
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
            patch(
                "src.malla.services.traceroute_service.TracerouteService.get_network_graph_data",
                return_value={"links": []},
            ),
            patch.object(
                NeighborService, "get_zero_hop_neighbors", side_effect=_fake_reach
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
            patch(
                "src.malla.services.traceroute_service.TracerouteService.get_network_graph_data",
                return_value={"links": []},
            ),
            patch.object(
                NeighborService, "get_zero_hop_neighbors", side_effect=_fake_reach
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
        assert "direct (0-hop)" in html
        assert 'value="168" selected' in html
        # Node-detail Reach palette
        assert "#0d6efd" in html
        assert "#198754" in html
        assert "#fd7e14" in html
        assert "confirmed_both_ways" in html
