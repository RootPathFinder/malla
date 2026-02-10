"""
Integration tests for the custom dashboard feature.

Tests cover:
- Dashboard page rendering
- Batch telemetry API endpoint
- Node search API endpoint
- Telemetry history API endpoint
- Error handling and validation
"""

import json

import pytest


class TestCustomDashboardPage:
    """Test the custom dashboard page route."""

    @pytest.mark.integration
    def test_custom_dashboard_page_loads(self, client):
        """Test that the custom dashboard page loads successfully."""
        response = client.get("/custom-dashboard")
        assert response.status_code == 200
        assert b"Custom Dashboard" in response.data

    @pytest.mark.integration
    def test_custom_dashboard_has_required_elements(self, client):
        """Test that the dashboard page includes required JS and CSS."""
        response = client.get("/custom-dashboard")
        assert response.status_code == 200
        assert b"custom-dashboard.js" in response.data
        assert b"custom-dashboard.css" in response.data
        assert b"dashboard-toolbar" in response.data
        assert b"widget-grid" in response.data


class TestBatchNodeTelemetry:
    """Test the batch node telemetry API endpoint."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_basic(self, client):
        """Test batch telemetry with valid node IDs."""
        # Get a node ID from the test database
        nodes_response = client.get("/api/nodes?limit=1")
        assert nodes_response.status_code == 200
        nodes_data = nodes_response.get_json()

        if nodes_data.get("nodes") and len(nodes_data["nodes"]) > 0:
            node = nodes_data["nodes"][0]
            node_hex = node.get("hex_id", "!00000001")

            response = client.post(
                "/api/custom-dashboard/nodes/telemetry",
                data=json.dumps({"node_ids": [node_hex]}),
                content_type="application/json",
            )
            assert response.status_code == 200
            data = response.get_json()
            assert "nodes" in data
            assert node_hex in data["nodes"]

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_multiple_nodes(self, client):
        """Test batch telemetry with multiple node IDs."""
        # Get multiple nodes from the test database
        nodes_response = client.get("/api/nodes?limit=3")
        assert nodes_response.status_code == 200
        nodes_data = nodes_response.get_json()

        if nodes_data.get("nodes") and len(nodes_data["nodes"]) >= 2:
            node_ids = [n.get("hex_id") for n in nodes_data["nodes"][:3] if n.get("hex_id")]

            response = client.post(
                "/api/custom-dashboard/nodes/telemetry",
                data=json.dumps({"node_ids": node_ids}),
                content_type="application/json",
            )
            assert response.status_code == 200
            data = response.get_json()
            assert "nodes" in data
            for nid in node_ids:
                assert nid in data["nodes"]

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_missing_body(self, client):
        """Test batch telemetry with missing request body."""
        response = client.post(
            "/api/custom-dashboard/nodes/telemetry",
            content_type="application/json",
        )
        assert response.status_code == 400

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_missing_node_ids(self, client):
        """Test batch telemetry with missing node_ids field."""
        response = client.post(
            "/api/custom-dashboard/nodes/telemetry",
            data=json.dumps({"something_else": []}),
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_empty_node_ids(self, client):
        """Test batch telemetry with empty node_ids list."""
        response = client.post(
            "/api/custom-dashboard/nodes/telemetry",
            data=json.dumps({"node_ids": []}),
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_too_many_nodes(self, client):
        """Test batch telemetry with more than 50 nodes."""
        node_ids = [f"!{i:08x}" for i in range(51)]
        response = client.post(
            "/api/custom-dashboard/nodes/telemetry",
            data=json.dumps({"node_ids": node_ids}),
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data
        assert "50" in data["error"]

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_invalid_node_id(self, client):
        """Test batch telemetry with an invalid node ID format."""
        response = client.post(
            "/api/custom-dashboard/nodes/telemetry",
            data=json.dumps({"node_ids": ["not_a_valid_id"]}),
            content_type="application/json",
        )
        assert response.status_code == 200
        data = response.get_json()
        assert "nodes" in data
        # Invalid node should have an error entry
        assert "not_a_valid_id" in data["nodes"]
        node_data = data["nodes"]["not_a_valid_id"]
        assert "error" in node_data

    @pytest.mark.integration
    @pytest.mark.api
    def test_batch_telemetry_response_structure(self, client):
        """Test that batch telemetry response has the expected structure."""
        # Get a valid node
        nodes_response = client.get("/api/nodes?limit=1")
        nodes_data = nodes_response.get_json()

        if nodes_data.get("nodes") and len(nodes_data["nodes"]) > 0:
            node_hex = nodes_data["nodes"][0].get("hex_id", "!00000001")

            response = client.post(
                "/api/custom-dashboard/nodes/telemetry",
                data=json.dumps({"node_ids": [node_hex]}),
                content_type="application/json",
            )
            assert response.status_code == 200
            data = response.get_json()

            node_entry = data["nodes"][node_hex]
            assert "node_info" in node_entry
            # telemetry may be None if no telemetry data exists
            assert "telemetry" in node_entry


class TestDashboardNodeSearch:
    """Test the dashboard node search API endpoint."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_search_basic(self, client):
        """Test basic node search functionality."""
        response = client.get("/api/custom-dashboard/nodes/search?q=test")
        assert response.status_code == 200
        data = response.get_json()
        assert "nodes" in data
        assert isinstance(data["nodes"], list)

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_search_empty_query(self, client):
        """Test node search with empty query returns recent nodes."""
        response = client.get("/api/custom-dashboard/nodes/search?q=")
        assert response.status_code == 200
        data = response.get_json()
        assert "nodes" in data
        assert isinstance(data["nodes"], list)

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_search_no_query(self, client):
        """Test node search with no query parameter."""
        response = client.get("/api/custom-dashboard/nodes/search")
        assert response.status_code == 200
        data = response.get_json()
        assert "nodes" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_search_with_limit(self, client):
        """Test node search with limit parameter."""
        response = client.get("/api/custom-dashboard/nodes/search?q=&limit=5")
        assert response.status_code == 200
        data = response.get_json()
        assert "nodes" in data
        assert len(data["nodes"]) <= 5

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_search_result_structure(self, client):
        """Test that search results have the expected structure."""
        response = client.get("/api/custom-dashboard/nodes/search?q=")
        data = response.get_json()

        if data["nodes"] and len(data["nodes"]) > 0:
            node = data["nodes"][0]
            # Check for expected fields
            assert "node_id" in node
            assert "hex_id" in node

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_search_by_hex_id(self, client):
        """Test node search with hex ID prefix."""
        response = client.get("/api/custom-dashboard/nodes/search?q=!")
        assert response.status_code == 200
        data = response.get_json()
        assert "nodes" in data


class TestDashboardTelemetryHistory:
    """Test the dashboard telemetry history API endpoint."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_telemetry_history_basic(self, client):
        """Test telemetry history with a valid node ID."""
        # Get a node ID
        nodes_response = client.get("/api/nodes?limit=1")
        nodes_data = nodes_response.get_json()

        if nodes_data.get("nodes") and len(nodes_data["nodes"]) > 0:
            node_hex = nodes_data["nodes"][0].get("hex_id", "!00000001")

            response = client.get(
                f"/api/custom-dashboard/node/{node_hex}/telemetry/history"
            )
            assert response.status_code == 200
            data = response.get_json()
            assert "history" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_telemetry_history_with_hours(self, client):
        """Test telemetry history with custom hours parameter."""
        nodes_response = client.get("/api/nodes?limit=1")
        nodes_data = nodes_response.get_json()

        if nodes_data.get("nodes") and len(nodes_data["nodes"]) > 0:
            node_hex = nodes_data["nodes"][0].get("hex_id", "!00000001")

            response = client.get(
                f"/api/custom-dashboard/node/{node_hex}/telemetry/history?hours=48"
            )
            assert response.status_code == 200
            data = response.get_json()
            assert "history" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_telemetry_history_hours_clamped(self, client):
        """Test that hours parameter is clamped to valid range."""
        nodes_response = client.get("/api/nodes?limit=1")
        nodes_data = nodes_response.get_json()

        if nodes_data.get("nodes") and len(nodes_data["nodes"]) > 0:
            node_hex = nodes_data["nodes"][0].get("hex_id", "!00000001")

            # Test exceeding max (168)
            response = client.get(
                f"/api/custom-dashboard/node/{node_hex}/telemetry/history?hours=999"
            )
            assert response.status_code == 200

            # Test below min (1)
            response = client.get(
                f"/api/custom-dashboard/node/{node_hex}/telemetry/history?hours=0"
            )
            assert response.status_code == 200

    @pytest.mark.integration
    @pytest.mark.api
    def test_telemetry_history_invalid_node(self, client):
        """Test telemetry history with invalid node ID."""
        response = client.get(
            "/api/custom-dashboard/node/invalid_id/telemetry/history"
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data


class TestCustomDashboardNavigation:
    """Test that the custom dashboard appears in navigation."""

    @pytest.mark.integration
    def test_nav_link_present(self, client):
        """Test that the custom dashboard link is in the navigation."""
        response = client.get("/")
        assert response.status_code == 200
        assert b"Custom Dashboard" in response.data
        assert b"custom-dashboard" in response.data
