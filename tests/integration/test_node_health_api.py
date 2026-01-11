"""
Integration tests for node health API endpoints
"""

import time

import pytest


class TestNodeHealthAPIEndpoints:
    """Test suite for node health API endpoints."""

    @pytest.fixture
    def db_with_health_data(self, client):
        """Create database with test data for health endpoints."""
        from malla.database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        current_time = int(time.time())
        hours_24_ago = current_time - (24 * 3600)

        # Create test node
        cursor.execute(
            """
            INSERT INTO node_info (
                node_id, hex_id, long_name, short_name, hw_model, role,
                first_seen, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                2001,
                "!000007d1",
                "Test Node",
                "TN01",
                "HELTEC_V3",
                "CLIENT",
                hours_24_ago,
                current_time,
            ),
        )

        # Create packets with varying signal quality
        for i in range(50):
            timestamp = hours_24_ago + (i * 1728)
            rssi = -85 if i < 40 else -120  # Last 10 packets have poor signal
            snr = 8.0 if i < 40 else -8.0

            cursor.execute(
                """
                INSERT INTO packet_history (
                    timestamp, topic, from_node_id, to_node_id,
                    portnum, portnum_name, gateway_id,
                    rssi, snr, hop_limit, hop_start,
                    processed_successfully
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    timestamp,
                    "test/topic",
                    2001,
                    4294967295,
                    1,
                    "TEXT_MESSAGE_APP",
                    "!00000001",
                    rssi,
                    snr,
                    3,
                    3,
                    True,
                ),
            )

        conn.commit()
        conn.close()

        yield client

    @pytest.mark.integration
    @pytest.mark.api
    @pytest.mark.integration
    @pytest.mark.api
    def test_api_node_health(self, db_with_health_data):
        """Test /api/health/node/<node_id> endpoint."""
        response = db_with_health_data.get("/api/health/node/2001")
        assert response.status_code == 200

        data = response.get_json()
        assert data is not None
        assert "node_id" in data
        assert data["node_id"] == 2001
        assert "health_score" in data
        assert "health_status" in data
        assert "issues" in data
        assert "metrics" in data
        assert "analyzed_hours" in data

        # Verify metrics structure
        metrics = data["metrics"]
        assert "total_packets" in metrics
        assert "avg_rssi" in metrics
        assert "avg_snr" in metrics
        assert "unique_gateways" in metrics

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_node_health_with_hours_param(self, db_with_health_data):
        """Test node health endpoint with custom hours parameter."""
        response = db_with_health_data.get("/api/health/node/2001?hours=12")
        assert response.status_code == 200

        data = response.get_json()
        assert data["analyzed_hours"] == 12

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_node_health_nonexistent(self, db_with_health_data):
        """Test node health endpoint for non-existent node."""
        response = db_with_health_data.get("/api/health/node/9999")
        assert response.status_code == 404

        data = response.get_json()
        assert "error" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_node_health_hex_id(self, db_with_health_data):
        """Test node health endpoint with hex ID."""
        response = db_with_health_data.get("/api/health/node/!000007d1")
        assert response.status_code == 200

        data = response.get_json()
        assert data["node_id"] == 2001

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_problematic_nodes(self, db_with_health_data):
        """Test /api/health/problematic-nodes endpoint."""
        response = db_with_health_data.get("/api/health/problematic-nodes")
        assert response.status_code == 200

        data = response.get_json()
        assert data is not None
        assert "problematic_nodes" in data
        assert "total_count" in data
        assert "filters" in data

        # Verify filters
        assert data["filters"]["hours"] == 24  # Default
        assert data["filters"]["min_health_score"] == 70  # Default
        assert data["filters"]["limit"] == 50  # Default

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_problematic_nodes_with_params(self, db_with_health_data):
        """Test problematic nodes endpoint with custom parameters."""
        response = db_with_health_data.get(
            "/api/health/problematic-nodes?hours=12&min_health_score=80&limit=10"
        )
        assert response.status_code == 200

        data = response.get_json()
        assert data["filters"]["hours"] == 12
        assert data["filters"]["min_health_score"] == 80
        assert data["filters"]["limit"] == 10

        # Verify all returned nodes meet criteria
        for node in data["problematic_nodes"]:
            assert node["health_score"] < 80

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_network_health_summary(self, db_with_health_data):
        """Test /api/health/network-summary endpoint."""
        response = db_with_health_data.get("/api/health/network-summary")
        assert response.status_code == 200

        data = response.get_json()
        assert data is not None
        assert "network_health_score" in data
        assert "active_nodes" in data
        assert "health_distribution" in data
        assert "network_metrics" in data
        assert "analyzed_hours" in data
        assert "timestamp" in data

        # Verify health distribution structure
        dist = data["health_distribution"]
        assert "healthy" in dist
        assert "degraded" in dist
        assert "poor" in dist
        assert "critical" in dist

        # Verify network metrics structure
        metrics = data["network_metrics"]
        assert "avg_rssi" in metrics
        assert "avg_snr" in metrics
        assert "total_packets" in metrics
        assert "poor_signal_nodes" in metrics
        assert "isolated_nodes" in metrics

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_network_health_summary_caching(self, db_with_health_data):
        """Test that network health summary endpoint returns cache headers."""
        response = db_with_health_data.get("/api/health/network-summary")
        assert response.status_code == 200

        # Check for cache headers
        assert "Cache-Control" in response.headers

    @pytest.mark.integration
    @pytest.mark.api
    def test_api_network_health_summary_with_hours(self, db_with_health_data):
        """Test network health summary with custom hours parameter."""
        response = db_with_health_data.get("/api/health/network-summary?hours=6")
        assert response.status_code == 200

        data = response.get_json()
        assert data["analyzed_hours"] == 6

    @pytest.mark.integration
    @pytest.mark.api
    def test_node_health_route(self, db_with_health_data):
        """Test that the /node-health route is accessible."""
        response = db_with_health_data.get("/node-health")
        assert response.status_code == 200
        assert b"Node Health" in response.data

    @pytest.mark.integration
    @pytest.mark.api
    def test_health_data_structure_consistency(self, db_with_health_data):
        """Test that all health-related endpoints return consistent data structures."""
        # Get individual node health
        node_response = db_with_health_data.get("/api/health/node/2001")
        node_data = node_response.get_json()

        # Get problematic nodes
        prob_response = db_with_health_data.get(
            "/api/health/problematic-nodes?min_health_score=100"
        )
        prob_data = prob_response.get_json()

        # If our test node appears in problematic nodes, verify structure matches
        if prob_data["problematic_nodes"]:
            prob_node = next(
                (n for n in prob_data["problematic_nodes"] if n["node_id"] == 2001),
                None,
            )
            if prob_node:
                # Verify same structure
                assert set(node_data.keys()) == set(prob_node.keys())
                assert set(node_data["metrics"].keys()) == set(
                    prob_node["metrics"].keys()
                )
