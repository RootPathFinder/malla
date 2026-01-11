"""
Unit tests for node health service
"""

import time

import pytest

from malla.database import get_db_connection
from malla.services.node_health_service import NodeHealthService


@pytest.fixture
def db_with_test_data(temp_database, monkeypatch):
    """Create a database with test data for health analysis."""
    # Set the database path environment variable
    monkeypatch.setenv("MALLA_DATABASE_FILE", temp_database)

    conn = get_db_connection()
    cursor = conn.cursor()

    current_time = int(time.time())
    hours_24_ago = current_time - (24 * 3600)

    # Create test nodes
    test_nodes = [
        {
            "node_id": 1001,
            "hex_id": "!000003e9",
            "long_name": "Healthy Node",
            "short_name": "HN01",
            "hw_model": "HELTEC_V3",
            "role": "CLIENT",
        },
        {
            "node_id": 1002,
            "hex_id": "!000003ea",
            "long_name": "Poor Signal Node",
            "short_name": "PS01",
            "hw_model": "TBEAM",
            "role": "CLIENT",
        },
        {
            "node_id": 1003,
            "hex_id": "!000003eb",
            "long_name": "Inactive Node",
            "short_name": "IN01",
            "hw_model": "TLORA_V2",
            "role": "CLIENT",
        },
    ]

    for node in test_nodes:
        cursor.execute(
            """
            INSERT OR REPLACE INTO node_info (
                node_id, hex_id, long_name, short_name, hw_model, role,
                first_seen, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                node["node_id"],
                node["hex_id"],
                node["long_name"],
                node["short_name"],
                node["hw_model"],
                node["role"],
                hours_24_ago,
                current_time,
            ),
        )

    # Create packets for healthy node (good signal, regular activity)
    for i in range(100):
        timestamp = hours_24_ago + (i * 864)  # Spread over 24 hours
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
                1001,
                4294967295,
                1,
                "TEXT_MESSAGE_APP",
                "!00000001",
                -85,  # Good RSSI
                8.5,  # Good SNR
                3,
                3,
                True,
            ),
        )

    # Create packets for poor signal node (poor signal)
    for i in range(50):
        timestamp = hours_24_ago + (i * 1728)
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
                1002,
                4294967295,
                1,
                "TEXT_MESSAGE_APP",
                "!00000001",
                -120,  # Poor RSSI
                -8.0,  # Poor SNR
                3,
                3,
                True,
            ),
        )

    # Create only 2 packets for inactive node (very low activity)
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
            hours_24_ago,
            "test/topic",
            1003,
            4294967295,
            1,
            "TEXT_MESSAGE_APP",
            "!00000001",
            -90,
            5.0,
            3,
            3,
            True,
        ),
    )

    conn.commit()
    conn.close()

    yield temp_database

    # Cleanup is handled by temp_database fixture


class TestNodeHealthService:
    """Test suite for NodeHealthService."""

    def test_analyze_healthy_node(self, db_with_test_data):
        """Test analysis of a healthy node."""
        health_data = NodeHealthService.analyze_node_health(1001, hours=24)

        assert health_data is not None
        assert health_data["node_id"] == 1001
        assert health_data["health_score"] >= 80  # Should be healthy
        assert health_data["health_status"] == "healthy"
        assert health_data["metrics"]["total_packets"] == 100
        assert health_data["metrics"]["avg_rssi"] is not None
        assert health_data["metrics"]["avg_snr"] is not None
        # Should have no critical or warning issues
        critical_issues = [
            i for i in health_data["issues"] if i["severity"] == "critical"
        ]
        assert len(critical_issues) == 0

    def test_analyze_poor_signal_node(self, db_with_test_data):
        """Test analysis of a node with poor signal quality."""
        health_data = NodeHealthService.analyze_node_health(1002, hours=24)

        assert health_data is not None
        assert health_data["node_id"] == 1002
        # With new scoring: RSSI/SNR don't affect health, so node should be healthy
        # since it has good activity (50 packets) and gateway connectivity
        assert health_data["health_score"] >= 80  # Should be healthy
        # Should still have signal-related issues (informational)
        signal_issues = [i for i in health_data["issues"] if i["category"] == "signal"]
        assert len(signal_issues) > 0
        # Signal issues should be informational only
        for issue in signal_issues:
            assert issue["severity"] == "info"

    def test_analyze_inactive_node(self, db_with_test_data):
        """Test analysis of an inactive node."""
        health_data = NodeHealthService.analyze_node_health(1003, hours=24)

        assert health_data is not None
        assert health_data["node_id"] == 1003
        assert health_data["health_score"] < 80  # Should be degraded
        # Should have activity-related issues
        activity_issues = [
            i for i in health_data["issues"] if i["category"] == "activity"
        ]
        assert len(activity_issues) > 0
        assert health_data["metrics"]["total_packets"] < 5

    def test_analyze_nonexistent_node(self, db_with_test_data):
        """Test analysis of a node that doesn't exist."""
        health_data = NodeHealthService.analyze_node_health(9999, hours=24)

        assert health_data is None

    def test_get_problematic_nodes(self, db_with_test_data):
        """Test retrieval of problematic nodes."""
        problematic = NodeHealthService.get_problematic_nodes(
            hours=24, min_health_score=90, limit=10
        )

        assert isinstance(problematic, list)
        # Should find at least the poor signal and inactive nodes
        assert len(problematic) >= 2

        # Verify sorting (worst health score first)
        if len(problematic) > 1:
            for i in range(len(problematic) - 1):
                assert (
                    problematic[i]["health_score"] <= problematic[i + 1]["health_score"]
                )

        # Verify all returned nodes meet the criteria
        for node in problematic:
            assert node["health_score"] < 90

    def test_get_network_health_summary(self, db_with_test_data):
        """Test network-wide health summary."""
        summary = NodeHealthService.get_network_health_summary(hours=24)

        assert summary is not None
        assert "network_health_score" in summary
        assert "active_nodes" in summary
        assert summary["active_nodes"] >= 3  # We have 3 test nodes
        assert "health_distribution" in summary
        assert "healthy" in summary["health_distribution"]
        assert "degraded" in summary["health_distribution"]
        assert "poor" in summary["health_distribution"]
        assert "critical" in summary["health_distribution"]
        assert "network_metrics" in summary
        assert "avg_rssi" in summary["network_metrics"]
        assert "avg_snr" in summary["network_metrics"]

    def test_health_score_calculation(self, db_with_test_data):
        """Test that health scores are calculated correctly."""
        healthy_node = NodeHealthService.analyze_node_health(1001, hours=24)
        poor_signal_node = NodeHealthService.analyze_node_health(1002, hours=24)
        inactive_node = NodeHealthService.analyze_node_health(1003, hours=24)

        # With new scoring: RSSI/SNR don't affect health scores
        # Nodes 1001 and 1002 have similar activity, so should have similar scores
        # Both should be healthy since they have good packet activity
        assert healthy_node["health_score"] >= 80
        assert poor_signal_node["health_score"] >= 80
        
        # Inactive node should have worse score due to low activity
        assert inactive_node["health_score"] < healthy_node["health_score"]
        assert inactive_node["health_score"] < poor_signal_node["health_score"]

        # Health scores should be between 0 and 100
        assert 0 <= healthy_node["health_score"] <= 100
        assert 0 <= poor_signal_node["health_score"] <= 100
        assert 0 <= inactive_node["health_score"] <= 100

    def test_different_time_periods(self, db_with_test_data):
        """Test analysis with different time periods."""
        health_24h = NodeHealthService.analyze_node_health(1001, hours=24)
        health_12h = NodeHealthService.analyze_node_health(1001, hours=12)

        assert health_24h is not None
        assert health_12h is not None

        # Metrics should reflect the time period
        assert health_24h["analyzed_hours"] == 24
        assert health_12h["analyzed_hours"] == 12

        # 24h should have more packets than 12h
        assert (
            health_24h["metrics"]["total_packets"]
            > health_12h["metrics"]["total_packets"]
        )
