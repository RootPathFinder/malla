"""
Unit tests for AlertService voltage handling.
"""

import time

import pytest

from malla.database import get_db_connection
from malla.services.alert_service import AlertService


@pytest.fixture
def db_with_voltage_data(temp_database, monkeypatch):
    """Create a database with specific voltage test data."""
    # Set the database path environment variable
    monkeypatch.setenv("MALLA_DATABASE_FILE", temp_database)

    conn = get_db_connection()
    cursor = conn.cursor()

    current_time = int(time.time())

    # Create a test node
    node_id = 12345
    cursor.execute(
        """
        INSERT OR REPLACE INTO node_info (
            node_id, hex_id, long_name, short_name, hw_model, role,
            first_seen, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            node_id,
            f"!{node_id:08x}",
            "Voltage Test Node",
            "VTN",
            "HELTEC_V3",
            "CLIENT",
            current_time - 3600,
            current_time,
        ),
    )

    # Insert older valid telemetry (1 hour ago)
    # Voltage: 4.0V
    cursor.execute(
        """
        INSERT INTO telemetry_data (
            node_id, timestamp, battery_level, voltage,
            channel_utilization, air_util_tx
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (node_id, current_time - 3600, 80, 4.0, 10, 5),
    )

    # Insert newer invalid telemetry (now)
    # Voltage: 0.0V (simulating the issue)
    cursor.execute(
        """
        INSERT INTO telemetry_data (
            node_id, timestamp, battery_level, voltage,
            channel_utilization, air_util_tx
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            node_id,
            current_time,
            None,  # Maybe battery level is missing too
            0.0,  # Invalid voltage
            10,
            5,
        ),
    )

    conn.commit()
    conn.close()

    yield temp_database


class TestAlertServiceVoltage:
    """Test suite for AlertService voltage handling."""

    def test_get_node_health_summary_ignores_zero_voltage(self, db_with_voltage_data):
        """Test that get_node_health_summary ignores 0.00V readings."""

        # Get health summary
        summary = AlertService.get_node_health_summary()

        # Find our test node in the nodes needing attention
        nodes = summary.get("nodes_needing_attention", [])
        # Note: Healthy nodes might not be in "nodes_needing_attention" if they are healthy.
        # If the voltage is correctly read as 4.0V, it should be healthy and NOT in the list.
        # If it reads 0.00V incorrectly, it would be critical and IN the list.

        # Check if our test node (12345) is in "nodes_needing_attention"
        test_node = None
        for node in nodes:
            if node["node_id"] == 12345:
                test_node = node
                break

        if test_node:
            # If it's in the list, it shouldn't be because of voltage
            # (The older valid reading of 4.0V should be used, not 0.0V)
            print(f"Node found in attention list: {test_node}")
            assert "Critical voltage" not in str(test_node.get("issue", "")), (
                "Node should not have Critical voltage issue - 0.00V was incorrectly used"
            )
            assert "Low voltage" not in str(test_node.get("issue", "")), (
                "Node should not have Low voltage issue - 0.00V was incorrectly used"
            )
            assert test_node.get("value") != "0.00V", (
                "Node value should not be 0.00V - invalid reading was used"
            )
        else:
            # If not in list, it means it's healthy (voltage 4.0V > 3.4V)
            # This is the expected behavior
            pass

    def test_check_battery_health_ignores_zero_voltage(self, db_with_voltage_data):
        """Test that _check_battery_health ignores 0.00V readings."""

        # Run battery check
        AlertService._check_battery_health()

        # Should not generate alerts for 0.00V if ignored correctly
        # If it picked up 0.00V, it would generate a CRITICAL_BATTERY alert

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM alerts WHERE node_id = 12345")
        alerts = cursor.fetchall()
        conn.close()

        for alert in alerts:
            print(f"Alert generated: {alert['title']} - {alert['message']}")
            assert "Critical Voltage" not in alert["title"]
            assert "0.00V" not in alert["title"]
