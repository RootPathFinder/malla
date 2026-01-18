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

        # Find our test node
        nodes = summary.get("nodes_needing_attention", [])
        # Note: Healthy nodes might not be in "nodes_needing_attention" if they are healthy.
        # But wait, get_node_health_summary returns counts and a list of nodes needing attention.
        # If the voltage is correctly read as 4.0V, it should be healthy and NOT in the list (unless offline).
        # If it reads 0.00V, it would be critical and IN the list.

        # Let's check the counts first
        assert summary["total"] == 1

        # If fixed, it should be healthy (voltage 4.0V > 3.4V)
        # If broken, it would be critical (voltage 0.0V < 3.2V)

        # Check if node is in "nodes_needing_attention"
        # If it is, check why.

        attention_node = None
        for node in nodes:
            if node["node_id"] == 12345:
                attention_node = node
                break

        if attention_node:
            # If it's in the list, it shouldn't be because of voltage
            print(f"Node found in attention list: {attention_node}")
            assert "Critical voltage" not in str(attention_node.get("issue", ""))
            assert "Low voltage" not in str(attention_node.get("issue", ""))
            assert attention_node["value"] != "0.00V"
        else:
            # If not in list, it means it's healthy, which implies voltage was read correctly as > 3.4V
            assert summary["healthy"] == 1
            assert summary["critical"] == 0

    def test_check_battery_health_ignores_zero_voltage(self, db_with_voltage_data):
        """Test that _check_battery_health ignores 0.00V readings."""

        # Run battery check
        results = AlertService._check_battery_health()

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
