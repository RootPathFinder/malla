"""
Integration tests for node telemetry API endpoint.
"""

import json
import time

import pytest
from meshtastic import telemetry_pb2

from malla.database import get_db_connection


@pytest.mark.integration
class TestTelemetryEndpoint:
    """Test node telemetry API endpoint functionality."""

    def test_telemetry_endpoint_basic(self, client, temp_database):
        """Test basic telemetry endpoint functionality."""
        # Get a database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        node_id = 123456789

        # Insert node info
        cursor.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, short_name, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                "Test Node",
                "TEST",
                time.time(),
                time.time(),
            ),
        )

        # Create telemetry protobuf
        telemetry = telemetry_pb2.Telemetry()
        telemetry.device_metrics.battery_level = 85
        telemetry.device_metrics.voltage = 4200  # mV
        telemetry.device_metrics.channel_utilization = 15.5
        telemetry.device_metrics.air_util_tx = 3.2

        # Insert telemetry packet
        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id,
             processed_successfully, raw_payload, payload_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                "test/topic",
                node_id,
                node_id,
                3,  # TELEMETRY_APP
                "TELEMETRY_APP",
                f"!{node_id:08x}",
                1,
                telemetry.SerializeToString(),
                len(telemetry.SerializeToString()),
            ),
        )
        conn.commit()
        conn.close()

        # Fetch telemetry via API
        response = client.get(f"/api/node/{node_id}/telemetry")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "telemetry" in data
        assert data["telemetry"] is not None

        telemetry_data = data["telemetry"]
        assert "device_metrics" in telemetry_data
        assert telemetry_data["device_metrics"]["battery_level"] == 85
        assert telemetry_data["device_metrics"]["voltage"] == 4.2  # Converted to V
        assert telemetry_data["device_metrics"]["channel_utilization"] == 15.5
        assert abs(telemetry_data["device_metrics"]["air_util_tx"] - 3.2) < 0.01

        # Check timestamp fields
        assert "timestamp" in telemetry_data
        assert "timestamp_unix" in telemetry_data
        assert "timestamp_relative" in telemetry_data

    def test_telemetry_endpoint_no_data(self, client, temp_database):
        """Test telemetry endpoint when no telemetry data exists."""
        # Create a test node without telemetry
        conn = get_db_connection()
        cursor = conn.cursor()
        node_id = 987654321

        cursor.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, short_name, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                "Test Node",
                "TEST",
                time.time(),
                time.time(),
            ),
        )
        conn.commit()
        conn.close()

        # Fetch telemetry via API
        response = client.get(f"/api/node/{node_id}/telemetry")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "telemetry" in data
        assert data["telemetry"] is None
        assert "message" in data

    def test_telemetry_endpoint_environment_metrics(self, client, temp_database):
        """Test telemetry endpoint with environment metrics."""
        # Create a test node
        conn = get_db_connection()
        cursor = conn.cursor()
        node_id = 111222333

        cursor.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, short_name, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                "Env Node",
                "ENV",
                time.time(),
                time.time(),
            ),
        )

        # Create telemetry with environment metrics
        telemetry = telemetry_pb2.Telemetry()
        telemetry.environment_metrics.temperature = 22.5
        telemetry.environment_metrics.relative_humidity = 65.0
        telemetry.environment_metrics.barometric_pressure = 1013.25

        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id,
             processed_successfully, raw_payload, payload_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                "test/topic",
                node_id,
                node_id,
                3,
                "TELEMETRY_APP",
                f"!{node_id:08x}",
                1,
                telemetry.SerializeToString(),
                len(telemetry.SerializeToString()),
            ),
        )
        conn.commit()
        conn.close()

        # Fetch telemetry via API
        response = client.get(f"/api/node/{node_id}/telemetry")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "telemetry" in data
        telemetry_data = data["telemetry"]

        assert "environment_metrics" in telemetry_data
        assert telemetry_data["environment_metrics"]["temperature"] == 22.5
        assert telemetry_data["environment_metrics"]["relative_humidity"] == 65.0
        assert telemetry_data["environment_metrics"]["barometric_pressure"] == 1013.25

    def test_telemetry_endpoint_extra_sensor_and_power_fields(self, client, temp_database):
        """Env V/I, air-quality environmental PM, and battery health are exposed."""
        conn = get_db_connection()
        cursor = conn.cursor()
        node_id = 555666777
        analysis_ts = time.time() - 120

        cursor.execute(
            """
            INSERT INTO node_info (
                node_id, hex_id, long_name, short_name, first_seen, last_updated,
                power_type, power_type_reason, power_analysis_timestamp, battery_health_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                "Sensor Node",
                "SENS",
                time.time(),
                time.time(),
                "solar",
                "diurnal voltage pattern",
                analysis_ts,
                82,
            ),
        )

        env_tel = telemetry_pb2.Telemetry()
        env_tel.environment_metrics.temperature = 19.0
        env_tel.environment_metrics.voltage = 3.85
        env_tel.environment_metrics.current = 12.5

        aq_tel = telemetry_pb2.Telemetry()
        aq_tel.air_quality_metrics.pm25_standard = 8
        aq_tel.air_quality_metrics.pm25_environmental = 11
        aq_tel.air_quality_metrics.pm10_environmental = 6
        aq_tel.air_quality_metrics.pm100_environmental = 14

        now = time.time()
        for tel, ts in ((env_tel, now - 1), (aq_tel, now)):
            payload = tel.SerializeToString()
            cursor.execute(
                """
                INSERT INTO packet_history
                (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id,
                 processed_successfully, raw_payload, payload_length)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    "test/topic",
                    node_id,
                    node_id,
                    3,
                    "TELEMETRY_APP",
                    f"!{node_id:08x}",
                    1,
                    payload,
                    len(payload),
                ),
            )
        conn.commit()
        conn.close()

        response = client.get(f"/api/node/{node_id}/telemetry")
        assert response.status_code == 200
        telemetry_data = json.loads(response.data)["telemetry"]

        assert telemetry_data["power_type"] == "solar"
        assert telemetry_data["battery_health_score"] == 82
        assert telemetry_data["power_analysis_timestamp"] == analysis_ts
        assert telemetry_data["environment_metrics"]["voltage"] == pytest.approx(3.85)
        assert telemetry_data["environment_metrics"]["current"] == pytest.approx(12.5)
        assert telemetry_data["air_quality_metrics"]["pm25_standard"] == 8
        assert telemetry_data["air_quality_metrics"]["pm25_environmental"] == 11
        assert telemetry_data["air_quality_metrics"]["pm10_environmental"] == 6
        assert telemetry_data["air_quality_metrics"]["pm100_environmental"] == 14

    def test_telemetry_endpoint_hex_node_id(self, client, temp_database):
        """Test telemetry endpoint with hex node ID format."""
        # Create a test node
        conn = get_db_connection()
        cursor = conn.cursor()
        node_id = 444555666

        cursor.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, short_name, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                "Hex Node",
                "HEX",
                time.time(),
                time.time(),
            ),
        )

        # Create telemetry
        telemetry = telemetry_pb2.Telemetry()
        telemetry.device_metrics.battery_level = 50

        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id,
             processed_successfully, raw_payload, payload_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                "test/topic",
                node_id,
                node_id,
                3,
                "TELEMETRY_APP",
                f"!{node_id:08x}",
                1,
                telemetry.SerializeToString(),
                len(telemetry.SerializeToString()),
            ),
        )
        conn.commit()
        conn.close()

        # Fetch using hex format
        hex_id = f"!{node_id:08x}"
        response = client.get(f"/api/node/{hex_id}/telemetry")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "telemetry" in data
        assert data["telemetry"] is not None

    def test_telemetry_endpoint_latest_data(self, client, temp_database):
        """Test that endpoint returns only the latest telemetry."""
        # Create a test node
        conn = get_db_connection()
        cursor = conn.cursor()
        node_id = 777888999

        cursor.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, short_name, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                "Multi Node",
                "MULTI",
                time.time(),
                time.time(),
            ),
        )

        # Insert older telemetry
        old_telemetry = telemetry_pb2.Telemetry()
        old_telemetry.device_metrics.battery_level = 30
        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id,
             processed_successfully, raw_payload, payload_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time() - 3600,  # 1 hour ago
                "test/topic",
                node_id,
                node_id,
                3,
                "TELEMETRY_APP",
                f"!{node_id:08x}",
                1,
                old_telemetry.SerializeToString(),
                len(old_telemetry.SerializeToString()),
            ),
        )

        # Insert newer telemetry
        new_telemetry = telemetry_pb2.Telemetry()
        new_telemetry.device_metrics.battery_level = 90
        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name, gateway_id,
             processed_successfully, raw_payload, payload_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),  # Now
                "test/topic",
                node_id,
                node_id,
                3,
                "TELEMETRY_APP",
                f"!{node_id:08x}",
                1,
                new_telemetry.SerializeToString(),
                len(new_telemetry.SerializeToString()),
            ),
        )
        conn.commit()
        conn.close()

        # Fetch telemetry via API
        response = client.get(f"/api/node/{node_id}/telemetry")
        assert response.status_code == 200

        data = json.loads(response.data)
        telemetry_data = data["telemetry"]

        # Should get the latest (90%) not the old (30%)
        assert telemetry_data["device_metrics"]["battery_level"] == 90
