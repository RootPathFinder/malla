"""
Battery analytics routes for power monitoring dashboard
"""

import logging
import sqlite3

from flask import Blueprint, jsonify, render_template

from ..database.connection import get_db_connection
from ..database.repositories import BatteryAnalyticsRepository

logger = logging.getLogger(__name__)
battery_bp = Blueprint("battery", __name__)


@battery_bp.route("/battery-analytics")
def battery_analytics():
    """Battery analytics dashboard page."""
    try:
        # Get power source summary
        power_summary = BatteryAnalyticsRepository.get_power_source_summary()
        logger.debug(f"Power summary: {power_summary}")

        # Get battery health overview
        battery_health = BatteryAnalyticsRepository.get_battery_health_overview()
        logger.debug(f"Battery health items: {len(battery_health) if battery_health else 0}")

        # Get critical battery alerts
        critical_batteries = BatteryAnalyticsRepository.get_critical_batteries()
        logger.debug(f"Critical batteries: {len(critical_batteries) if critical_batteries else 0}")

        # Get all nodes with battery telemetry
        nodes_with_telemetry = (
            BatteryAnalyticsRepository.get_nodes_with_battery_telemetry()
        )
        logger.info(
            f"Battery analytics loaded: {len(nodes_with_telemetry) if nodes_with_telemetry else 0} nodes with telemetry"
        )
        logger.debug(f"Nodes with telemetry: {nodes_with_telemetry}")

        return render_template(
            "battery_analytics.html",
            power_summary=power_summary,
            battery_health=battery_health,
            critical_batteries=critical_batteries,
            nodes_with_telemetry=nodes_with_telemetry,
        )
    except Exception as e:
        logger.error(f"Error loading battery analytics: {e}", exc_info=True)
        return render_template(
            "battery_analytics.html",
            error_message="Unable to load battery analytics data.",
            power_summary={
                "solar": 0,
                "battery": 0,
                "mains": 0,
                "unknown": 0,
            },
            battery_health=[],
            critical_batteries=[],
            nodes_with_telemetry=[],
        )


@battery_bp.route("/api/battery-debug", methods=["GET"])
def battery_debug():
    """Debug endpoint to check telemetry data in database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get counts
        cursor.execute("SELECT COUNT(*) as total FROM telemetry_data")
        total_telemetry = cursor.fetchone()["total"]

        cursor.execute(
            "SELECT COUNT(*) as total FROM telemetry_data WHERE voltage IS NOT NULL"
        )
        voltage_count = cursor.fetchone()["total"]

        cursor.execute(
            "SELECT COUNT(*) as total FROM telemetry_data WHERE battery_level IS NOT NULL"
        )
        battery_level_count = cursor.fetchone()["total"]

        cursor.execute("SELECT COUNT(*) as total FROM node_info")
        total_nodes = cursor.fetchone()["total"]

        # Get sample telemetry data
        cursor.execute(
            """
            SELECT DISTINCT
                td.node_id,
                ni.hex_id,
                ni.long_name,
                COUNT(*) as count,
                MAX(td.timestamp) as last_timestamp,
                MAX(td.voltage) as max_voltage,
                AVG(td.voltage) as avg_voltage,
                MAX(td.battery_level) as max_battery
            FROM telemetry_data td
            LEFT JOIN node_info ni ON td.node_id = ni.node_id
            GROUP BY td.node_id
            ORDER BY MAX(td.timestamp) DESC
            LIMIT 20
        """
        )

        telemetry_samples = []
        for row in cursor.fetchall():
            max_v = row["max_voltage"]
            avg_v = row["avg_voltage"]

            # Check if voltage might be incorrectly scaled
            # If max_voltage < 1, it might be in fractional volts (should be multiplied by 1000)
            if max_v and max_v < 1:
                max_v_scaled = max_v * 1000
                avg_v_scaled = avg_v * 1000 if avg_v else None
            else:
                max_v_scaled = max_v
                avg_v_scaled = avg_v

            telemetry_samples.append(
                {
                    "node_id": row["node_id"],
                    "hex_id": row["hex_id"],
                    "name": row["long_name"],
                    "telemetry_count": row["count"],
                    "last_timestamp": row["last_timestamp"],
                    "max_voltage": row["max_voltage"],
                    "avg_voltage": row["avg_voltage"],
                    "max_voltage_scaled": max_v_scaled,
                    "avg_voltage_scaled": avg_v_scaled,
                    "max_battery": row["max_battery"],
                }
            )

        conn.close()

        return jsonify(
            {
                "total_telemetry_records": total_telemetry,
                "records_with_voltage": voltage_count,
                "records_with_battery_level": battery_level_count,
                "total_nodes": total_nodes,
                "sample_nodes": telemetry_samples,
            }
        )

    except Exception as e:
        logger.error(f"Error in battery debug endpoint: {e}")
        return jsonify({"error": str(e)}), 500


@battery_bp.route("/api/battery-telemetry-nodes", methods=["GET"])
def battery_telemetry_nodes():
    """API endpoint to get nodes with battery telemetry."""
    try:
        nodes = BatteryAnalyticsRepository.get_nodes_with_battery_telemetry()
        logger.info(f"API returning {len(nodes) if nodes else 0} nodes with telemetry")
        return jsonify({
            "count": len(nodes) if nodes else 0,
            "nodes": nodes
        })
    except Exception as e:
        logger.error(f"Error getting telemetry nodes: {e}", exc_info=True)
        return jsonify({"error": str(e), "count": 0, "nodes": []}), 500


@battery_bp.route("/api/voltage-trends", methods=["GET"])
def voltage_trends():
    """API endpoint to get voltage trends for all nodes over the last 7 days."""
    try:
        import time
        from datetime import datetime, UTC

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get data for the last 7 days
        cutoff_time = time.time() - (7 * 24 * 3600)

        cursor.execute(
            """
            SELECT
                td.node_id,
                ni.long_name,
                ni.hex_id,
                td.timestamp,
                td.voltage,
                td.battery_level
            FROM telemetry_data td
            LEFT JOIN node_info ni ON td.node_id = ni.node_id
            WHERE td.voltage IS NOT NULL AND td.timestamp > ?
            ORDER BY td.node_id, td.timestamp
            """,
            (cutoff_time,)
        )

        rows = cursor.fetchall()
        conn.close()

        # Group data by node for charting
        nodes_data = {}
        for row in rows:
            node_id = row["node_id"]
            if node_id not in nodes_data:
                node_name = row["long_name"] or row["hex_id"] or f"Node {node_id}"
                nodes_data[node_id] = {
                    "name": node_name,
                    "timestamps": [],
                    "voltages": [],
                }

            # Scale voltage if needed
            voltage = row["voltage"]
            if voltage is not None and voltage < 1:
                voltage = voltage * 1000

            # Convert Unix timestamp to ISO format
            try:
                dt = datetime.fromtimestamp(row["timestamp"], tz=UTC)
                nodes_data[node_id]["timestamps"].append(dt.isoformat())
                nodes_data[node_id]["voltages"].append(voltage)
            except Exception as e:
                logger.warning(f"Could not parse timestamp {row['timestamp']}: {e}")

        logger.info(f"Voltage trends: {len(nodes_data)} nodes with data")
        return jsonify({
            "nodes": nodes_data,
            "count": len(nodes_data)
        })

    except Exception as e:
        logger.error(f"Error getting voltage trends: {e}", exc_info=True)
        return jsonify({"error": str(e), "nodes": {}, "count": 0}), 500
