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

        # Get battery health overview
        battery_health = BatteryAnalyticsRepository.get_battery_health_overview()

        # Get critical battery alerts
        critical_batteries = BatteryAnalyticsRepository.get_critical_batteries()

        # Get all nodes with battery telemetry
        nodes_with_telemetry = (
            BatteryAnalyticsRepository.get_nodes_with_battery_telemetry()
        )

        logger.info(
            f"Battery analytics loaded: {len(nodes_with_telemetry)} nodes with telemetry"
        )

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
