"""
Battery analytics routes for power monitoring dashboard
"""

import logging

from flask import Blueprint, render_template

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

        return render_template(
            "battery_analytics.html",
            power_summary=power_summary,
            battery_health=battery_health,
            critical_batteries=critical_batteries,
            nodes_with_telemetry=nodes_with_telemetry,
        )
    except Exception as e:
        logger.error(f"Error loading battery analytics: {e}")
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
