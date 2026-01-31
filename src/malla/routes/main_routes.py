"""
Main routes for the Meshtastic Mesh Health Web UI
"""

import logging

from flask import Blueprint, render_template, request

# Import from the new modular architecture
from ..database.repositories import (
    DashboardRepository,
)

logger = logging.getLogger(__name__)
main_bp = Blueprint("main", __name__)


@main_bp.route("/")
def dashboard():
    """Dashboard route with network statistics."""
    try:
        # Get basic dashboard stats
        stats = DashboardRepository.get_stats()

        # Get gateway statistics from the new cached service
        from ..services.gateway_service import GatewayService

        gateway_stats = GatewayService.get_gateway_statistics(hours=24)
        gateway_count = gateway_stats.get("total_gateways", 0)

        return render_template(
            "dashboard.html",
            stats=stats,
            gateway_count=gateway_count,
        )
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        # Provide a graceful fallback with empty/default stats
        fallback_stats = {
            "total_nodes": 0,
            "active_nodes_24h": 0,
            "total_packets": 0,
            "recent_packets": 0,
            "success_rate": 0.0,
            "avg_rssi": 0.0,
            "avg_snr": 0.0,
            "packet_types": [],
        }
        return render_template(
            "dashboard.html",
            stats=fallback_stats,
            gateway_count=0,
            error_message="Unable to load dashboard data. Please check if the database is properly initialized.",
        )


@main_bp.route("/map")
def map_view():
    """Node location map view."""
    try:
        return render_template("map.html")
    except Exception as e:
        logger.error(f"Error in map route: {e}")
        return f"Map error: {e}", 500


@main_bp.route("/longest-links")
def longest_links():
    """Longest links analysis page."""
    logger.info("Longest links route accessed")
    try:
        return render_template("longest_links.html")
    except Exception as e:
        logger.error(f"Error in longest links route: {e}")
        return f"Longest links error: {e}", 500


@main_bp.route("/line-of-sight")
def line_of_sight():
    """Line of sight analysis tool page."""
    logger.info("Line of sight tool route accessed")
    try:
        # Get optional query parameters for pre-loading analysis
        from_node_id = request.args.get("from")
        to_node_id = request.args.get("to")

        return render_template(
            "line_of_sight.html", from_node_id=from_node_id, to_node_id=to_node_id
        )
    except Exception as e:
        logger.error(f"Error in line of sight route: {e}")
        return f"Line of sight error: {e}", 500


@main_bp.route("/weather-map")
def weather_map():
    """Mesh weather dashboard with sensor data on a map."""
    logger.info("Weather map route accessed")
    try:
        return render_template("weather_map.html")
    except Exception as e:
        logger.error(f"Error in weather map route: {e}")
        return f"Weather map error: {e}", 500


@main_bp.route("/network-dependency")
def network_dependency():
    """Network dependency analysis dashboard for impact assessment."""
    logger.info("Network dependency route accessed")
    try:
        return render_template("network_dependency.html")
    except Exception as e:
        logger.error(f"Error in network dependency route: {e}")
        return f"Network dependency error: {e}", 500


@main_bp.route("/detection-sensors")
def detection_sensors():
    """Detection sensor events dashboard."""
    logger.info("Detection sensors route accessed")
    try:
        return render_template("detection_sensors.html")
    except Exception as e:
        logger.error(f"Error in detection sensors route: {e}")
        return f"Detection sensors error: {e}", 500


@main_bp.route("/paxcounter")
def paxcounter():
    """Paxcounter dashboard for monitoring people/device counts."""
    logger.info("Paxcounter route accessed")
    try:
        return render_template("paxcounter.html")
    except Exception as e:
        logger.error(f"Error in paxcounter route: {e}")
        return f"Paxcounter error: {e}", 500
