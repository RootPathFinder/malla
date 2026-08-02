"""
Main routes for the Meshtastic Mesh Health Web UI
"""

import logging
from pathlib import Path

from flask import Blueprint, current_app, render_template, request, send_from_directory

# Import from the new modular architecture
from ..database.repositories import (
    DashboardRepository,
)

logger = logging.getLogger(__name__)
main_bp = Blueprint("main", __name__)


@main_bp.route("/sw.js")
def service_worker():
    """Serve the service worker from root scope for browser notifications."""
    static_folder = Path(current_app.static_folder or "")
    response = send_from_directory(static_folder, "sw.js")
    response.headers["Content-Type"] = "application/javascript; charset=utf-8"
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-cache"
    return response


@main_bp.route("/manifest.webmanifest")
def web_manifest():
    """PWA manifest (helps mobile install / notification support)."""
    static_folder = Path(current_app.static_folder or "")
    response = send_from_directory(static_folder, "manifest.webmanifest")
    response.headers["Content-Type"] = "application/manifest+json"
    return response


@main_bp.route("/")
def dashboard():
    """Dashboard route with network statistics."""
    try:
        # Get basic dashboard stats
        stats = DashboardRepository.get_stats()
        last_24h = DashboardRepository.get_last_24h_summary()

        # Get gateway statistics from the new cached service
        from ..services.gateway_service import GatewayService

        gateway_stats = GatewayService.get_gateway_statistics(hours=24)
        gateway_count = gateway_stats.get("total_gateways", 0)

        return render_template(
            "dashboard.html",
            stats=stats,
            last_24h=last_24h,
            gateway_count=gateway_count,
        )
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        # Provide a graceful fallback with empty/default stats
        fallback_stats = {
            "total_nodes": 0,
            "active_nodes_24h": 0,
            "total_packets": 0,
            "packets_24h": 0,
            "recent_packets": 0,
            "success_rate": 0.0,
            "avg_rssi": 0.0,
            "avg_snr": 0.0,
            "packet_types": [],
        }
        fallback_24h = {
            "packets_24h": 0,
            "packets_prior_24h": 0,
            "packets_trend_pct": 0.0,
            "text_messages_24h": 0,
            "active_nodes_24h": 0,
            "active_nodes_prior_24h": 0,
            "active_nodes_delta": 0,
            "new_nodes_24h": 0,
            "new_node_names": [],
            "avg_snr": 0.0,
            "avg_rssi": 0.0,
            "decode_success_rate": 0.0,
            "gateways_24h": 0,
            "protocol_types_24h": 0,
            "direct_packets": 0,
            "relayed_packets": 0,
            "low_battery_nodes": 0,
            "top_talkers": [],
            "farthest_node": None,
            "hourly": [{"hour": h, "packets": 0} for h in range(24)],
            "timezone": "UTC",
        }
        return render_template(
            "dashboard.html",
            stats=fallback_stats,
            last_24h=fallback_24h,
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


@main_bp.route("/coverage-map")
def coverage_map():
    """Coverage map builder for multi-node RF coverage visualization."""
    logger.info("Coverage map route accessed")
    try:
        return render_template("coverage_map.html")
    except Exception as e:
        logger.error(f"Error in coverage map route: {e}")
        return f"Coverage map error: {e}", 500


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
    """Redirect to sensor dashboard for backwards compatibility."""
    from flask import redirect, url_for

    return redirect(url_for("main.sensor_dashboard", sensor_type="detection"))


@main_bp.route("/sensor-dashboard")
def sensor_dashboard():
    """Generic sensor dashboard for all sensor types."""
    logger.info("Sensor dashboard route accessed")
    try:
        return render_template("sensor_dashboard.html")
    except Exception as e:
        logger.error(f"Error in sensor dashboard route: {e}")
        return f"Sensor dashboard error: {e}", 500


@main_bp.route("/paxcounter")
def paxcounter():
    """Paxcounter dashboard for monitoring people/device counts."""
    logger.info("Paxcounter route accessed")
    try:
        return render_template("paxcounter.html")
    except Exception as e:
        logger.error(f"Error in paxcounter route: {e}")
        return f"Paxcounter error: {e}", 500


@main_bp.route("/paxcounter/id/<path:profile_id>")
def pax_id_status(profile_id: str):
    """Per-ID status page: RSSI and presence over time for a fingerprinted/MAC ID."""
    logger.info("Paxcounter ID status route accessed: %s", profile_id)
    try:
        from ..utils.paxcount_decode import normalize_profile_id

        normalized = normalize_profile_id(profile_id)
        if not normalized:
            return "Invalid PAX ID", 400
        return render_template(
            "pax_id_status.html",
            profile_id=normalized,
        )
    except Exception as e:
        logger.error(f"Error in paxcounter ID status route: {e}")
        return f"Paxcounter ID status error: {e}", 500
