"""
Alert Routes - API and page routes for the alert system

Provides:
- Alert dashboard page
- API endpoints for alerts, trends, and heatmaps
- Threshold configuration
"""

import logging

from flask import Blueprint, jsonify, render_template, request

from ..services.alert_service import (
    AlertService,
    AlertSeverity,
    AlertThresholds,
    AlertType,
)

logger = logging.getLogger(__name__)

alert_bp = Blueprint("alerts", __name__, url_prefix="/alerts")


@alert_bp.route("/")
def alerts_page():
    """Render the alerts dashboard page."""
    # Run health checks if needed
    AlertService.run_health_checks()

    return render_template("alerts.html")


@alert_bp.route("/api/list")
def api_alerts_list():
    """
    Get list of alerts.

    Query parameters:
        include_resolved: Include resolved alerts (default: false)
        severity: Filter by severity (info, warning, critical)
        node_id: Filter by node ID
        limit: Maximum alerts to return (default: 100)

    Returns:
        JSON with alerts list
    """
    try:
        include_resolved = (
            request.args.get("include_resolved", "false").lower() == "true"
        )
        severity_str = request.args.get("severity")
        node_id = request.args.get("node_id", type=int)
        limit = request.args.get("limit", 100, type=int)

        severity = None
        if severity_str:
            try:
                severity = AlertSeverity(severity_str)
            except ValueError:
                pass

        alerts = AlertService.get_alerts(
            include_resolved=include_resolved,
            severity=severity,
            node_id=node_id,
            limit=limit,
        )

        summary = AlertService.get_alert_summary()

        return jsonify(
            {
                "alerts": alerts,
                "summary": summary,
            }
        )

    except Exception as e:
        logger.error(f"Error getting alerts: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/summary")
def api_alerts_summary():
    """Get alert summary statistics."""
    try:
        summary = AlertService.get_alert_summary()
        return jsonify(summary)

    except Exception as e:
        logger.error(f"Error getting alert summary: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/run-checks", methods=["POST"])
def api_run_checks():
    """Force run health checks."""
    try:
        results = AlertService.run_health_checks(force=True)
        return jsonify(results)

    except Exception as e:
        logger.error(f"Error running health checks: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/thresholds", methods=["GET", "POST"])
def api_thresholds():
    """
    Get or update alert thresholds.

    GET: Returns current thresholds
    POST: Updates thresholds with JSON body
    """
    try:
        if request.method == "GET":
            thresholds = AlertService.get_thresholds()
            return jsonify(thresholds.to_dict())

        if request.method == "POST":
            data = request.get_json()
            if not data:
                return jsonify({"error": "No data provided"}), 400

            thresholds = AlertThresholds.from_dict(data)
            AlertService.set_thresholds(thresholds)

            return jsonify(
                {
                    "success": True,
                    "thresholds": thresholds.to_dict(),
                }
            )

        return jsonify({"error": "Method not allowed"}), 405

    except Exception as e:
        logger.error(f"Error with thresholds: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/heatmap")
def api_heatmap():
    """
    Get activity heatmap data.

    Query parameters:
        node_id: Optional node to analyze (omit for network-wide)
        days: Number of days to analyze (default: 7)
        timezone: 'utc' or 'local' (default: local)

    Returns:
        Heatmap data structure
    """
    try:
        node_id = request.args.get("node_id", type=int)
        days = request.args.get("days", 7, type=int)
        days = min(max(days, 1), 30)
        timezone = request.args.get("timezone", "local")
        use_utc = timezone.lower() == "utc"

        heatmap = AlertService.get_activity_heatmap(
            node_id=node_id, days=days, use_utc=use_utc
        )
        return jsonify(heatmap)

    except Exception as e:
        logger.error(f"Error getting heatmap: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/trends")
def api_trends():
    """
    Get time-series trend data.

    Query parameters:
        metric: What to trend (packets, nodes, signal) - default: packets
        hours: Number of hours to analyze (default: 168 = 7 days)
        timezone: 'utc' or 'local' (default: local)

    Returns:
        Time-series data for charting
    """
    try:
        metric = request.args.get("metric", "packets")
        hours = request.args.get("hours", 168, type=int)
        hours = min(max(hours, 1), 720)
        timezone = request.args.get("timezone", "local")
        use_utc = timezone.lower() == "utc"

        if metric not in ["packets", "nodes", "signal"]:
            return jsonify({"error": "Invalid metric"}), 400

        trend_data = AlertService.get_trend_data(
            metric=metric, hours=hours, use_utc=use_utc
        )
        return jsonify(trend_data)

    except Exception as e:
        logger.error(f"Error getting trends: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/resolve/<alert_type>/<int:node_id>", methods=["POST"])
def api_resolve_alert(alert_type: str, node_id: int):
    """Manually resolve an alert."""
    try:
        try:
            at = AlertType(alert_type)
        except ValueError:
            return jsonify({"error": "Invalid alert type"}), 400

        resolved = AlertService.resolve_alert(at, node_id)

        return jsonify(
            {
                "success": resolved,
                "message": "Alert resolved" if resolved else "Alert not found",
            }
        )

    except Exception as e:
        logger.error(f"Error resolving alert: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/stale-nodes")
def api_stale_nodes():
    """
    Get nodes that haven't transmitted within the stale threshold.

    Query parameters:
        days: Override threshold (default: 14 days)

    Returns:
        List of stale nodes eligible for archival
    """
    try:
        days = request.args.get("days", type=int)
        stale_nodes = AlertService.get_stale_nodes(days)
        thresholds = AlertService.get_thresholds()

        return jsonify(
            {
                "stale_nodes": stale_nodes,
                "count": len(stale_nodes),
                "threshold_days": days or thresholds.stale_node_days,
            }
        )

    except Exception as e:
        logger.error(f"Error getting stale nodes: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/archived-nodes")
def api_archived_nodes():
    """Get all archived nodes."""
    try:
        archived_nodes = AlertService.get_archived_nodes()
        return jsonify(
            {
                "archived_nodes": archived_nodes,
                "count": len(archived_nodes),
            }
        )

    except Exception as e:
        logger.error(f"Error getting archived nodes: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/archive-node/<int:node_id>", methods=["POST"])
def api_archive_node(node_id: int):
    """Archive a single node."""
    try:
        success = AlertService.archive_node(node_id)
        return jsonify(
            {
                "success": success,
                "message": "Node archived" if success else "Failed to archive node",
            }
        )

    except Exception as e:
        logger.error(f"Error archiving node: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/unarchive-node/<int:node_id>", methods=["POST"])
def api_unarchive_node(node_id: int):
    """Unarchive a node (restore to active)."""
    try:
        success = AlertService.unarchive_node(node_id)
        return jsonify(
            {
                "success": success,
                "message": "Node restored" if success else "Failed to restore node",
            }
        )

    except Exception as e:
        logger.error(f"Error unarchiving node: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@alert_bp.route("/api/archive-stale-nodes", methods=["POST"])
def api_archive_stale_nodes():
    """
    Archive all stale nodes at once.

    Query parameters:
        days: Override threshold (default: 14 days)

    Returns:
        Summary of archived nodes
    """
    try:
        days = request.args.get("days", type=int)
        result = AlertService.archive_stale_nodes(days)
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error archiving stale nodes: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
