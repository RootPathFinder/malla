"""
Custom dashboard routes for user-configurable node monitoring.
"""

import logging

from flask import Blueprint, jsonify, render_template, request
from flask_login import current_user

from ..database.connection import get_db_connection
from ..database.dashboard_repository import DashboardConfigRepository
from ..database.repositories import NodeRepository
from ..utils.cache_utils import cache_response
from ..utils.node_utils import convert_node_id
from ..utils.serialization_utils import sanitize_floats

logger = logging.getLogger(__name__)
dashboard_bp = Blueprint("custom_dashboard", __name__)


@dashboard_bp.route("/custom-dashboard")
def custom_dashboard():
    """Render the custom dashboard page."""
    is_authenticated = current_user.is_authenticated
    return render_template(
        "custom_dashboard.html",
        is_authenticated=is_authenticated,
    )


# ── Dashboard config persistence (server-side) ─────────────────────────
@dashboard_bp.route("/api/custom-dashboard/config", methods=["GET"])
def get_dashboard_config():
    """Return the authenticated user's saved dashboard configuration.

    Returns 401 if not logged in, 204 if no config stored yet.
    """
    if not current_user.is_authenticated:
        return jsonify({"error": "Authentication required"}), 401

    config = DashboardConfigRepository.get_config(current_user.id)
    if config is None:
        return "", 204  # No saved config yet

    return jsonify(config)


@dashboard_bp.route("/api/custom-dashboard/config", methods=["PUT"])
def save_dashboard_config():
    """Save the authenticated user's dashboard configuration.

    Expects JSON body:
    {
      "dashboards": [ ... ],
      "active_dashboard_id": "db_..."
    }
    """
    if not current_user.is_authenticated:
        return jsonify({"error": "Authentication required"}), 401

    data = request.get_json(silent=True)
    if not data or "dashboards" not in data:
        return jsonify({"error": "Missing 'dashboards' in request body"}), 400

    dashboards = data["dashboards"]
    if not isinstance(dashboards, list):
        return jsonify({"error": "'dashboards' must be a list"}), 400

    # Enforce same limits as frontend
    if len(dashboards) > 20:
        return jsonify({"error": "Maximum 20 dashboards allowed"}), 400

    for db in dashboards:
        if not isinstance(db, dict):
            return jsonify({"error": "Each dashboard must be an object"}), 400
        widgets = db.get("widgets", [])
        if isinstance(widgets, list) and len(widgets) > 50:
            return jsonify({"error": "Maximum 50 widgets per dashboard"}), 400

    active_id = data.get("active_dashboard_id")

    ok = DashboardConfigRepository.save_config(
        user_id=current_user.id,
        dashboards=dashboards,
        active_dashboard_id=active_id,
    )
    if ok:
        return jsonify({"status": "saved"})
    return jsonify({"error": "Failed to save dashboard config"}), 500


@dashboard_bp.route("/api/custom-dashboard/nodes/telemetry", methods=["POST"])
def batch_node_telemetry():
    """Fetch latest telemetry for multiple nodes in a single request.

    Expects JSON body: {"node_ids": ["!aabbccdd", "!11223344", ...]}
    Returns: {"nodes": {"!aabbccdd": {...}, ...}}
    """
    try:
        data = request.get_json(silent=True)
        if not data or "node_ids" not in data:
            return jsonify({"error": "Missing node_ids in request body"}), 400

        node_ids = data["node_ids"]
        if not isinstance(node_ids, list) or len(node_ids) == 0:
            return jsonify({"error": "node_ids must be a non-empty list"}), 400

        # Limit to 50 nodes per request
        if len(node_ids) > 50:
            return (
                jsonify({"error": "Maximum 50 nodes per request"}),
                400,
            )

        result = {}
        for hex_id in node_ids:
            try:
                node_id_int = convert_node_id(hex_id)
                telemetry = NodeRepository.get_latest_telemetry(node_id_int)

                # Also get basic node info
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT node_id, hex_id, long_name, short_name, hw_model,
                           role, last_updated, firmware_version,
                           power_type, last_battery_voltage
                    FROM node_info
                    WHERE node_id = ?
                    """,
                    (node_id_int,),
                )
                row = cursor.fetchone()
                conn.close()

                node_info = dict(row) if row else {}
                result[hex_id] = {
                    "node_info": sanitize_floats(node_info),
                    "telemetry": sanitize_floats(telemetry) if telemetry else None,
                }
            except (ValueError, Exception) as e:
                logger.warning(f"Error fetching telemetry for node {hex_id}: {e}")
                result[hex_id] = {"node_info": {}, "telemetry": None, "error": str(e)}

        return jsonify({"nodes": result})
    except Exception as e:
        logger.error(f"Error in batch node telemetry: {e}")
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/custom-dashboard/node/<node_id>/telemetry/history")
@cache_response(ttl_seconds=60)
def dashboard_node_telemetry_history(node_id):
    """Get telemetry history for a specific node for sparkline charts.

    Query params:
        hours: Number of hours of history (default 24, max 168)
        metric: Specific metric to return (optional, returns all if omitted)
    """
    try:
        node_id_int = convert_node_id(node_id)

        hours = request.args.get("hours", 24, type=int)
        hours = max(1, min(168, hours))

        history = NodeRepository.get_telemetry_history(node_id_int, hours=hours)

        return jsonify(sanitize_floats({"history": history}))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error in dashboard telemetry history: {e}")
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/custom-dashboard/nodes/search")
def dashboard_node_search():
    """Search nodes for the dashboard node picker.

    Query params:
        q: Search query string
        limit: Max results (default 20)
    """
    try:
        query = request.args.get("q", "").strip()
        limit = request.args.get("limit", 20, type=int)
        limit = max(1, min(50, limit))

        conn = get_db_connection()
        cursor = conn.cursor()

        if query:
            cursor.execute(
                """
                SELECT node_id, hex_id, long_name, short_name, hw_model,
                       role, last_updated, power_type
                FROM node_info
                WHERE COALESCE(archived, 0) = 0
                  AND (long_name LIKE ? OR short_name LIKE ? OR hex_id LIKE ?)
                ORDER BY last_updated DESC
                LIMIT ?
                """,
                (f"%{query}%", f"%{query}%", f"%{query}%", limit),
            )
        else:
            cursor.execute(
                """
                SELECT node_id, hex_id, long_name, short_name, hw_model,
                       role, last_updated, power_type
                FROM node_info
                WHERE COALESCE(archived, 0) = 0
                ORDER BY last_updated DESC
                LIMIT ?
                """,
                (limit,),
            )

        nodes = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return jsonify({"nodes": sanitize_floats(nodes)})
    except Exception as e:
        logger.error(f"Error in dashboard node search: {e}")
        return jsonify({"error": str(e)}), 500
