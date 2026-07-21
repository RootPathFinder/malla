"""
Node-related routes for the Meshtastic Mesh Health Web UI
"""

import logging

from flask import Blueprint, render_template, request

# Import from the new modular architecture
from ..database.repositories import NodeRepository
from ..services.admin_service import get_admin_service
from ..services.neighbor_service import NeighborService

logger = logging.getLogger(__name__)
node_bp = Blueprint("node", __name__)


@node_bp.route("/nodes")
def nodes():
    """Node browser page using modern table interface."""
    logger.info("Nodes route accessed")
    try:
        logger.info("Nodes page rendered")
        return render_template("nodes.html")
    except Exception as e:
        logger.error(f"Error in nodes route: {e}")
        return f"Nodes error: {e}", 500


@node_bp.route("/node/<node_id>")
def node_detail(node_id):
    """Node detail page showing comprehensive information about a specific node."""
    logger.info(f"Node detail route accessed for node {node_id}")
    try:
        # Handle both hex ID and integer node ID
        if isinstance(node_id, str) and node_id.startswith("!"):
            node_id_int = int(node_id[1:], 16)
        elif isinstance(node_id, str) and not node_id.isdigit():
            try:
                node_id_int = int(node_id, 16)
            except ValueError:
                return "Invalid node ID format", 400
        else:
            node_id_int = int(node_id)

        # Get node details using the repository
        node_details = NodeRepository.get_node_details(node_id_int)
        if not node_details:
            return "Node not found", 404

        # Check if admin features are available for live telemetry
        try:
            admin_service = get_admin_service()
            admin_status = admin_service.get_connection_status()
            # Live telemetry requires TCP or Serial connection (not MQTT)
            can_send_commands = admin_status.get(
                "connected", False
            ) and admin_status.get("connection_type") in ["tcp", "serial"]
        except Exception:
            can_send_commands = False

        node_details["can_send_commands"] = can_send_commands

        # Zero-hop RF neighbors: NeighborInfo + observed + traceroute peers
        zh_hours = request.args.get("zh_hours", default=168, type=int)
        if zh_hours not in (24, 168, 720, 0):
            zh_hours = 168
        try:
            zero_hop = NeighborService.get_zero_hop_neighbors(
                node_id_int, hours=zh_hours or None
            )
            node_details["zero_hop_neighbors"] = zero_hop.get("neighbors") or []
            node_details["zero_hop_hours"] = zh_hours
            node_details["zero_hop_center"] = zero_hop.get("center")
            node_details["zero_hop_summary"] = zero_hop.get("summary") or {
                "neighbor_count": 0,
                "both_ways": 0,
                "one_way": 0,
                "with_location": 0,
                "avg_snr": None,
                "best_snr": None,
                "avg_distance_km": None,
                "max_distance_km": None,
            }
            node_details["zero_hop_meta"] = {
                "neighbor_count": zero_hop.get("neighbor_count", 0),
                "last_neighborinfo_report": zero_hop.get("last_neighborinfo_report"),
                "hours": zh_hours,
            }
        except Exception as e:
            logger.warning(
                "Failed to load zero-hop neighbors for %s: %s", node_id_int, e
            )
            node_details["zero_hop_neighbors"] = []
            node_details["zero_hop_hours"] = zh_hours
            node_details["zero_hop_center"] = None
            node_details["zero_hop_summary"] = {
                "neighbor_count": 0,
                "both_ways": 0,
                "one_way": 0,
                "with_location": 0,
                "avg_snr": None,
                "best_snr": None,
                "avg_distance_km": None,
                "max_distance_km": None,
            }
            node_details["zero_hop_meta"] = {
                "neighbor_count": 0,
                "last_neighborinfo_report": None,
                "hours": zh_hours,
            }

        logger.info("Node detail page rendered successfully")
        return render_template("node_detail.html", **node_details)
    except Exception as e:
        logger.error(f"Error in node detail route: {e}")
        return f"Node detail error: {e}", 500
