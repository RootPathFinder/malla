"""
Mesh Topology Routes - API and page routes for mesh topology visualization

Provides:
- Mesh topology visualization page
- API endpoints for topology data
- Neighbor stability analysis
- Node health scores for map coloring
"""

import logging

from flask import Blueprint, jsonify, render_template, request

from ..services.neighbor_service import NeighborService
from ..services.node_health_service import NodeHealthService

logger = logging.getLogger(__name__)

mesh_bp = Blueprint("mesh", __name__, url_prefix="/mesh")


@mesh_bp.route("/topology")
def topology_page():
    """Render the mesh topology visualization page."""
    return render_template("mesh_topology.html")


@mesh_bp.route("/api/topology")
def api_topology():
    """
    Get mesh topology data.

    Query parameters:
        hours: Number of hours to analyze (default: 24)

    Returns:
        JSON with nodes, edges, and topology statistics
    """
    try:
        hours = request.args.get("hours", 24, type=int)
        hours = min(max(hours, 1), 720)  # Clamp between 1 hour and 30 days

        topology = NeighborService.get_mesh_topology(hours=hours)
        return jsonify(topology)

    except Exception as e:
        logger.error(f"Error getting topology: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@mesh_bp.route("/api/stability")
def api_stability():
    """
    Get neighbor stability analysis.

    Query parameters:
        hours: Number of hours to analyze (default: 168 = 7 days)
        node_id: Optional specific node to analyze

    Returns:
        JSON with stability analysis for nodes
    """
    try:
        hours = request.args.get("hours", 168, type=int)
        hours = min(max(hours, 24), 720)  # Clamp between 1 day and 30 days

        node_id = request.args.get("node_id", type=int)

        stability = NeighborService.get_neighbor_stability(node_id=node_id, hours=hours)
        return jsonify(stability)

    except Exception as e:
        logger.error(f"Error getting stability: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@mesh_bp.route("/api/node/<int:node_id>/neighbors")
def api_node_neighbors(node_id: int):
    """
    Get neighbors for a specific node.

    Args:
        node_id: The node ID

    Returns:
        JSON with node's neighbors and quality metrics
    """
    try:
        neighbors = NeighborService.get_node_neighbors(node_id)
        return jsonify(neighbors)

    except Exception as e:
        logger.error(f"Error getting node neighbors: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@mesh_bp.route("/api/links")
def api_links():
    """
    Get link data for map overlay.

    This endpoint returns edges in a format suitable for drawing
    on the map between nodes.

    Query parameters:
        hours: Number of hours to analyze (default: 24)

    Returns:
        JSON with link data including coordinates (if available)
    """
    try:
        from ..database.repositories import LocationRepository

        hours = request.args.get("hours", 24, type=int)
        hours = min(max(hours, 1), 720)

        topology = NeighborService.get_mesh_topology(hours=hours)

        # Get locations for all nodes
        node_ids = [n["node_id"] for n in topology["nodes"]]
        locations = {}

        if node_ids:
            # Get locations from repository
            location_data = LocationRepository.get_node_locations(
                filters={"node_ids": node_ids}
            )
            for loc in location_data:
                if loc.get("latitude") and loc.get("longitude"):
                    locations[loc["node_id"]] = {
                        "lat": loc["latitude"],
                        "lng": loc["longitude"],
                    }

        # Build links with coordinates
        links = []
        for edge in topology["edges"]:
            node_a = edge["node_a"]
            node_b = edge["node_b"]

            # Only include links where both nodes have location
            if node_a in locations and node_b in locations:
                links.append(
                    {
                        "node_a": node_a,
                        "node_b": node_b,
                        "node_a_name": edge["node_a_name"],
                        "node_b_name": edge["node_b_name"],
                        "node_a_location": locations[node_a],
                        "node_b_location": locations[node_b],
                        "snr_a_to_b": edge["snr_a_to_b"],
                        "snr_b_to_a": edge["snr_b_to_a"],
                        "avg_snr": edge["avg_snr"],
                        "quality": edge["quality"],
                        "bidirectional": edge["bidirectional"],
                    }
                )

        return jsonify(
            {
                "links": links,
                "total_links": len(links),
                "links_without_location": len(topology["edges"]) - len(links),
                "analysis_hours": hours,
            }
        )

    except Exception as e:
        logger.error(f"Error getting links: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@mesh_bp.route("/api/health-scores")
def api_health_scores():
    """
    Get health scores for all nodes (for map coloring).

    Query parameters:
        hours: Number of hours to analyze (default: 24)

    Returns:
        JSON with node_id -> health_score mapping
    """
    try:
        hours = request.args.get("hours", 24, type=int)
        hours = min(max(hours, 1), 168)

        # Get network health summary which includes problematic nodes
        summary = NodeHealthService.get_network_health_summary(hours=hours)

        # Build a map of node_id -> health data
        health_map = {}

        # Get all problematic nodes with their scores
        problematic = NodeHealthService.get_problematic_nodes(hours=hours, limit=500)

        for node in problematic:
            health_map[node["node_id"]] = {
                "health_score": node.get("health_score", 100),
                "health_category": node.get("health_category", "unknown"),
                "issues": node.get("issues", []),
            }

        return jsonify(
            {
                "health_scores": health_map,
                "network_score": summary.get("overall_score", 0),
                "analysis_hours": hours,
            }
        )

    except Exception as e:
        logger.error(f"Error getting health scores: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
