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
from ..services.traceroute_service import TracerouteService

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

    Attempts to build topology from NeighborInfo packets first, then falls back to
    traceroute-based network graphs if NeighborInfo data is insufficient.

    Query parameters:
        hours: Number of hours to analyze (default: 24)
        source: Force data source - 'neighbor', 'traceroute', or 'combined' (default: auto)

    Returns:
        JSON with nodes, edges, and topology statistics
    """
    try:
        hours = request.args.get("hours", 24, type=int)
        hours = min(max(hours, 1), 720)  # Clamp between 1 hour and 30 days
        source = request.args.get(
            "source", "auto"
        )  # auto, neighbor, traceroute, combined

        topology = None
        source_used = None

        # Try NeighborInfo first if not forcing traceroute
        if source in ("auto", "neighbor", "combined"):
            try:
                topology = NeighborService.get_mesh_topology(hours=hours)
                if topology and topology.get("nodes") and len(topology["nodes"]) > 0:
                    source_used = "neighbor"
                    logger.info(
                        f"Built topology from {len(topology['nodes'])} NeighborInfo reports"
                    )
            except Exception as e:
                logger.warning(f"Failed to get NeighborInfo topology: {e}")

        # Fallback to traceroute-based topology if NeighborInfo insufficient
        if (
            not topology
            or not topology.get("nodes")
            or len(topology.get("nodes", [])) == 0
        ):
            if source in ("auto", "traceroute", "combined"):
                try:
                    logger.info(
                        "NeighborInfo data insufficient, using traceroute-based topology"
                    )
                    traceroute_graph = TracerouteService.get_network_graph_data(
                        hours=hours
                    )
                    if traceroute_graph and (
                        traceroute_graph.get("nodes") or traceroute_graph.get("links")
                    ):
                        source_used = "traceroute"
                        # Convert traceroute graph to neighbor service format for compatibility
                        # Traceroute nodes have: {id, name, ...}
                        # Need to convert to: {node_id, name, hex_id, neighbor_count, ...}
                        converted_nodes = []
                        for node in traceroute_graph.get("nodes", []):
                            node_id = node.get("id")
                            converted_nodes.append(
                                {
                                    "node_id": node_id,
                                    "name": node.get(
                                        "name",
                                        f"!{node_id:08x}" if node_id else "Unknown",
                                    ),
                                    "hex_id": f"!{node_id:08x}"
                                    if node_id
                                    else "!00000000",
                                    "neighbor_count": node.get("connections", 0),
                                    "last_seen": node.get("last_seen"),
                                    "broadcast_interval": None,
                                }
                            )

                        # Convert edges: traceroute uses {source, target, avg_snr}
                        # Need to convert to: {node_a, node_b, snr_a_to_b, quality, ...}
                        converted_edges = []
                        for link in traceroute_graph.get("links", []):
                            avg_snr = link.get("avg_snr")
                            # Determine quality from SNR
                            quality = "unknown"
                            if avg_snr is not None:
                                if avg_snr >= 10:
                                    quality = "excellent"
                                elif avg_snr >= 5:
                                    quality = "good"
                                elif avg_snr >= 0:
                                    quality = "fair"
                                else:
                                    quality = "poor"

                            converted_edges.append(
                                {
                                    "node_a": link.get("source"),
                                    "node_b": link.get("target"),
                                    "snr_a_to_b": avg_snr,
                                    "snr_b_to_a": None,
                                    "avg_snr": avg_snr,
                                    "quality": quality,
                                    "bidirectional": link.get("type")
                                    == "bidirectional",
                                    "last_seen": link.get("last_seen"),
                                }
                            )

                        topology = {
                            "nodes": converted_nodes,
                            "edges": converted_edges,
                            "statistics": traceroute_graph.get("stats", {}),
                            "source": "traceroute",
                        }
                        logger.info(
                            f"Built topology from {len(topology['nodes'])} traceroute nodes"
                        )
                except Exception as e:
                    logger.warning(f"Failed to get traceroute topology: {e}")

        # If we still don't have data, return empty topology
        if not topology:
            topology = {
                "nodes": [],
                "edges": [],
                "statistics": {
                    "total_nodes": 0,
                    "total_edges": 0,
                    "bidirectional_edges": 0,
                    "unidirectional_edges": 0,
                },
                "source": "none",
            }
            source_used = "none"

        # Add source information to response
        if source_used:
            topology["source"] = source_used

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
    on the map between nodes. Uses NeighborInfo if available,
    falls back to traceroute-based topology.

    Query parameters:
        hours: Number of hours to analyze (default: 24)

    Returns:
        JSON with link data including coordinates (if available)
    """
    try:
        from ..database.repositories import LocationRepository

        hours = request.args.get("hours", 24, type=int)
        hours = min(max(hours, 1), 720)

        # Get topology with fallback logic (same as /api/topology endpoint)
        topology = None

        # Try NeighborInfo first
        try:
            topology = NeighborService.get_mesh_topology(hours=hours)
            if topology and topology.get("nodes") and len(topology["nodes"]) > 0:
                logger.info("Using NeighborInfo topology for links endpoint")
        except Exception as e:
            logger.warning(f"Failed to get NeighborInfo topology for links: {e}")

        # Fallback to traceroute-based topology
        if (
            not topology
            or not topology.get("nodes")
            or len(topology.get("nodes", [])) == 0
        ):
            try:
                logger.info(
                    "NeighborInfo data insufficient for links, using traceroute-based topology"
                )
                traceroute_graph = TracerouteService.get_network_graph_data(hours=hours)
                if traceroute_graph and (
                    traceroute_graph.get("nodes") or traceroute_graph.get("links")
                ):
                    # Convert traceroute graph nodes to neighbor service format
                    converted_nodes = []
                    for node in traceroute_graph.get("nodes", []):
                        node_id = node.get("id")
                        converted_nodes.append(
                            {
                                "node_id": node_id,
                                "name": node.get(
                                    "name", f"!{node_id:08x}" if node_id else "Unknown"
                                ),
                                "hex_id": f"!{node_id:08x}" if node_id else "!00000000",
                                "neighbor_count": node.get("connections", 0),
                            }
                        )

                    # Convert traceroute links to edge format
                    converted_edges = []
                    for link in traceroute_graph.get("links", []):
                        # Get node names from the converted nodes
                        node_a = link.get("source")
                        node_b = link.get("target")
                        node_a_name = next(
                            (
                                n["name"]
                                for n in converted_nodes
                                if n["node_id"] == node_a
                            ),
                            f"!{node_a:08x}" if node_a else "Unknown",
                        )
                        node_b_name = next(
                            (
                                n["name"]
                                for n in converted_nodes
                                if n["node_id"] == node_b
                            ),
                            f"!{node_b:08x}" if node_b else "Unknown",
                        )

                        avg_snr = link.get("avg_snr")
                        quality = "unknown"
                        if avg_snr is not None:
                            if avg_snr >= 10:
                                quality = "excellent"
                            elif avg_snr >= 5:
                                quality = "good"
                            elif avg_snr >= 0:
                                quality = "fair"
                            else:
                                quality = "poor"

                        converted_edges.append(
                            {
                                "node_a": node_a,
                                "node_b": node_b,
                                "node_a_name": node_a_name,
                                "node_b_name": node_b_name,
                                "snr_a_to_b": avg_snr,
                                "snr_b_to_a": None,
                                "avg_snr": avg_snr,
                                "quality": quality,
                                "bidirectional": link.get("type") == "bidirectional",
                            }
                        )

                    topology = {
                        "nodes": converted_nodes,
                        "edges": converted_edges,
                    }
            except Exception as e:
                logger.warning(f"Failed to get traceroute topology for links: {e}")

        # If still no topology, return empty
        if not topology:
            topology = {"nodes": [], "edges": []}

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
                        "node_a_name": edge.get("node_a_name", f"!{node_a:08x}"),
                        "node_b_name": edge.get("node_b_name", f"!{node_b:08x}"),
                        "node_a_location": locations[node_a],
                        "node_b_location": locations[node_b],
                        "snr_a_to_b": edge.get("snr_a_to_b"),
                        "snr_b_to_a": edge.get("snr_b_to_a"),
                        "avg_snr": edge.get("avg_snr"),
                        "quality": edge.get("quality"),
                        "bidirectional": edge.get("bidirectional"),
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
