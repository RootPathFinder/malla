"""
Mesh Topology Routes - API and page routes for mesh topology visualization

Provides:
- Mesh topology visualization page
- API endpoints for topology data
- Neighbor stability analysis
- Node health scores for map coloring
"""

import logging
import time

from flask import Blueprint, jsonify, render_template, request

from ..services.neighbor_service import ROUTER_ROLES, NeighborService
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
                        # Need to convert to: {node_id, name, hex_id, neighbor_count, role, is_router, ...}

                        # Collect all node IDs to fetch roles
                        all_node_ids = [
                            node.get("id")
                            for node in traceroute_graph.get("nodes", [])
                            if node.get("id")
                        ]
                        node_roles = (
                            NeighborService._get_bulk_node_roles(all_node_ids)
                            if all_node_ids
                            else {}
                        )

                        converted_nodes = []
                        for node in traceroute_graph.get("nodes", []):
                            node_id = node.get("id")
                            role = node_roles.get(node_id) if node_id else None
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
                                    "role": role,
                                    "is_router": role in ROUTER_ROLES
                                    if role
                                    else False,
                                }
                            )

                        # Create a node lookup map for resolving names
                        node_lookup = {n["node_id"]: n for n in converted_nodes}

                        # Convert edges: traceroute uses {source, target, avg_snr}
                        # Need to convert to: {node_a, node_b, node_a_name, node_b_name, snr_a_to_b, quality, ...}
                        converted_edges = []
                        for link in traceroute_graph.get("links", []):
                            avg_snr = link.get("avg_snr")
                            node_a = link.get("source")
                            node_b = link.get("target")

                            # Resolve node names from lookup
                            node_a_name = node_lookup.get(node_a, {}).get(
                                "name", f"!{node_a:08x}" if node_a else "Unknown"
                            )
                            node_b_name = node_lookup.get(node_b, {}).get(
                                "name", f"!{node_b:08x}" if node_b else "Unknown"
                            )

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
                                    "node_a": node_a,
                                    "node_b": node_b,
                                    "node_a_name": node_a_name,
                                    "node_b_name": node_b_name,
                                    "snr_a_to_b": avg_snr,
                                    "snr_b_to_a": None,
                                    "avg_snr": avg_snr,
                                    "quality": quality,
                                    "bidirectional": link.get("type")
                                    == "bidirectional",
                                    "last_seen": link.get("last_seen"),
                                }
                            )

                        # Build proper statistics
                        total_nodes = len(converted_nodes)
                        total_edges = len(converted_edges)
                        bidirectional_edges = sum(
                            1 for e in converted_edges if e.get("bidirectional", False)
                        )

                        topology = {
                            "nodes": converted_nodes,
                            "edges": converted_edges,
                            "statistics": {
                                "total_nodes": total_nodes,
                                "total_edges": total_edges,
                                "bidirectional_edges": bidirectional_edges,
                                "unidirectional_edges": total_edges
                                - bidirectional_edges,
                                "avg_neighbors_per_node": round(
                                    sum(
                                        n.get("neighbor_count", 0)
                                        for n in converted_nodes
                                    )
                                    / total_nodes,
                                    1,
                                )
                                if total_nodes > 0
                                else 0,
                                "mesh_density": round(
                                    (2 * total_edges)
                                    / (total_nodes * (total_nodes - 1))
                                    * 100,
                                    1,
                                )
                                if total_nodes > 1
                                else 0,
                            },
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

    Tries NeighborInfo packets first, falls back to traceroute-based
    connections if NeighborInfo is not available.

    Args:
        node_id: The node ID

    Returns:
        JSON with node's neighbors and quality metrics
    """
    try:
        # Try NeighborInfo first
        neighbors = NeighborService.get_node_neighbors(node_id)

        # If NeighborInfo has data, return it
        if neighbors.get("has_data", False):
            return jsonify(neighbors)

        # Fallback to traceroute-based connections
        try:
            from ..utils.node_utils import get_bulk_node_names

            hours = request.args.get("hours", 24, type=int)
            traceroute_graph = TracerouteService.get_network_graph_data(hours=hours)

            if traceroute_graph and traceroute_graph.get("links"):
                # Find all links involving this node
                node_neighbors = []
                for link in traceroute_graph.get("links", []):
                    source = link.get("source")
                    target = link.get("target")

                    if source == node_id:
                        neighbor_id = target
                    elif target == node_id:
                        neighbor_id = source
                    else:
                        continue

                    avg_snr = link.get("avg_snr", 0)
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

                    node_neighbors.append(
                        {
                            "node_id": neighbor_id,
                            "snr": avg_snr or 0,
                            "quality": quality,
                            "last_rx_time": link.get("last_seen"),
                        }
                    )

                if node_neighbors:
                    # Get node names
                    all_ids = [node_id] + [n["node_id"] for n in node_neighbors]
                    node_names = get_bulk_node_names(all_ids)

                    # Enhance with names
                    for n in node_neighbors:
                        n["node_name"] = node_names.get(
                            n["node_id"], f"!{n['node_id']:08x}"
                        )
                        n["hex_id"] = f"!{n['node_id']:08x}"

                    # Sort by SNR (best first)
                    node_neighbors.sort(key=lambda x: x["snr"], reverse=True)

                    return jsonify(
                        {
                            "node_id": node_id,
                            "node_name": node_names.get(node_id, f"!{node_id:08x}"),
                            "hex_id": f"!{node_id:08x}",
                            "has_data": True,
                            "last_report": None,
                            "neighbor_count": len(node_neighbors),
                            "neighbors": node_neighbors,
                            "broadcast_interval": None,
                            "source": "traceroute",
                        }
                    )
        except Exception as e:
            logger.warning(
                f"Failed to get traceroute neighbors for node {node_id}: {e}"
            )

        # No data from either source
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


@mesh_bp.route("/api/hop-stats")
def api_hop_stats():
    """
    Get mesh hop configuration and statistics.

    Provides an overview of:
    - Node role distribution (routers, clients, etc.)
    - Observed hop counts from packet data
    - Per-node hop configuration details

    Query parameters:
        hours: Number of hours to analyze (default: 24)

    Returns:
        JSON with hop statistics and node configurations
    """
    try:
        from ..database.connection import get_db_connection

        hours = request.args.get("hours", 24, type=int)
        hours = min(max(hours, 1), 720)

        conn = get_db_connection()
        cursor = conn.cursor()
        cutoff_time = time.time() - (hours * 3600)

        # Get role distribution from node_info
        cursor.execute(
            """
            SELECT
                COALESCE(role, 'UNKNOWN') as role,
                COUNT(*) as count
            FROM node_info
            WHERE COALESCE(archived, 0) = 0
            GROUP BY role
            ORDER BY count DESC
        """
        )
        role_rows = cursor.fetchall()
        role_distribution = [
            {"role": row["role"] or "UNKNOWN", "count": row["count"]}
            for row in role_rows
        ]

        # Calculate totals
        total_nodes = sum(r["count"] for r in role_distribution)
        router_count = sum(
            r["count"] for r in role_distribution if r["role"] in ROUTER_ROLES
        )

        # Get hop count distribution from recent packets
        cursor.execute(
            """
            SELECT
                (hop_start - hop_limit) as hop_count,
                COUNT(*) as packet_count
            FROM packet_history
            WHERE timestamp >= ?
                AND hop_start IS NOT NULL
                AND hop_limit IS NOT NULL
            GROUP BY hop_count
            ORDER BY hop_count
        """,
            (cutoff_time,),
        )
        hop_rows = cursor.fetchall()
        hop_distribution = [
            {"hops": row["hop_count"], "count": row["packet_count"]}
            for row in hop_rows
            if row["hop_count"] is not None and row["hop_count"] >= 0
        ]

        # Calculate hop statistics
        total_packets_with_hops = sum(h["count"] for h in hop_distribution)
        direct_packets = next(
            (h["count"] for h in hop_distribution if h["hops"] == 0), 0
        )
        relayed_packets = total_packets_with_hops - direct_packets
        avg_hops = 0.0
        max_hops_observed = 0
        if hop_distribution:
            weighted_sum = sum(h["hops"] * h["count"] for h in hop_distribution)
            if total_packets_with_hops > 0:
                avg_hops = weighted_sum / total_packets_with_hops
            max_hops_observed = max(h["hops"] for h in hop_distribution)

        # Get per-node hop behavior (nodes that relay the most)
        cursor.execute(
            """
            SELECT
                ph.from_node_id as node_id,
                ni.long_name as node_name,
                ni.role as role,
                COUNT(*) as packet_count,
                AVG(CASE WHEN ph.hop_start IS NOT NULL AND ph.hop_limit IS NOT NULL
                    THEN ph.hop_start - ph.hop_limit ELSE NULL END) as avg_hops,
                MAX(CASE WHEN ph.hop_start IS NOT NULL AND ph.hop_limit IS NOT NULL
                    THEN ph.hop_start - ph.hop_limit ELSE NULL END) as max_hops
            FROM packet_history ph
            LEFT JOIN node_info ni ON ph.from_node_id = ni.node_id
            WHERE ph.timestamp >= ?
                AND ph.from_node_id IS NOT NULL
            GROUP BY ph.from_node_id
            HAVING packet_count >= 5
            ORDER BY packet_count DESC
            LIMIT 50
        """,
            (cutoff_time,),
        )
        node_rows = cursor.fetchall()

        nodes_by_activity = []
        for row in node_rows:
            node_id = row["node_id"]
            nodes_by_activity.append(
                {
                    "node_id": node_id,
                    "hex_id": f"!{node_id:08x}" if node_id else "!00000000",
                    "node_name": row["node_name"] or f"!{node_id:08x}",
                    "role": row["role"] or "UNKNOWN",
                    "is_router": row["role"] in ROUTER_ROLES if row["role"] else False,
                    "packet_count": row["packet_count"],
                    "avg_hops": round(row["avg_hops"], 2) if row["avg_hops"] else 0,
                    "max_hops": row["max_hops"] or 0,
                }
            )

        # Get relay activity (packets that have been relayed)
        cursor.execute(
            """
            SELECT
                relay_node,
                COUNT(*) as relay_count
            FROM packet_history
            WHERE timestamp >= ?
                AND relay_node IS NOT NULL
                AND relay_node != 0
            GROUP BY relay_node
            ORDER BY relay_count DESC
            LIMIT 20
        """,
            (cutoff_time,),
        )
        relay_rows = cursor.fetchall()

        # Get names for relay nodes
        relay_node_ids = [row["relay_node"] for row in relay_rows]
        relay_names = {}
        if relay_node_ids:
            from ..utils.node_utils import get_bulk_node_names

            relay_names = get_bulk_node_names(relay_node_ids)

        top_relayers = [
            {
                "node_id": row["relay_node"],
                "hex_id": f"!{row['relay_node']:08x}",
                "node_name": relay_names.get(
                    row["relay_node"], f"!{row['relay_node']:08x}"
                ),
                "relay_count": row["relay_count"],
            }
            for row in relay_rows
        ]

        conn.close()

        return jsonify(
            {
                "role_distribution": role_distribution,
                "hop_distribution": hop_distribution,
                "nodes_by_activity": nodes_by_activity,
                "top_relayers": top_relayers,
                "statistics": {
                    "total_nodes": total_nodes,
                    "router_count": router_count,
                    "client_count": total_nodes - router_count,
                    "router_percentage": round(
                        (router_count / total_nodes * 100) if total_nodes > 0 else 0, 1
                    ),
                    "total_packets_analyzed": total_packets_with_hops,
                    "direct_packets": direct_packets,
                    "relayed_packets": relayed_packets,
                    "relay_percentage": round(
                        (relayed_packets / total_packets_with_hops * 100)
                        if total_packets_with_hops > 0
                        else 0,
                        1,
                    ),
                    "avg_hops": round(avg_hops, 2),
                    "max_hops_observed": max_hops_observed,
                },
                "analysis_hours": hours,
            }
        )

    except Exception as e:
        logger.error(f"Error getting hop stats: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
