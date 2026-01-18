"""
Neighbor Info Service - Mesh Topology and Neighbor Stability Analysis

This service provides:
- Mesh topology visualization from NeighborInfo packets
- Neighbor stability tracking over time
- Link quality analysis between nodes
"""

import logging
import time
from collections import defaultdict
from typing import Any

from meshtastic import mesh_pb2

from ..database.connection import get_db_connection
from ..utils.node_utils import get_bulk_node_names

logger = logging.getLogger(__name__)


class NeighborService:
    """Service for analyzing mesh topology from NeighborInfo packets."""

    # In-memory cache for topology data
    _topology_cache: dict[str, tuple[float, Any]] = {}
    _CACHE_TTL = 120  # 2 minutes

    @staticmethod
    def get_mesh_topology(hours: int = 24) -> dict[str, Any]:
        """
        Build mesh topology from NeighborInfo packets.

        Returns a graph structure with nodes and edges representing
        which nodes can hear each other.

        Args:
            hours: Number of hours of data to analyze

        Returns:
            Dictionary with nodes, edges, and topology statistics
        """
        cache_key = f"topology_{hours}h"
        now = time.time()

        # Check cache
        if cache_key in NeighborService._topology_cache:
            cached_time, cached_data = NeighborService._topology_cache[cache_key]
            if now - cached_time < NeighborService._CACHE_TTL:
                logger.debug(f"Returning cached topology for {hours}h")
                return cached_data

        logger.info(f"Building mesh topology from NeighborInfo packets ({hours}h)")

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cutoff_time = now - (hours * 3600)

            # Get all NeighborInfo packets in the time window
            cursor.execute(
                """
                SELECT id, from_node_id, timestamp, raw_payload, gateway_id
                FROM packet_history
                WHERE portnum_name = 'NEIGHBORINFO_APP'
                AND timestamp >= ?
                AND raw_payload IS NOT NULL
                ORDER BY timestamp DESC
            """,
                (cutoff_time,),
            )

            packets = cursor.fetchall()
            logger.info(f"Found {len(packets)} NeighborInfo packets")

            # Build the topology graph
            # nodes: {node_id: {name, last_seen, neighbor_count, ...}}
            # edges: {(node_a, node_b): {snr_a_to_b, snr_b_to_a, last_seen, ...}}
            nodes: dict[int, dict[str, Any]] = {}
            edges: dict[tuple[int, int], dict[str, Any]] = {}

            # Track the latest neighbor info per reporting node
            latest_neighbor_info: dict[int, dict[str, Any]] = {}

            for packet in packets:
                try:
                    reporting_node = packet["from_node_id"]
                    if not reporting_node:
                        continue

                    # Parse the NeighborInfo protobuf
                    neighbor_info = mesh_pb2.NeighborInfo()
                    neighbor_info.ParseFromString(packet["raw_payload"])

                    # Use the node_id from the protobuf if available
                    actual_node_id = (
                        neighbor_info.node_id
                        if neighbor_info.node_id
                        else reporting_node
                    )

                    # Only keep the latest report per node
                    if actual_node_id in latest_neighbor_info:
                        if (
                            packet["timestamp"]
                            <= latest_neighbor_info[actual_node_id]["timestamp"]
                        ):
                            continue

                    # Record node
                    nodes[actual_node_id] = {
                        "node_id": actual_node_id,
                        "last_seen": packet["timestamp"],
                        "neighbor_count": len(neighbor_info.neighbors),
                        "broadcast_interval": neighbor_info.node_broadcast_interval_secs
                        or None,
                    }

                    latest_neighbor_info[actual_node_id] = {
                        "timestamp": packet["timestamp"],
                        "neighbors": [],
                    }

                    # Process neighbors
                    for neighbor in neighbor_info.neighbors:
                        neighbor_node_id = neighbor.node_id
                        if not neighbor_node_id:
                            continue

                        snr = neighbor.snr if neighbor.snr else 0.0

                        # Record neighbor node if not seen
                        if neighbor_node_id not in nodes:
                            nodes[neighbor_node_id] = {
                                "node_id": neighbor_node_id,
                                "last_seen": None,  # Haven't seen their own report
                                "neighbor_count": 0,
                                "broadcast_interval": None,
                            }

                        # Create edge key (always smaller node_id first for consistency)
                        edge_key = (
                            min(actual_node_id, neighbor_node_id),
                            max(actual_node_id, neighbor_node_id),
                        )

                        if edge_key not in edges:
                            edges[edge_key] = {
                                "node_a": edge_key[0],
                                "node_b": edge_key[1],
                                "snr_a_to_b": None,
                                "snr_b_to_a": None,
                                "last_seen": packet["timestamp"],
                                "confirmed_both_ways": False,
                                # All RF links are bidirectional - this tracks data completeness
                                "data_sources": 0,  # Count of nodes that reported this link
                            }

                        # Update SNR based on direction
                        if actual_node_id == edge_key[0]:
                            # This node is node_a, reporting hearing node_b
                            if edges[edge_key]["snr_a_to_b"] is None:
                                edges[edge_key]["data_sources"] += 1
                            edges[edge_key]["snr_a_to_b"] = snr
                        else:
                            # This node is node_b, reporting hearing node_a
                            if edges[edge_key]["snr_b_to_a"] is None:
                                edges[edge_key]["data_sources"] += 1
                            edges[edge_key]["snr_b_to_a"] = snr

                        edges[edge_key]["last_seen"] = max(
                            edges[edge_key]["last_seen"], packet["timestamp"]
                        )

                        # Check if we have SNR data from both directions
                        if (
                            edges[edge_key]["snr_a_to_b"] is not None
                            and edges[edge_key]["snr_b_to_a"] is not None
                        ):
                            edges[edge_key]["confirmed_both_ways"] = True

                        latest_neighbor_info[actual_node_id]["neighbors"].append(
                            {"node_id": neighbor_node_id, "snr": snr}
                        )

                except Exception as e:
                    logger.warning(
                        f"Failed to parse NeighborInfo packet {packet['id']}: {e}"
                    )
                    continue

            conn.close()

            # Get node names
            all_node_ids = list(nodes.keys())
            node_names = get_bulk_node_names(all_node_ids) if all_node_ids else {}

            # Enhance nodes with names
            for node_id, node_data in nodes.items():
                node_data["name"] = node_names.get(node_id, f"!{node_id:08x}")
                node_data["hex_id"] = f"!{node_id:08x}"

            # Convert edges to list format for JSON
            edge_list = []
            for _edge_key, edge_data in edges.items():
                edge_data["node_a_name"] = nodes[edge_data["node_a"]]["name"]
                edge_data["node_b_name"] = nodes[edge_data["node_b"]]["name"]

                # Calculate average SNR if bidirectional
                snrs = [
                    s
                    for s in [edge_data["snr_a_to_b"], edge_data["snr_b_to_a"]]
                    if s is not None
                ]
                edge_data["avg_snr"] = sum(snrs) / len(snrs) if snrs else None

                # Classify link quality
                if edge_data["avg_snr"] is not None:
                    if edge_data["avg_snr"] >= 10:
                        edge_data["quality"] = "excellent"
                    elif edge_data["avg_snr"] >= 5:
                        edge_data["quality"] = "good"
                    elif edge_data["avg_snr"] >= 0:
                        edge_data["quality"] = "fair"
                    else:
                        edge_data["quality"] = "poor"
                else:
                    edge_data["quality"] = "unknown"

                edge_list.append(edge_data)

            # Calculate topology statistics
            total_nodes = len(nodes)
            total_edges = len(edge_list)
            confirmed_both_ways = sum(1 for e in edge_list if e["confirmed_both_ways"])
            partial_data = total_edges - confirmed_both_ways
            nodes_with_neighbors = sum(
                1 for n in nodes.values() if n["neighbor_count"] > 0
            )

            # Calculate average connectivity
            avg_neighbors = (
                sum(n["neighbor_count"] for n in nodes.values()) / total_nodes
                if total_nodes > 0
                else 0
            )

            result = {
                "nodes": list(nodes.values()),
                "edges": edge_list,
                "statistics": {
                    "total_nodes": total_nodes,
                    "total_edges": total_edges,
                    "confirmed_both_ways": confirmed_both_ways,
                    "partial_data": partial_data,
                    "nodes_with_neighbors": nodes_with_neighbors,
                    "nodes_without_reports": total_nodes - nodes_with_neighbors,
                    "avg_neighbors_per_node": round(avg_neighbors, 1),
                    "mesh_density": round(
                        (2 * total_edges) / (total_nodes * (total_nodes - 1)) * 100, 1
                    )
                    if total_nodes > 1
                    else 0,
                },
                "analysis_hours": hours,
                "generated_at": now,
            }

            # Cache the result
            NeighborService._topology_cache[cache_key] = (now, result)

            logger.info(
                f"Topology built: {total_nodes} nodes, {total_edges} links, "
                f"{confirmed_both_ways} confirmed both ways, {partial_data} partial data"
            )

            return result

        except Exception as e:
            logger.error(f"Error building mesh topology: {e}", exc_info=True)
            raise

    @staticmethod
    def get_neighbor_stability(
        node_id: int | None = None, hours: int = 168
    ) -> dict[str, Any]:
        """
        Analyze neighbor stability over time.

        Tracks when nodes gain/lose neighbors to detect unstable links.

        Args:
            node_id: Optional specific node to analyze (None for all nodes)
            hours: Number of hours to analyze (default 7 days)

        Returns:
            Dictionary with stability analysis
        """
        logger.info(f"Analyzing neighbor stability for {hours}h, node_id={node_id}")

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cutoff_time = time.time() - (hours * 3600)

            # Build query
            if node_id:
                query = """
                    SELECT id, from_node_id, timestamp, raw_payload
                    FROM packet_history
                    WHERE portnum_name = 'NEIGHBORINFO_APP'
                    AND timestamp >= ?
                    AND from_node_id = ?
                    AND raw_payload IS NOT NULL
                    ORDER BY timestamp ASC
                """
                cursor.execute(query, (cutoff_time, node_id))
            else:
                query = """
                    SELECT id, from_node_id, timestamp, raw_payload
                    FROM packet_history
                    WHERE portnum_name = 'NEIGHBORINFO_APP'
                    AND timestamp >= ?
                    AND raw_payload IS NOT NULL
                    ORDER BY timestamp ASC
                """
                cursor.execute(query, (cutoff_time,))

            packets = cursor.fetchall()
            conn.close()

            logger.info(
                f"Found {len(packets)} NeighborInfo packets for stability analysis"
            )

            # Track neighbor history per node
            # {node_id: [{timestamp, neighbors: set()}]}
            neighbor_history: dict[int, list[dict[str, Any]]] = defaultdict(list)

            for packet in packets:
                try:
                    reporting_node = packet["from_node_id"]
                    if not reporting_node:
                        continue

                    neighbor_info = mesh_pb2.NeighborInfo()
                    neighbor_info.ParseFromString(packet["raw_payload"])

                    actual_node_id = (
                        neighbor_info.node_id
                        if neighbor_info.node_id
                        else reporting_node
                    )

                    neighbors_set = set()
                    neighbor_snrs = {}
                    for neighbor in neighbor_info.neighbors:
                        if neighbor.node_id:
                            neighbors_set.add(neighbor.node_id)
                            neighbor_snrs[neighbor.node_id] = neighbor.snr

                    neighbor_history[actual_node_id].append(
                        {
                            "timestamp": packet["timestamp"],
                            "neighbors": neighbors_set,
                            "neighbor_snrs": neighbor_snrs,
                        }
                    )

                except Exception as e:
                    logger.warning(f"Failed to parse packet for stability: {e}")
                    continue

            # Analyze stability for each node
            stability_results: list[dict[str, Any]] = []
            all_node_ids = set(neighbor_history.keys())

            # Collect all referenced neighbor node IDs
            for history in neighbor_history.values():
                for entry in history:
                    all_node_ids.update(entry["neighbors"])

            node_names = get_bulk_node_names(list(all_node_ids)) if all_node_ids else {}

            for nid, history in neighbor_history.items():
                if len(history) < 2:
                    continue  # Need at least 2 data points

                # Track changes
                neighbors_gained = []
                neighbors_lost = []
                stable_neighbors = set()
                unstable_neighbors = set()

                for i in range(1, len(history)):
                    prev_neighbors = history[i - 1]["neighbors"]
                    curr_neighbors = history[i]["neighbors"]

                    gained = curr_neighbors - prev_neighbors
                    lost = prev_neighbors - curr_neighbors

                    for g in gained:
                        neighbors_gained.append(
                            {
                                "node_id": g,
                                "timestamp": history[i]["timestamp"],
                            }
                        )
                        unstable_neighbors.add(g)

                    for lost_node in lost:
                        neighbors_lost.append(
                            {
                                "node_id": lost_node,
                                "timestamp": history[i]["timestamp"],
                            }
                        )
                        unstable_neighbors.add(lost_node)

                # Stable neighbors are those that appear in most reports without changes
                all_ever_neighbors = set()
                for entry in history:
                    all_ever_neighbors.update(entry["neighbors"])

                stable_neighbors = all_ever_neighbors - unstable_neighbors

                # Calculate stability score (0-100)
                total_neighbor_appearances = sum(len(e["neighbors"]) for e in history)
                total_changes = len(neighbors_gained) + len(neighbors_lost)

                if total_neighbor_appearances > 0:
                    stability_score = max(
                        0, 100 - (total_changes / total_neighbor_appearances * 100)
                    )
                else:
                    stability_score = 100

                stability_results.append(
                    {
                        "node_id": nid,
                        "node_name": node_names.get(nid, f"!{nid:08x}"),
                        "hex_id": f"!{nid:08x}",
                        "report_count": len(history),
                        "first_report": history[0]["timestamp"],
                        "last_report": history[-1]["timestamp"],
                        "current_neighbors": len(history[-1]["neighbors"]),
                        "stable_neighbors": len(stable_neighbors),
                        "unstable_neighbors": len(unstable_neighbors),
                        "neighbors_gained_count": len(neighbors_gained),
                        "neighbors_lost_count": len(neighbors_lost),
                        "stability_score": round(stability_score, 1),
                        "neighbors_gained": [
                            {
                                "node_id": ng["node_id"],
                                "node_name": node_names.get(
                                    ng["node_id"], f"!{ng['node_id']:08x}"
                                ),
                                "timestamp": ng["timestamp"],
                            }
                            for ng in neighbors_gained[-10:]  # Last 10 gained
                        ],
                        "neighbors_lost": [
                            {
                                "node_id": nl["node_id"],
                                "node_name": node_names.get(
                                    nl["node_id"], f"!{nl['node_id']:08x}"
                                ),
                                "timestamp": nl["timestamp"],
                            }
                            for nl in neighbors_lost[-10:]  # Last 10 lost
                        ],
                    }
                )

            # Sort by stability score (most unstable first)
            stability_results.sort(key=lambda x: x["stability_score"])

            return {
                "nodes": stability_results,
                "total_analyzed": len(stability_results),
                "analysis_hours": hours,
                "generated_at": time.time(),
            }

        except Exception as e:
            logger.error(f"Error analyzing neighbor stability: {e}", exc_info=True)
            raise

    @staticmethod
    def get_node_neighbors(node_id: int) -> dict[str, Any]:
        """
        Get detailed neighbor information for a specific node.

        Args:
            node_id: The node ID to get neighbors for

        Returns:
            Dictionary with node's current neighbors and history
        """
        logger.info(f"Getting neighbors for node {node_id}")

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Get the most recent NeighborInfo packet for this node
            cursor.execute(
                """
                SELECT id, from_node_id, timestamp, raw_payload, gateway_id
                FROM packet_history
                WHERE portnum_name = 'NEIGHBORINFO_APP'
                AND from_node_id = ?
                AND raw_payload IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
            """,
                (node_id,),
            )

            latest_packet = cursor.fetchone()

            if not latest_packet:
                conn.close()
                return {
                    "node_id": node_id,
                    "has_data": False,
                    "neighbors": [],
                }

            # Parse the packet
            neighbor_info = mesh_pb2.NeighborInfo()
            neighbor_info.ParseFromString(latest_packet["raw_payload"])

            neighbors = []
            neighbor_ids = []

            for neighbor in neighbor_info.neighbors:
                if neighbor.node_id:
                    neighbor_ids.append(neighbor.node_id)
                    neighbors.append(
                        {
                            "node_id": neighbor.node_id,
                            "snr": neighbor.snr if neighbor.snr else 0.0,
                            "last_rx_time": neighbor.last_rx_time
                            if neighbor.last_rx_time
                            else None,
                        }
                    )

            conn.close()

            # Get node names
            all_ids = [node_id] + neighbor_ids
            node_names = get_bulk_node_names(all_ids)

            # Enhance neighbor data with names
            for n in neighbors:
                n["node_name"] = node_names.get(n["node_id"], f"!{n['node_id']:08x}")
                n["hex_id"] = f"!{n['node_id']:08x}"

                # Classify SNR quality
                if n["snr"] >= 10:
                    n["quality"] = "excellent"
                elif n["snr"] >= 5:
                    n["quality"] = "good"
                elif n["snr"] >= 0:
                    n["quality"] = "fair"
                else:
                    n["quality"] = "poor"

            # Sort by SNR (best first)
            neighbors.sort(key=lambda x: x["snr"], reverse=True)

            return {
                "node_id": node_id,
                "node_name": node_names.get(node_id, f"!{node_id:08x}"),
                "hex_id": f"!{node_id:08x}",
                "has_data": True,
                "last_report": latest_packet["timestamp"],
                "neighbor_count": len(neighbors),
                "neighbors": neighbors,
                "broadcast_interval": neighbor_info.node_broadcast_interval_secs
                or None,
            }

        except Exception as e:
            logger.error(f"Error getting node neighbors: {e}", exc_info=True)
            raise
