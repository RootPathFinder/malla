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

# Router roles that should be highlighted in the dependency dashboard
ROUTER_ROLES = {"ROUTER", "ROUTER_CLIENT", "ROUTER_LATE", "REPEATER"}
# Roles shown on the Mesh Topology Router Reach map (includes base stations).
REACH_LAYER_ROLES = ROUTER_ROLES | {"CLIENT_BASE"}


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

            # Get node names and roles
            all_node_ids = list(nodes.keys())
            node_names = get_bulk_node_names(all_node_ids) if all_node_ids else {}
            node_roles = (
                NeighborService._get_bulk_node_roles(all_node_ids)
                if all_node_ids
                else {}
            )

            # Enhance nodes with names and roles
            for node_id, node_data in nodes.items():
                node_data["name"] = node_names.get(node_id, f"!{node_id:08x}")
                node_data["hex_id"] = f"!{node_id:08x}"
                role = node_roles.get(node_id)
                node_data["role"] = role
                node_data["is_router"] = role in ROUTER_ROLES if role else False

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
    def _get_bulk_node_roles(node_ids: list[int]) -> dict[int, str | None]:
        """
        Get roles for multiple nodes in a single database query.

        Args:
            node_ids: List of node IDs to get roles for

        Returns:
            Dictionary mapping node_id to role (or None if not found)
        """
        if not node_ids:
            return {}

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            placeholders = ",".join("?" * len(node_ids))
            cursor.execute(
                f"""
                SELECT node_id, role
                FROM node_info
                WHERE node_id IN ({placeholders})
            """,
                node_ids,
            )

            results = cursor.fetchall()
            conn.close()

            return {row["node_id"]: row["role"] for row in results}

        except Exception as e:
            logger.warning(f"Failed to get bulk node roles: {e}")
            return {}

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

    @staticmethod
    def _classify_snr_quality(snr: float | None) -> str:
        if snr is None:
            return "unknown"
        if snr >= 10:
            return "excellent"
        if snr >= 5:
            return "good"
        if snr >= 0:
            return "fair"
        return "poor"

    @staticmethod
    def _parse_gateway_node_id(gateway_id: Any) -> int | None:
        if gateway_id is None:
            return None
        if isinstance(gateway_id, int):
            return gateway_id & 0xFFFFFFFF
        if isinstance(gateway_id, str) and gateway_id.startswith("!"):
            try:
                return int(gateway_id[1:], 16)
            except ValueError:
                return None
        return None

    @staticmethod
    def _load_observed_zero_hop_peers(
        node_id: int, *, limit: int = 50, hours: int | None = None
    ) -> dict[int, dict[str, Any]]:
        """Load 0-hop packet peers (heard-by / heard-from) without per-packet series."""
        peers: dict[int, dict[str, Any]] = {}
        gateway_hex = f"!{node_id:08x}"
        cutoff = (time.time() - hours * 3600) if hours and hours > 0 else None

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            time_clause = ""
            time_params: list[Any] = []
            if cutoff is not None:
                time_clause = "AND p.timestamp >= ?"
                time_params = [cutoff]

            # Nodes this radio heard directly (as gateway).
            cursor.execute(
                f"""
                SELECT
                    p.from_node_id AS peer_id,
                    COUNT(*) AS packet_count,
                    AVG(CAST(p.rssi AS FLOAT)) AS rssi_avg,
                    AVG(CAST(p.snr AS FLOAT)) AS snr_avg,
                    MAX(p.timestamp) AS last_seen
                FROM packet_history p
                WHERE p.gateway_id = ?
                  AND p.from_node_id IS NOT NULL
                  AND p.from_node_id != ?
                  AND p.hop_start IS NOT NULL
                  AND p.hop_limit IS NOT NULL
                  AND p.hop_start = p.hop_limit
                  {time_clause}
                GROUP BY p.from_node_id
                ORDER BY packet_count DESC
                LIMIT ?
                """,
                (gateway_hex, node_id, *time_params, limit),
            )
            for row in cursor.fetchall():
                peer_id = int(row["peer_id"])
                peers[peer_id] = {
                    "node_id": peer_id,
                    "packet_count": int(row["packet_count"] or 0),
                    "rssi_avg": round(row["rssi_avg"], 1)
                    if row["rssi_avg"] is not None
                    else None,
                    "snr_avg": round(row["snr_avg"], 1)
                    if row["snr_avg"] is not None
                    else None,
                    "last_seen": row["last_seen"],
                    "heard_from": True,
                    "heard_by": False,
                }

            # Gateways that heard this node directly.
            cursor.execute(
                f"""
                SELECT
                    p.gateway_id,
                    COUNT(*) AS packet_count,
                    AVG(CAST(p.rssi AS FLOAT)) AS rssi_avg,
                    AVG(CAST(p.snr AS FLOAT)) AS snr_avg,
                    MAX(p.timestamp) AS last_seen
                FROM packet_history p
                WHERE p.from_node_id = ?
                  AND p.gateway_id IS NOT NULL
                  AND p.gateway_id != ?
                  AND p.hop_start IS NOT NULL
                  AND p.hop_limit IS NOT NULL
                  AND p.hop_start = p.hop_limit
                  {time_clause}
                GROUP BY p.gateway_id
                ORDER BY packet_count DESC
                LIMIT ?
                """,
                (node_id, gateway_hex, *time_params, limit),
            )
            for row in cursor.fetchall():
                peer_id = NeighborService._parse_gateway_node_id(row["gateway_id"])
                if peer_id is None or peer_id == node_id:
                    continue
                existing = peers.get(peer_id)
                if existing is None:
                    peers[peer_id] = {
                        "node_id": peer_id,
                        "packet_count": int(row["packet_count"] or 0),
                        "rssi_avg": round(row["rssi_avg"], 1)
                        if row["rssi_avg"] is not None
                        else None,
                        "snr_avg": round(row["snr_avg"], 1)
                        if row["snr_avg"] is not None
                        else None,
                        "last_seen": row["last_seen"],
                        "heard_from": False,
                        "heard_by": True,
                    }
                else:
                    existing["heard_by"] = True
                    existing["packet_count"] = int(existing["packet_count"] or 0) + int(
                        row["packet_count"] or 0
                    )
                    # Prefer the stronger/more recent observation when merging.
                    if row["last_seen"] and (
                        existing["last_seen"] is None
                        or row["last_seen"] > existing["last_seen"]
                    ):
                        existing["last_seen"] = row["last_seen"]
                    if row["snr_avg"] is not None:
                        snr = round(row["snr_avg"], 1)
                        if existing["snr_avg"] is None or snr > existing["snr_avg"]:
                            existing["snr_avg"] = snr
                    if row["rssi_avg"] is not None:
                        rssi = round(row["rssi_avg"], 1)
                        if existing["rssi_avg"] is None or rssi > existing["rssi_avg"]:
                            existing["rssi_avg"] = rssi

            conn.close()
        except Exception as e:
            logger.warning(
                "Failed loading observed zero-hop peers for %s: %s", node_id, e
            )

        return peers

    @staticmethod
    def _load_traceroute_rf_peers(
        node_id: int, *, hours: int = 168
    ) -> dict[int, dict[str, Any]]:
        """RF-adjacent peers from traceroute hops (not hop_count==0 receptions)."""
        peers: dict[int, dict[str, Any]] = {}
        try:
            from .traceroute_service import TracerouteService

            graph = TracerouteService.get_network_graph_data(hours=max(hours, 1))
            for link in graph.get("links") or []:
                source = link.get("source")
                target = link.get("target")
                if source == node_id:
                    peer_id = target
                elif target == node_id:
                    peer_id = source
                else:
                    continue
                if peer_id is None:
                    continue
                peer_id = int(peer_id)
                snr = link.get("avg_snr")
                try:
                    snr_f = float(snr) if snr is not None else None
                except (TypeError, ValueError):
                    snr_f = None
                existing = peers.get(peer_id)
                packet_count = int(link.get("packet_count") or 0)
                last_seen = link.get("last_seen")
                if existing is None:
                    peers[peer_id] = {
                        "node_id": peer_id,
                        "snr_avg": snr_f,
                        "packet_count": packet_count,
                        "last_seen": last_seen,
                    }
                else:
                    existing["packet_count"] = int(existing["packet_count"] or 0) + (
                        packet_count
                    )
                    if snr_f is not None and (
                        existing["snr_avg"] is None or snr_f > existing["snr_avg"]
                    ):
                        existing["snr_avg"] = snr_f
                    if last_seen and (
                        existing["last_seen"] is None
                        or last_seen > existing["last_seen"]
                    ):
                        existing["last_seen"] = last_seen
        except Exception as e:
            logger.warning(
                "Failed loading traceroute RF peers for %s: %s", node_id, e
            )
        return peers

    @staticmethod
    def _apply_neighborinfo_both_ways(
        node_id: int,
        by_id: dict[int, dict[str, Any]],
        *,
        hours: int = 168,
    ) -> None:
        """Mark NeighborInfo links confirmed when the peer also reports this node."""
        try:
            topo = NeighborService.get_mesh_topology(hours=max(hours, 1))
        except Exception as e:
            logger.debug("Topology both-ways lookup failed for %s: %s", node_id, e)
            return

        for edge in topo.get("edges") or []:
            a = edge.get("node_a")
            b = edge.get("node_b")
            if a == node_id:
                peer_id = b
            elif b == node_id:
                peer_id = a
            else:
                continue
            if peer_id is None:
                continue
            peer_id = int(peer_id)
            confirmed = bool(edge.get("confirmed_both_ways"))
            snr = edge.get("avg_snr")
            try:
                snr_f = float(snr) if snr is not None else None
            except (TypeError, ValueError):
                snr_f = None

            entry = by_id.get(peer_id)
            if entry is None:
                by_id[peer_id] = {
                    "node_id": peer_id,
                    "hex_id": f"!{peer_id:08x}",
                    "node_name": edge.get("node_a_name")
                    if peer_id == a
                    else edge.get("node_b_name"),
                    "snr": snr_f,
                    "rssi": None,
                    "quality": NeighborService._classify_snr_quality(snr_f),
                    "packet_count": None,
                    "last_seen": edge.get("last_seen"),
                    "sources": ["neighborinfo"],
                    "heard_from": False,
                    "heard_by": False,
                    "is_bidirectional": confirmed,
                    "confirmed_both_ways": confirmed,
                    "distance_km": None,
                    "distance_display": None,
                }
            else:
                if "neighborinfo" not in entry["sources"]:
                    entry["sources"].append("neighborinfo")
                entry["confirmed_both_ways"] = confirmed or entry.get(
                    "confirmed_both_ways", False
                )
                if entry["confirmed_both_ways"]:
                    entry["is_bidirectional"] = True
                if snr_f is not None and (
                    entry.get("snr") is None or snr_f > entry["snr"]
                ):
                    entry["snr"] = snr_f
                    entry["quality"] = NeighborService._classify_snr_quality(snr_f)

    @staticmethod
    def _apply_peer_distances(
        node_id: int, by_id: dict[int, dict[str, Any]]
    ) -> dict[str, float] | None:
        """Attach lat/lon + geographic distance when positions are known.

        Returns the subject node's ``{latitude, longitude}`` when available.
        """
        center: dict[str, float] | None = None
        try:
            from ..database.repositories import LocationRepository
            from ..utils.geo_utils import calculate_distance

            node_ids = [node_id, *list(by_id.keys())]
            locations = LocationRepository.get_node_locations(
                {"node_ids": node_ids}
            )
            by_loc = {
                int(loc["node_id"]): loc
                for loc in locations
                if loc.get("latitude") is not None and loc.get("longitude") is not None
            }
            self_loc = by_loc.get(node_id)
            if self_loc:
                center = {
                    "latitude": float(self_loc["latitude"]),
                    "longitude": float(self_loc["longitude"]),
                }
            for peer_id, entry in by_id.items():
                peer_loc = by_loc.get(peer_id)
                if not peer_loc:
                    entry.setdefault("latitude", None)
                    entry.setdefault("longitude", None)
                    continue
                entry["latitude"] = float(peer_loc["latitude"])
                entry["longitude"] = float(peer_loc["longitude"])
                if not self_loc:
                    continue
                dist_km = calculate_distance(
                    self_loc["latitude"],
                    self_loc["longitude"],
                    peer_loc["latitude"],
                    peer_loc["longitude"],
                )
                entry["distance_km"] = round(dist_km, 2)
                if dist_km < 1:
                    entry["distance_display"] = f"{int(round(dist_km * 1000))} m"
                else:
                    entry["distance_display"] = f"{dist_km:.1f} km"
        except Exception as e:
            logger.debug("Distance enrichment failed for %s: %s", node_id, e)
        return center

    @staticmethod
    def _source_label(sources: list[str]) -> str:
        parts: list[str] = []
        has_ni = "neighborinfo" in sources
        has_obs = "observed" in sources
        has_tr = "traceroute" in sources
        if has_ni and has_obs:
            parts.append("Reported + Observed")
        elif has_ni:
            parts.append("NeighborInfo")
        elif has_obs:
            parts.append("Observed")
        if has_tr:
            parts.append("Traceroute RF")
        return " · ".join(parts) if parts else "Unknown"

    @staticmethod
    def get_zero_hop_neighbors(
        node_id: int, *, limit: int = 50, hours: int | None = 168
    ) -> dict[str, Any]:
        """Build a zero-hop / direct-RF neighbor list for node detail.

        Merges:
        - NeighborInfo reports (node-advertised RF neighbors)
        - Observed 0-hop packet partners (``hop_start = hop_limit``)
        - Traceroute RF-adjacent peers (labeled separately)

        ``hours`` limits observed/traceroute/topology windows. ``None`` or ``0``
        means no time filter for observed packets (traceroute still uses 168h).
        """
        from ..utils.formatting import format_time_ago

        node_id = int(node_id) & 0xFFFFFFFF
        by_id: dict[int, dict[str, Any]] = {}
        window_hours = hours if hours and hours > 0 else None
        cutoff = (time.time() - window_hours * 3600) if window_hours else None
        topo_hours = window_hours or 168

        # --- NeighborInfo (authoritative when present) ---
        ni_last_report: float | None = None
        try:
            ni = NeighborService.get_node_neighbors(node_id)
            if ni.get("has_data"):
                ni_last_report = ni.get("last_report")
                for n in ni.get("neighbors") or []:
                    nid = n.get("node_id")
                    if nid is None:
                        continue
                    nid = int(nid)
                    last_seen = n.get("last_rx_time") or ni_last_report
                    if cutoff is not None and last_seen is not None and last_seen < cutoff:
                        continue
                    snr = n.get("snr")
                    try:
                        snr_f = float(snr) if snr is not None else None
                    except (TypeError, ValueError):
                        snr_f = None
                    by_id[nid] = {
                        "node_id": nid,
                        "hex_id": f"!{nid:08x}",
                        "node_name": n.get("node_name") or f"!{nid:08x}",
                        "snr": snr_f,
                        "rssi": None,
                        "quality": NeighborService._classify_snr_quality(snr_f),
                        "packet_count": None,
                        "last_seen": last_seen,
                        "sources": ["neighborinfo"],
                        "heard_from": False,
                        "heard_by": False,
                        "is_bidirectional": False,
                        "confirmed_both_ways": False,
                        "distance_km": None,
                        "distance_display": None,
                    }
        except Exception as e:
            logger.warning("NeighborInfo lookup failed for %s: %s", node_id, e)

        # Topology both-ways confirmation (+ reverse-only NI edges)
        NeighborService._apply_neighborinfo_both_ways(
            node_id, by_id, hours=topo_hours
        )

        # --- Observed 0-hop packet partners ---
        observed = NeighborService._load_observed_zero_hop_peers(
            node_id, limit=limit, hours=window_hours
        )

        # --- Traceroute RF-adjacent peers ---
        traceroute_peers = NeighborService._load_traceroute_rf_peers(
            node_id, hours=topo_hours
        )

        name_ids = list(
            {node_id, *by_id.keys(), *observed.keys(), *traceroute_peers.keys()}
        )
        names = get_bulk_node_names(name_ids)

        for peer_id, obs in observed.items():
            snr_f = obs.get("snr_avg")
            entry = by_id.get(peer_id)
            if entry is None:
                entry = {
                    "node_id": peer_id,
                    "hex_id": f"!{peer_id:08x}",
                    "node_name": names.get(peer_id, f"!{peer_id:08x}"),
                    "snr": snr_f,
                    "rssi": obs.get("rssi_avg"),
                    "quality": NeighborService._classify_snr_quality(snr_f),
                    "packet_count": obs.get("packet_count"),
                    "last_seen": obs.get("last_seen"),
                    "sources": ["observed"],
                    "heard_from": bool(obs.get("heard_from")),
                    "heard_by": bool(obs.get("heard_by")),
                    "is_bidirectional": bool(
                        obs.get("heard_from") and obs.get("heard_by")
                    ),
                    "confirmed_both_ways": False,
                    "distance_km": None,
                    "distance_display": None,
                }
                by_id[peer_id] = entry
            else:
                if "observed" not in entry["sources"]:
                    entry["sources"].append("observed")
                entry["heard_from"] = bool(obs.get("heard_from"))
                entry["heard_by"] = bool(obs.get("heard_by"))
                entry["is_bidirectional"] = bool(
                    entry["heard_from"] and entry["heard_by"]
                ) or bool(entry.get("confirmed_both_ways"))
                entry["packet_count"] = obs.get("packet_count")
                if obs.get("rssi_avg") is not None:
                    entry["rssi"] = obs["rssi_avg"]
                if snr_f is not None and (
                    entry["snr"] is None or snr_f > entry["snr"]
                ):
                    entry["snr"] = snr_f
                    entry["quality"] = NeighborService._classify_snr_quality(snr_f)
                if obs.get("last_seen") and (
                    entry["last_seen"] is None or obs["last_seen"] > entry["last_seen"]
                ):
                    entry["last_seen"] = obs["last_seen"]
                if not entry.get("node_name"):
                    entry["node_name"] = names.get(peer_id, f"!{peer_id:08x}")

        for peer_id, tr in traceroute_peers.items():
            snr_f = tr.get("snr_avg")
            entry = by_id.get(peer_id)
            if entry is None:
                by_id[peer_id] = {
                    "node_id": peer_id,
                    "hex_id": f"!{peer_id:08x}",
                    "node_name": names.get(peer_id, f"!{peer_id:08x}"),
                    "snr": snr_f,
                    "rssi": None,
                    "quality": NeighborService._classify_snr_quality(snr_f),
                    "packet_count": tr.get("packet_count"),
                    "last_seen": tr.get("last_seen"),
                    "sources": ["traceroute"],
                    "heard_from": False,
                    "heard_by": False,
                    "is_bidirectional": False,
                    "confirmed_both_ways": False,
                    "distance_km": None,
                    "distance_display": None,
                }
            else:
                if "traceroute" not in entry["sources"]:
                    entry["sources"].append("traceroute")
                if tr.get("packet_count") and not entry.get("packet_count"):
                    entry["packet_count"] = tr.get("packet_count")
                if snr_f is not None and (
                    entry["snr"] is None or snr_f > entry["snr"]
                ):
                    entry["snr"] = snr_f
                    entry["quality"] = NeighborService._classify_snr_quality(snr_f)
                if tr.get("last_seen") and (
                    entry["last_seen"] is None or tr["last_seen"] > entry["last_seen"]
                ):
                    entry["last_seen"] = tr["last_seen"]
                if not entry.get("node_name"):
                    entry["node_name"] = names.get(peer_id, f"!{peer_id:08x}")

        # Ensure names for topology-added peers
        for peer_id, entry in by_id.items():
            if not entry.get("node_name") or str(entry["node_name"]).startswith("!"):
                named = names.get(peer_id)
                if named:
                    entry["node_name"] = named
            entry["hex_id"] = f"!{peer_id:08x}"

        center = NeighborService._apply_peer_distances(node_id, by_id)

        neighbors = list(by_id.values())
        for n in neighbors:
            n["last_seen_relative"] = format_time_ago(n.get("last_seen"))
            n["source_label"] = NeighborService._source_label(n.get("sources") or [])
            n.setdefault("latitude", None)
            n.setdefault("longitude", None)
            # Prefer observed direction for Direct Receptions deep-link
            if n.get("heard_from"):
                n["direct_receptions_direction"] = "received"
            elif n.get("heard_by"):
                n["direct_receptions_direction"] = "transmitted"
            else:
                n["direct_receptions_direction"] = "received"

        neighbors.sort(
            key=lambda n: (
                n.get("snr") is not None,
                n.get("snr") if n.get("snr") is not None else -999,
                n.get("packet_count") or 0,
            ),
            reverse=True,
        )
        if limit and len(neighbors) > limit:
            neighbors = neighbors[:limit]

        both_ways = sum(
            1
            for n in neighbors
            if n.get("confirmed_both_ways") or n.get("is_bidirectional")
        )
        with_location = sum(
            1
            for n in neighbors
            if n.get("latitude") is not None and n.get("longitude") is not None
        )
        snr_vals = [
            float(n["snr"]) for n in neighbors if n.get("snr") is not None
        ]
        dist_vals = [
            float(n["distance_km"])
            for n in neighbors
            if n.get("distance_km") is not None
        ]
        summary = {
            "neighbor_count": len(neighbors),
            "both_ways": both_ways,
            "one_way": max(0, len(neighbors) - both_ways),
            "with_location": with_location,
            "avg_snr": round(sum(snr_vals) / len(snr_vals), 1) if snr_vals else None,
            "best_snr": round(max(snr_vals), 1) if snr_vals else None,
            "avg_distance_km": (
                round(sum(dist_vals) / len(dist_vals), 2) if dist_vals else None
            ),
            "max_distance_km": round(max(dist_vals), 2) if dist_vals else None,
        }

        return {
            "node_id": node_id,
            "hex_id": f"!{node_id:08x}",
            "node_name": names.get(node_id, f"!{node_id:08x}"),
            "has_data": bool(neighbors),
            "neighbor_count": len(neighbors),
            "last_neighborinfo_report": ni_last_report,
            "hours": window_hours,
            "center": center,
            "summary": summary,
            "neighbors": neighbors,
        }

    # Distinct palette for overlaying many router reach layers on one map.
    _ROUTER_REACH_COLORS = (
        "#0d6efd",
        "#198754",
        "#fd7e14",
        "#6f42c1",
        "#d63384",
        "#20c997",
        "#dc3545",
        "#0dcaf0",
        "#ffc107",
        "#6610f2",
        "#e83e8c",
        "#17a2b8",
    )

    @staticmethod
    def _normalize_device_role(role: Any) -> str:
        """Normalize node_info / location role values to uppercase role names."""
        if role is None:
            return ""
        if isinstance(role, bool):
            return ""
        if isinstance(role, (int, float)) and int(role) == role:
            from .config_metadata import DEVICE_ROLE_ENUM

            return str(DEVICE_ROLE_ENUM.get(int(role), "")).upper()
        text = str(role).strip()
        if not text:
            return ""
        if text.isdigit():
            from .config_metadata import DEVICE_ROLE_ENUM

            return str(DEVICE_ROLE_ENUM.get(int(text), "")).upper()
        return text.upper().replace(" ", "_")

    @classmethod
    def _is_reach_layer_role(cls, role: Any) -> bool:
        return cls._normalize_device_role(role) in REACH_LAYER_ROLES

    @staticmethod
    def _list_router_candidates(limit: int = 500) -> list[dict[str, Any]]:
        """Return non-archived router/base-station nodes, newest first."""
        roles = sorted(REACH_LAYER_ROLES)
        placeholders = ",".join("?" * len(roles))
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            # Match both canonical names and numeric role ids used by older data.
            numeric_roles = []
            try:
                from .config_metadata import DEVICE_ROLE_ENUM

                numeric_roles = [
                    str(num)
                    for num, name in DEVICE_ROLE_ENUM.items()
                    if name in REACH_LAYER_ROLES
                ]
            except Exception:
                numeric_roles = []
            role_values = roles + numeric_roles
            placeholders = ",".join("?" * len(role_values))
            cursor.execute(
                f"""
                SELECT node_id, long_name, short_name, role, last_seen
                FROM node_info
                WHERE COALESCE(archived, 0) = 0
                  AND role IN ({placeholders})
                ORDER BY (last_seen IS NULL), last_seen DESC
                LIMIT ?
                """,
                (*role_values, int(limit)),
            )
            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.warning("Failed listing router candidates: %s", e)
            return []

    @staticmethod
    def _list_positioned_reach_role_ids(limit: int = 200) -> list[int]:
        """Reach-layer role nodes that have at least one POSITION packet.

        Uses an EXISTS check (no protobuf decode) so GPS routers outside a short
        last_seen window are still discovered without scanning the whole mesh.
        """
        roles = sorted(REACH_LAYER_ROLES)
        try:
            from .config_metadata import DEVICE_ROLE_ENUM

            numeric_roles = [
                str(num)
                for num, name in DEVICE_ROLE_ENUM.items()
                if name in REACH_LAYER_ROLES
            ]
        except Exception:
            numeric_roles = []
        role_values = roles + numeric_roles
        placeholders = ",".join("?" * len(role_values))
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT ni.node_id
                FROM node_info ni
                WHERE COALESCE(ni.archived, 0) = 0
                  AND ni.role IN ({placeholders})
                  AND EXISTS (
                      SELECT 1
                      FROM packet_history ph
                      WHERE ph.from_node_id = ni.node_id
                        AND ph.portnum = 3
                        AND ph.raw_payload IS NOT NULL
                  )
                ORDER BY (ni.last_seen IS NULL), ni.last_seen DESC
                LIMIT ?
                """,
                (*role_values, int(limit)),
            )
            ids = [int(row["node_id"]) for row in cursor.fetchall()]
            conn.close()
            return ids
        except Exception as e:
            logger.warning("Failed listing positioned router ids: %s", e)
            return []

    @staticmethod
    def _resolve_located_routers(
        max_routers: int, *, topology: dict[str, Any] | None = None
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Find routers/base stations that have GPS positions.

        Targeted location lookups only (never decodes every mesh position).
        Combines role candidates, positioned role nodes, and topology routers.
        """
        from ..database.repositories import LocationRepository

        stats = {
            "router_candidates": 0,
            "locations_total": 0,
            "routers_with_location": 0,
        }

        candidates = NeighborService._list_router_candidates(
            limit=max(80, max_routers * 5)
        )
        stats["router_candidates"] = len(candidates)
        by_candidate = {int(row["node_id"]): row for row in candidates}

        positioned_ids = NeighborService._list_positioned_reach_role_ids(
            limit=max(80, max_routers * 5)
        )
        lookup_ids: list[int] = []
        seen_ids: set[int] = set()
        for nid in list(by_candidate.keys()) + positioned_ids:
            if nid in seen_ids:
                continue
            seen_ids.add(nid)
            lookup_ids.append(nid)

        located_by_id: dict[int, dict[str, Any]] = {}
        try:
            candidate_locs = (
                LocationRepository.get_node_locations({"node_ids": lookup_ids})
                if lookup_ids
                else []
            )
        except Exception as e:
            logger.warning("Failed loading candidate router locations: %s", e)
            candidate_locs = []
        stats["locations_total"] = len(candidate_locs)

        for loc in candidate_locs:
            try:
                node_id = int(loc["node_id"])
            except (TypeError, ValueError, KeyError):
                continue
            if loc.get("latitude") is None or loc.get("longitude") is None:
                continue
            candidate = by_candidate.get(node_id) or {}
            role = candidate.get("role") or loc.get("role")
            if node_id not in by_candidate and not NeighborService._is_reach_layer_role(
                role
            ):
                continue
            located_by_id[node_id] = {
                "node_id": node_id,
                "hex_id": f"!{node_id:08x}",
                "node_name": (
                    candidate.get("long_name")
                    or candidate.get("short_name")
                    or loc.get("display_name")
                    or loc.get("long_name")
                    or loc.get("short_name")
                    or f"!{node_id:08x}"
                ),
                "role": NeighborService._normalize_device_role(role) or "ROUTER",
                "last_seen": candidate.get("last_seen") or loc.get("timestamp"),
                "latitude": float(loc["latitude"]),
                "longitude": float(loc["longitude"]),
            }

        # Topology fallback when role metadata is sparse.
        if len(located_by_id) < max_routers:
            if topology is None:
                try:
                    topology = NeighborService.get_mesh_topology(hours=168)
                except Exception:
                    topology = None
            if topology and topology.get("nodes"):
                topo_ids = [
                    int(n["node_id"])
                    for n in topology["nodes"]
                    if n.get("node_id") is not None
                    and (
                        n.get("is_router")
                        or NeighborService._is_reach_layer_role(n.get("role"))
                    )
                    and int(n["node_id"]) not in located_by_id
                ]
                if topo_ids:
                    try:
                        topo_locs = LocationRepository.get_node_locations(
                            {"node_ids": topo_ids}
                        )
                    except Exception:
                        topo_locs = []
                    stats["locations_total"] += len(topo_locs)
                    topo_by_id = {
                        int(n["node_id"]): n
                        for n in topology["nodes"]
                        if n.get("node_id") is not None
                    }
                    for loc in topo_locs:
                        try:
                            node_id = int(loc["node_id"])
                        except (TypeError, ValueError, KeyError):
                            continue
                        if node_id in located_by_id:
                            continue
                        if loc.get("latitude") is None or loc.get("longitude") is None:
                            continue
                        node = topo_by_id.get(node_id) or {}
                        located_by_id[node_id] = {
                            "node_id": node_id,
                            "hex_id": node.get("hex_id") or f"!{node_id:08x}",
                            "node_name": node.get("name")
                            or loc.get("display_name")
                            or f"!{node_id:08x}",
                            "role": NeighborService._normalize_device_role(
                                node.get("role") or loc.get("role")
                            )
                            or "ROUTER",
                            "last_seen": node.get("last_seen") or loc.get("timestamp"),
                            "latitude": float(loc["latitude"]),
                            "longitude": float(loc["longitude"]),
                        }

        located = sorted(
            located_by_id.values(),
            key=lambda r: (
                r.get("last_seen") is None,
                -(r.get("last_seen") or 0),
            ),
        )
        stats["routers_with_location"] = len(located)
        return located[:max_routers], stats

    @staticmethod
    def _load_observed_zero_hop_peers_batch(
        router_ids: list[int],
        *,
        hours: int | None = 168,
        limit_per_router: int = 40,
    ) -> dict[int, dict[int, dict[str, Any]]]:
        """Batch 0-hop observed peers for many routers (2 SQL queries total)."""
        result: dict[int, dict[int, dict[str, Any]]] = {int(r): {} for r in router_ids}
        if not router_ids:
            return result

        router_ids = [int(r) for r in router_ids]
        gateway_hexes = [f"!{nid:08x}" for nid in router_ids]
        hex_to_id = {f"!{nid:08x}".lower(): nid for nid in router_ids}
        cutoff = (time.time() - hours * 3600) if hours and hours > 0 else None
        limit_per_router = max(5, min(int(limit_per_router), 80))

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            time_clause = ""
            time_params: list[Any] = []
            if cutoff is not None:
                time_clause = "AND p.timestamp >= ?"
                time_params = [cutoff]

            gw_placeholders = ",".join("?" * len(gateway_hexes))
            cursor.execute(
                f"""
                SELECT
                    p.gateway_id AS gateway_id,
                    p.from_node_id AS peer_id,
                    COUNT(*) AS packet_count,
                    AVG(CAST(p.snr AS FLOAT)) AS snr_avg,
                    MAX(p.timestamp) AS last_seen
                FROM packet_history p
                WHERE p.gateway_id IN ({gw_placeholders})
                  AND p.from_node_id IS NOT NULL
                  AND p.hop_start IS NOT NULL
                  AND p.hop_limit IS NOT NULL
                  AND p.hop_start = p.hop_limit
                  {time_clause}
                GROUP BY p.gateway_id, p.from_node_id
                """,
                (*gateway_hexes, *time_params),
            )
            for row in cursor.fetchall():
                router_id = hex_to_id.get(str(row["gateway_id"] or "").lower())
                if router_id is None:
                    # gateway_id may differ by case
                    router_id = hex_to_id.get(str(row["gateway_id"] or ""))
                if router_id is None:
                    parsed = NeighborService._parse_gateway_node_id(row["gateway_id"])
                    if parsed in result:
                        router_id = parsed
                if router_id is None:
                    continue
                try:
                    peer_id = int(row["peer_id"])
                except (TypeError, ValueError):
                    continue
                if peer_id == router_id:
                    continue
                result[router_id][peer_id] = {
                    "node_id": peer_id,
                    "packet_count": int(row["packet_count"] or 0),
                    "snr_avg": round(row["snr_avg"], 1)
                    if row["snr_avg"] is not None
                    else None,
                    "last_seen": row["last_seen"],
                    "heard_from": True,
                    "heard_by": False,
                }

            id_placeholders = ",".join("?" * len(router_ids))
            cursor.execute(
                f"""
                SELECT
                    p.from_node_id AS router_id,
                    p.gateway_id AS gateway_id,
                    COUNT(*) AS packet_count,
                    AVG(CAST(p.snr AS FLOAT)) AS snr_avg,
                    MAX(p.timestamp) AS last_seen
                FROM packet_history p
                WHERE p.from_node_id IN ({id_placeholders})
                  AND p.gateway_id IS NOT NULL
                  AND p.hop_start IS NOT NULL
                  AND p.hop_limit IS NOT NULL
                  AND p.hop_start = p.hop_limit
                  {time_clause}
                GROUP BY p.from_node_id, p.gateway_id
                """,
                (*router_ids, *time_params),
            )
            for row in cursor.fetchall():
                try:
                    router_id = int(row["router_id"])
                except (TypeError, ValueError):
                    continue
                if router_id not in result:
                    continue
                peer_id = NeighborService._parse_gateway_node_id(row["gateway_id"])
                if peer_id is None or peer_id == router_id:
                    continue
                existing = result[router_id].get(peer_id)
                if existing is None:
                    result[router_id][peer_id] = {
                        "node_id": peer_id,
                        "packet_count": int(row["packet_count"] or 0),
                        "snr_avg": round(row["snr_avg"], 1)
                        if row["snr_avg"] is not None
                        else None,
                        "last_seen": row["last_seen"],
                        "heard_from": False,
                        "heard_by": True,
                    }
                else:
                    existing["heard_by"] = True
                    existing["packet_count"] = int(existing["packet_count"] or 0) + int(
                        row["packet_count"] or 0
                    )
                    if row["snr_avg"] is not None:
                        snr = round(row["snr_avg"], 1)
                        if existing["snr_avg"] is None or snr > existing["snr_avg"]:
                            existing["snr_avg"] = snr
                    if row["last_seen"] and (
                        existing["last_seen"] is None
                        or row["last_seen"] > existing["last_seen"]
                    ):
                        existing["last_seen"] = row["last_seen"]

            conn.close()
        except Exception as e:
            logger.warning("Batch observed zero-hop load failed: %s", e)
            return {int(r): {} for r in router_ids}

        # Keep strongest peers per router.
        trimmed: dict[int, dict[int, dict[str, Any]]] = {}
        for router_id, peers in result.items():
            ranked = sorted(
                peers.values(),
                key=lambda p: (
                    p.get("packet_count") or 0,
                    p.get("snr_avg") if p.get("snr_avg") is not None else -999,
                ),
                reverse=True,
            )[:limit_per_router]
            trimmed[router_id] = {int(p["node_id"]): p for p in ranked}
        return trimmed

    @staticmethod
    def _neighbors_from_topology_edges(
        router_ids: list[int], topology: dict[str, Any] | None
    ) -> dict[int, dict[int, dict[str, Any]]]:
        """Project NeighborInfo topology edges onto each router."""
        out: dict[int, dict[int, dict[str, Any]]] = {int(r): {} for r in router_ids}
        if not topology:
            return out
        router_set = set(out.keys())
        for edge in topology.get("edges") or []:
            try:
                node_a = int(edge["node_a"])
                node_b = int(edge["node_b"])
            except (TypeError, ValueError, KeyError):
                continue
            both = bool(edge.get("confirmed_both_ways"))
            for router_id, peer_id, snr in (
                (node_a, node_b, edge.get("snr_a_to_b")),
                (node_b, node_a, edge.get("snr_b_to_a")),
            ):
                if router_id not in router_set:
                    continue
                try:
                    snr_f = float(snr) if snr is not None else None
                except (TypeError, ValueError):
                    snr_f = None
                # Prefer avg when directional missing
                if snr_f is None and edge.get("avg_snr") is not None:
                    try:
                        snr_f = float(edge["avg_snr"])
                    except (TypeError, ValueError):
                        snr_f = None
                out[router_id][peer_id] = {
                    "node_id": peer_id,
                    "snr": snr_f,
                    "confirmed_both_ways": both,
                    "heard_from": True,
                    "heard_by": both,
                    "sources": ["neighborinfo"],
                    "last_seen": edge.get("last_seen"),
                }
        return out

    @staticmethod
    def get_router_reach_layers(
        *,
        hours: int | None = 168,
        max_routers: int = 20,
        neighbors_per_router: int = 40,
    ) -> dict[str, Any]:
        """Build multi-router Reach overlays for the topology map.

        Same Reach sources as node detail (NeighborInfo + observed 0-hop), but
        batched into a few queries instead of one full Reach build per router.
        """
        from ..database.repositories import LocationRepository
        from ..utils.geo_utils import calculate_distance

        max_routers = max(1, min(int(max_routers), 40))
        neighbors_per_router = max(5, min(int(neighbors_per_router), 60))
        window_hours = hours if hours and hours > 0 else 168

        cache_key = f"router_reach:{window_hours}:{max_routers}:{neighbors_per_router}"
        now = time.time()
        cached = NeighborService._topology_cache.get(cache_key)
        if cached and now - cached[0] < NeighborService._CACHE_TTL:
            return cached[1]

        try:
            topology = NeighborService.get_mesh_topology(hours=window_hours)
        except Exception as e:
            logger.warning("Router reach topology unavailable: %s", e)
            topology = None

        located, discovery_stats = NeighborService._resolve_located_routers(
            max_routers, topology=topology
        )
        if not located:
            empty = {
                "hours": window_hours,
                "routers": [],
                "statistics": {
                    **discovery_stats,
                    "routers_mapped": 0,
                    "total_reach_links": 0,
                    "mapped_reach_links": 0,
                },
            }
            NeighborService._topology_cache[cache_key] = (now, empty)
            return empty

        router_ids = [int(r["node_id"]) for r in located]
        ni_by_router = NeighborService._neighbors_from_topology_edges(
            router_ids, topology
        )
        observed_by_router = NeighborService._load_observed_zero_hop_peers_batch(
            router_ids, hours=window_hours, limit_per_router=neighbors_per_router
        )

        # Merge NeighborInfo + observed into per-router peer maps.
        merged: dict[int, dict[int, dict[str, Any]]] = {}
        all_peer_ids: set[int] = set(router_ids)
        for router_id in router_ids:
            peers: dict[int, dict[str, Any]] = {}
            for peer_id, ni in (ni_by_router.get(router_id) or {}).items():
                peers[peer_id] = {
                    "node_id": peer_id,
                    "snr": ni.get("snr"),
                    "confirmed_both_ways": bool(ni.get("confirmed_both_ways")),
                    "heard_from": bool(ni.get("heard_from")),
                    "heard_by": bool(ni.get("heard_by")),
                    "is_bidirectional": bool(ni.get("confirmed_both_ways")),
                    "sources": list(ni.get("sources") or ["neighborinfo"]),
                    "last_seen": ni.get("last_seen"),
                    "packet_count": None,
                }
            for peer_id, obs in (observed_by_router.get(router_id) or {}).items():
                entry = peers.get(peer_id)
                if entry is None:
                    peers[peer_id] = {
                        "node_id": peer_id,
                        "snr": obs.get("snr_avg"),
                        "confirmed_both_ways": False,
                        "heard_from": bool(obs.get("heard_from")),
                        "heard_by": bool(obs.get("heard_by")),
                        "is_bidirectional": bool(
                            obs.get("heard_from") and obs.get("heard_by")
                        ),
                        "sources": ["observed"],
                        "last_seen": obs.get("last_seen"),
                        "packet_count": obs.get("packet_count"),
                    }
                else:
                    if "observed" not in entry["sources"]:
                        entry["sources"].append("observed")
                    entry["heard_from"] = entry["heard_from"] or bool(
                        obs.get("heard_from")
                    )
                    entry["heard_by"] = entry["heard_by"] or bool(obs.get("heard_by"))
                    entry["is_bidirectional"] = bool(
                        entry.get("confirmed_both_ways")
                        or (entry["heard_from"] and entry["heard_by"])
                    )
                    entry["packet_count"] = obs.get("packet_count")
                    if obs.get("snr_avg") is not None and (
                        entry["snr"] is None or obs["snr_avg"] > entry["snr"]
                    ):
                        entry["snr"] = obs["snr_avg"]
                    if obs.get("last_seen") and (
                        entry["last_seen"] is None
                        or obs["last_seen"] > entry["last_seen"]
                    ):
                        entry["last_seen"] = obs["last_seen"]
            # Cap peers
            ranked = sorted(
                peers.values(),
                key=lambda p: (
                    p.get("snr") is not None,
                    p.get("snr") if p.get("snr") is not None else -999,
                    p.get("packet_count") or 0,
                ),
                reverse=True,
            )[:neighbors_per_router]
            merged[router_id] = {int(p["node_id"]): p for p in ranked}
            all_peer_ids.update(merged[router_id].keys())

        names = get_bulk_node_names(list(all_peer_ids)) if all_peer_ids else {}
        try:
            locations = LocationRepository.get_node_locations(
                {"node_ids": list(all_peer_ids)}
            )
        except Exception as e:
            logger.warning("Router reach peer locations failed: %s", e)
            locations = []
        by_loc = {
            int(loc["node_id"]): loc
            for loc in locations
            if loc.get("latitude") is not None and loc.get("longitude") is not None
        }

        routers_out: list[dict[str, Any]] = []
        total_links = 0
        mapped_links = 0
        for idx, router in enumerate(located):
            router_id = int(router["node_id"])
            router_loc = by_loc.get(router_id)
            latitude = (
                float(router_loc["latitude"])
                if router_loc
                else float(router["latitude"])
            )
            longitude = (
                float(router_loc["longitude"])
                if router_loc
                else float(router["longitude"])
            )

            neighbors_out: list[dict[str, Any]] = []
            for peer_id, peer in (merged.get(router_id) or {}).items():
                total_links += 1
                peer_loc = by_loc.get(peer_id)
                lat: float | None = None
                lon: float | None = None
                dist_km = None
                dist_display = None
                if (
                    peer_loc
                    and peer_loc.get("latitude") is not None
                    and peer_loc.get("longitude") is not None
                ):
                    lat = float(peer_loc["latitude"])
                    lon = float(peer_loc["longitude"])
                    mapped_links += 1
                    dist_km = round(
                        calculate_distance(latitude, longitude, lat, lon), 2
                    )
                    dist_display = (
                        f"{int(round(dist_km * 1000))} m"
                        if dist_km < 1
                        else f"{dist_km:.1f} km"
                    )
                snr = peer.get("snr")
                both = bool(
                    peer.get("confirmed_both_ways") or peer.get("is_bidirectional")
                )
                neighbors_out.append(
                    {
                        "node_id": peer_id,
                        "hex_id": f"!{peer_id:08x}",
                        "node_name": names.get(peer_id, f"!{peer_id:08x}"),
                        "snr": snr,
                        "quality": NeighborService._classify_snr_quality(
                            float(snr) if snr is not None else None
                        ),
                        "is_bidirectional": both,
                        "confirmed_both_ways": bool(peer.get("confirmed_both_ways")),
                        "distance_km": dist_km,
                        "distance_display": dist_display,
                        "latitude": lat,
                        "longitude": lon,
                        "source_label": NeighborService._source_label(
                            peer.get("sources") or []
                        ),
                    }
                )

            # Prefer geo peers first for the map payload.
            neighbors_out.sort(
                key=lambda n: (
                    n.get("latitude") is not None,
                    n.get("snr") is not None,
                    n.get("snr") if n.get("snr") is not None else -999,
                ),
                reverse=True,
            )
            snr_vals = [
                float(n["snr"]) for n in neighbors_out if n.get("snr") is not None
            ]
            dist_vals = [
                float(n["distance_km"])
                for n in neighbors_out
                if n.get("distance_km") is not None
            ]
            both_ways = sum(1 for n in neighbors_out if n.get("is_bidirectional"))
            summary = {
                "neighbor_count": len(neighbors_out),
                "both_ways": both_ways,
                "one_way": max(0, len(neighbors_out) - both_ways),
                "with_location": sum(
                    1
                    for n in neighbors_out
                    if n.get("latitude") is not None and n.get("longitude") is not None
                ),
                "avg_snr": round(sum(snr_vals) / len(snr_vals), 1) if snr_vals else None,
                "best_snr": round(max(snr_vals), 1) if snr_vals else None,
                "avg_distance_km": (
                    round(sum(dist_vals) / len(dist_vals), 2) if dist_vals else None
                ),
                "max_distance_km": round(max(dist_vals), 2) if dist_vals else None,
            }
            routers_out.append(
                {
                    "node_id": router_id,
                    "hex_id": router.get("hex_id") or f"!{router_id:08x}",
                    "node_name": names.get(router_id) or router["node_name"],
                    "role": router["role"],
                    # Sidebar identity only; map uses node-Reach colors.
                    "color": NeighborService._ROUTER_REACH_COLORS[
                        idx % len(NeighborService._ROUTER_REACH_COLORS)
                    ],
                    "latitude": latitude,
                    "longitude": longitude,
                    "neighbor_count": len(neighbors_out),
                    "mapped_neighbor_count": summary["with_location"],
                    "summary": summary,
                    "neighbors": neighbors_out,
                }
            )

        payload = {
            "hours": window_hours,
            "routers": routers_out,
            "statistics": {
                **discovery_stats,
                "routers_mapped": len(routers_out),
                "total_reach_links": total_links,
                "mapped_reach_links": mapped_links,
            },
        }
        NeighborService._topology_cache[cache_key] = (now, payload)
        return payload
