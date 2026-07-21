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
        node_id: int, *, limit: int = 50
    ) -> dict[int, dict[str, Any]]:
        """Load 0-hop packet peers (heard-by / heard-from) without per-packet series."""
        peers: dict[int, dict[str, Any]] = {}
        gateway_hex = f"!{node_id:08x}"

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Nodes this radio heard directly (as gateway).
            cursor.execute(
                """
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
                GROUP BY p.from_node_id
                ORDER BY packet_count DESC
                LIMIT ?
                """,
                (gateway_hex, node_id, limit),
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
                """
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
                GROUP BY p.gateway_id
                ORDER BY packet_count DESC
                LIMIT ?
                """,
                (node_id, gateway_hex, limit),
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
    def get_zero_hop_neighbors(node_id: int, *, limit: int = 50) -> dict[str, Any]:
        """Build a zero-hop neighbor list for node detail.

        Merges:
        - NeighborInfo reports (node-advertised RF neighbors)
        - Observed 0-hop packet partners (``hop_start = hop_limit``)

        Returns a dict with ``neighbors`` suitable for the node detail sidebar.
        """
        from ..utils.formatting import format_time_ago

        node_id = int(node_id) & 0xFFFFFFFF
        by_id: dict[int, dict[str, Any]] = {}

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
                        "last_seen": n.get("last_rx_time") or ni_last_report,
                        "sources": ["neighborinfo"],
                        "heard_from": False,
                        "heard_by": False,
                        "is_bidirectional": False,
                    }
        except Exception as e:
            logger.warning("NeighborInfo lookup failed for %s: %s", node_id, e)

        # --- Observed 0-hop packet partners ---
        observed = NeighborService._load_observed_zero_hop_peers(node_id, limit=limit)
        name_ids = list(observed.keys()) + [node_id]
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
                    "is_bidirectional": bool(obs.get("heard_from") and obs.get("heard_by")),
                }
                by_id[peer_id] = entry
            else:
                if "observed" not in entry["sources"]:
                    entry["sources"].append("observed")
                entry["heard_from"] = bool(obs.get("heard_from"))
                entry["heard_by"] = bool(obs.get("heard_by"))
                entry["is_bidirectional"] = bool(
                    entry["heard_from"] and entry["heard_by"]
                )
                entry["packet_count"] = obs.get("packet_count")
                if obs.get("rssi_avg") is not None:
                    entry["rssi"] = obs["rssi_avg"]
                # Prefer stronger SNR when both sources report.
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

        neighbors = list(by_id.values())
        for n in neighbors:
            n["last_seen_relative"] = format_time_ago(n.get("last_seen"))
            sources = n.get("sources") or []
            if "neighborinfo" in sources and "observed" in sources:
                n["source_label"] = "Reported + Observed"
            elif "neighborinfo" in sources:
                n["source_label"] = "NeighborInfo"
            else:
                n["source_label"] = "Observed"

        # Best SNR first; fall back to packet count / name.
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

        return {
            "node_id": node_id,
            "hex_id": f"!{node_id:08x}",
            "node_name": names.get(node_id, f"!{node_id:08x}"),
            "has_data": bool(neighbors),
            "neighbor_count": len(neighbors),
            "last_neighborinfo_report": ni_last_report,
            "neighbors": neighbors,
        }
