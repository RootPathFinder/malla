"""
Node health monitoring service for identifying problematic nodes in the mesh network.

This service analyzes various metrics to identify nodes that may be experiencing issues:
- Poor signal quality (low RSSI/SNR)
- High packet loss rates
- Frequent disconnections
- Battery drain issues
- Routing inefficiencies
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from ..database import get_db_connection
from ..utils.node_utils import get_bulk_node_names

logger = logging.getLogger(__name__)


class NodeHealthService:
    """Service for analyzing node health and identifying problematic nodes."""

    @staticmethod
    def analyze_node_health(
        node_id: int, hours: int = 24
    ) -> dict[str, Any] | None:
        """
        Analyze health metrics for a specific node.

        Args:
            node_id: The node ID to analyze
            hours: Number of hours to analyze (default: 24)

        Returns:
            Dictionary containing health metrics and issues, or None if node not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Calculate time threshold
        cutoff_time = int(time.time()) - (hours * 3600)

        # Get node basic info
        cursor.execute(
            """
            SELECT node_id, long_name, short_name, hw_model, role
            FROM node_info
            WHERE node_id = ?
        """,
            (node_id,),
        )
        node_row = cursor.fetchone()

        if not node_row:
            conn.close()
            return None

        node_info = dict(node_row)

        # Get packet statistics
        cursor.execute(
            """
            SELECT
                COUNT(*) as total_packets,
                AVG(rssi) as avg_rssi,
                MIN(rssi) as min_rssi,
                AVG(snr) as avg_snr,
                MIN(snr) as min_snr,
                AVG(CASE WHEN hop_start > 0 THEN (hop_start - hop_limit) * 1.0 / hop_start ELSE 0 END) as avg_hop_usage
            FROM packet_history
            WHERE from_node_id = ?
            AND timestamp >= ?
            AND rssi IS NOT NULL
        """,
            (node_id, cutoff_time),
        )
        packet_stats = dict(cursor.fetchone())

        # Get gateway statistics (how many different gateways heard this node)
        cursor.execute(
            """
            SELECT COUNT(DISTINCT gateway_id) as unique_gateways
            FROM packet_history
            WHERE from_node_id = ?
            AND timestamp >= ?
            AND gateway_id IS NOT NULL
        """,
            (node_id, cutoff_time),
        )
        gateway_stats = dict(cursor.fetchone())

        # Get activity timeline (packets per hour)
        cursor.execute(
            """
            SELECT
                CAST((timestamp - ?) / 3600 AS INTEGER) as hour_bucket,
                COUNT(*) as packet_count
            FROM packet_history
            WHERE from_node_id = ?
            AND timestamp >= ?
            GROUP BY hour_bucket
            ORDER BY hour_bucket
        """,
            (cutoff_time, node_id, cutoff_time),
        )
        activity_timeline = [dict(row) for row in cursor.fetchall()]

        # Calculate gaps in activity (periods of inactivity)
        cursor.execute(
            """
            SELECT timestamp
            FROM packet_history
            WHERE from_node_id = ?
            AND timestamp >= ?
            ORDER BY timestamp
        """,
            (node_id, cutoff_time),
        )
        timestamps = [row["timestamp"] for row in cursor.fetchall()]

        gaps = []
        if len(timestamps) > 1:
            for i in range(1, len(timestamps)):
                gap_duration = timestamps[i] - timestamps[i - 1]
                # Report gaps longer than 30 minutes
                if gap_duration > 1800:
                    gaps.append(
                        {
                            "start": timestamps[i - 1],
                            "end": timestamps[i],
                            "duration_minutes": gap_duration / 60,
                        }
                    )

        conn.close()

        # Analyze health issues
        issues = []
        health_score = 100  # Start with perfect score

        # Check signal quality
        if packet_stats["avg_rssi"] is not None:
            if packet_stats["avg_rssi"] < -120:
                issues.append(
                    {
                        "severity": "critical",
                        "category": "signal",
                        "message": f"Very poor average RSSI: {packet_stats['avg_rssi']:.1f} dBm",
                    }
                )
                health_score -= 30
            elif packet_stats["avg_rssi"] < -110:
                issues.append(
                    {
                        "severity": "warning",
                        "category": "signal",
                        "message": f"Poor average RSSI: {packet_stats['avg_rssi']:.1f} dBm",
                    }
                )
                health_score -= 15

        if packet_stats["avg_snr"] is not None:
            if packet_stats["avg_snr"] < -10:
                issues.append(
                    {
                        "severity": "critical",
                        "category": "signal",
                        "message": f"Very poor average SNR: {packet_stats['avg_snr']:.1f} dB",
                    }
                )
                health_score -= 25
            elif packet_stats["avg_snr"] < -5:
                issues.append(
                    {
                        "severity": "warning",
                        "category": "signal",
                        "message": f"Poor average SNR: {packet_stats['avg_snr']:.1f} dB",
                    }
                )
                health_score -= 10

        # Check packet activity
        if packet_stats["total_packets"] == 0:
            issues.append(
                {
                    "severity": "critical",
                    "category": "activity",
                    "message": f"No packets transmitted in the last {hours} hours",
                }
            )
            health_score -= 50
        elif packet_stats["total_packets"] < 5:
            issues.append(
                {
                    "severity": "warning",
                    "category": "activity",
                    "message": f"Very low activity: only {packet_stats['total_packets']} packets in {hours} hours",
                }
            )
            health_score -= 20

        # Check gateway connectivity
        if gateway_stats["unique_gateways"] == 0:
            issues.append(
                {
                    "severity": "critical",
                    "category": "connectivity",
                    "message": "Not heard by any gateways",
                }
            )
            health_score -= 40
        elif gateway_stats["unique_gateways"] == 1:
            issues.append(
                {
                    "severity": "info",
                    "category": "connectivity",
                    "message": "Only heard by one gateway (single point of failure)",
                }
            )
            health_score -= 5

        # Check for activity gaps
        long_gaps = [g for g in gaps if g["duration_minutes"] > 120]  # > 2 hours
        if long_gaps:
            total_gap_time = sum(g["duration_minutes"] for g in long_gaps)
            issues.append(
                {
                    "severity": "warning",
                    "category": "reliability",
                    "message": f"{len(long_gaps)} significant outage(s) totaling {total_gap_time:.0f} minutes",
                }
            )
            health_score -= min(25, len(long_gaps) * 5)

        # Ensure health score doesn't go below 0
        health_score = max(0, health_score)

        # Determine overall health status
        if health_score >= 80:
            health_status = "healthy"
        elif health_score >= 60:
            health_status = "degraded"
        elif health_score >= 40:
            health_status = "poor"
        else:
            health_status = "critical"

        return {
            "node_id": node_id,
            "node_info": node_info,
            "health_score": health_score,
            "health_status": health_status,
            "issues": issues,
            "metrics": {
                "total_packets": packet_stats["total_packets"],
                "avg_rssi": packet_stats["avg_rssi"],
                "min_rssi": packet_stats["min_rssi"],
                "avg_snr": packet_stats["avg_snr"],
                "min_snr": packet_stats["min_snr"],
                "avg_hop_usage": packet_stats["avg_hop_usage"],
                "unique_gateways": gateway_stats["unique_gateways"],
                "activity_gaps": len(gaps),
                "long_outages": len(long_gaps),
            },
            "activity_timeline": activity_timeline,
            "analyzed_hours": hours,
        }

    @staticmethod
    def get_problematic_nodes(
        hours: int = 24, min_health_score: int = 70, limit: int = 50
    ) -> list[dict[str, Any]]:
        """
        Identify problematic nodes in the network based on health metrics.

        Args:
            hours: Number of hours to analyze (default: 24)
            min_health_score: Only return nodes with health score below this (default: 70)
            limit: Maximum number of nodes to return (default: 50)

        Returns:
            List of problematic nodes with their health analysis
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Calculate time threshold
        cutoff_time = int(time.time()) - (hours * 3600)

        # Get all nodes that have been active
        cursor.execute(
            """
            SELECT DISTINCT from_node_id
            FROM packet_history
            WHERE timestamp >= ?
            AND from_node_id IS NOT NULL
        """,
            (cutoff_time,),
        )
        active_nodes = [row["from_node_id"] for row in cursor.fetchall()]

        conn.close()

        # Analyze each node
        problematic_nodes = []
        for node_id in active_nodes:
            health_data = NodeHealthService.analyze_node_health(node_id, hours)
            if health_data and health_data["health_score"] < min_health_score:
                problematic_nodes.append(health_data)

        # Sort by health score (worst first)
        problematic_nodes.sort(key=lambda x: x["health_score"])

        return problematic_nodes[:limit]

    @staticmethod
    def get_network_health_summary(hours: int = 24) -> dict[str, Any]:
        """
        Get overall network health summary.

        Args:
            hours: Number of hours to analyze (default: 24)

        Returns:
            Dictionary containing network-wide health metrics
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Calculate time threshold
        cutoff_time = int(time.time()) - (hours * 3600)

        # Get active nodes count
        cursor.execute(
            """
            SELECT COUNT(DISTINCT from_node_id) as active_nodes
            FROM packet_history
            WHERE timestamp >= ?
            AND from_node_id IS NOT NULL
        """,
            (cutoff_time,),
        )
        active_nodes_count = cursor.fetchone()["active_nodes"]

        # Get average signal quality across network
        cursor.execute(
            """
            SELECT
                AVG(rssi) as avg_rssi,
                AVG(snr) as avg_snr,
                COUNT(*) as total_packets
            FROM packet_history
            WHERE timestamp >= ?
            AND rssi IS NOT NULL
        """,
            (cutoff_time,),
        )
        network_stats = dict(cursor.fetchone())

        # Get nodes with poor signal
        cursor.execute(
            """
            SELECT COUNT(DISTINCT from_node_id) as poor_signal_nodes
            FROM packet_history
            WHERE timestamp >= ?
            AND from_node_id IS NOT NULL
            AND (rssi < -115 OR snr < -7)
        """,
            (cutoff_time,),
        )
        poor_signal_count = cursor.fetchone()["poor_signal_nodes"]

        # Get nodes not heard by any gateway
        cursor.execute(
            """
            SELECT COUNT(DISTINCT from_node_id) as isolated_nodes
            FROM packet_history p1
            WHERE timestamp >= ?
            AND from_node_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM packet_history p2
                WHERE p2.from_node_id = p1.from_node_id
                AND p2.timestamp >= ?
                AND p2.gateway_id IS NOT NULL
            )
        """,
            (cutoff_time, cutoff_time),
        )
        isolated_count = cursor.fetchone()["isolated_nodes"]

        conn.close()

        # Analyze a sample of nodes to get health distribution
        problematic_nodes = NodeHealthService.get_problematic_nodes(
            hours, min_health_score=100, limit=1000
        )

        health_distribution = {
            "healthy": 0,  # 80-100
            "degraded": 0,  # 60-79
            "poor": 0,  # 40-59
            "critical": 0,  # 0-39
        }

        for node in problematic_nodes:
            status = node["health_status"]
            health_distribution[status] = health_distribution.get(status, 0) + 1

        # Calculate network health score
        if active_nodes_count > 0:
            healthy_percentage = (health_distribution["healthy"] / active_nodes_count) * 100
            network_health_score = max(
                0,
                100
                - (health_distribution["critical"] * 10)
                - (health_distribution["poor"] * 5)
                - (health_distribution["degraded"] * 2),
            )
        else:
            healthy_percentage = 0
            network_health_score = 0

        return {
            "network_health_score": network_health_score,
            "active_nodes": active_nodes_count,
            "health_distribution": health_distribution,
            "network_metrics": {
                "avg_rssi": network_stats["avg_rssi"],
                "avg_snr": network_stats["avg_snr"],
                "total_packets": network_stats["total_packets"],
                "poor_signal_nodes": poor_signal_count,
                "isolated_nodes": isolated_count,
            },
            "analyzed_hours": hours,
            "timestamp": int(time.time()),
        }
