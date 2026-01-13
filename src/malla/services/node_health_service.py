"""Node health monitoring service for identifying problematic nodes in the mesh network.

This service analyzes various metrics to identify nodes that may be experiencing issues:
- Poor signal quality (low RSSI/SNR)
- High packet loss rates
- Frequent disconnections
- Battery drain issues
- Routing inefficiencies
"""

import logging
import time
from typing import Any

from ..database import get_db_connection

logger = logging.getLogger(__name__)

# In-memory cache for health scores (node_id -> {score, timestamp})
_health_cache: dict[tuple[int, int], dict[str, Any]] = {}
_HEALTH_CACHE_TTL = 300  # 5 minutes


class NodeHealthService:
    """Service for analyzing node health and identifying problematic nodes."""

    @staticmethod
    def _get_cached_health(node_id: int, hours: int) -> dict[str, Any] | None:
        """Get cached health data if available and not expired."""
        cache_key = (node_id, hours)
        if cache_key in _health_cache:
            cached = _health_cache[cache_key]
            if time.time() - cached["cached_at"] < _HEALTH_CACHE_TTL:
                logger.debug(f"Health cache hit for node {node_id}")
                return cached["data"]
        return None

    @staticmethod
    def _set_cached_health(node_id: int, hours: int, data: dict[str, Any]) -> None:
        """Store health data in cache."""
        cache_key = (node_id, hours)
        _health_cache[cache_key] = {"data": data, "cached_at": time.time()}
        # Clean old cache entries (simple cleanup)
        current_time = time.time()
        keys_to_remove = [
            k
            for k, v in _health_cache.items()
            if current_time - v["cached_at"] > _HEALTH_CACHE_TTL * 2
        ]
        for k in keys_to_remove:
            del _health_cache[k]

    @staticmethod
    def _calculate_baseline_behavior(node_id: int, cursor: Any) -> dict[str, Any]:
        """
        Calculate baseline behavior for a node over the last 30 days.

        Returns baseline metrics including average packets per day,
        typical activity pattern, and confidence in the baseline.
        """
        # Look back 30 days for baseline
        baseline_cutoff = int(time.time()) - (30 * 24 * 3600)
        current_time = int(time.time())

        # Get all packets in baseline period
        cursor.execute(
            """
            SELECT timestamp
            FROM packet_history
            WHERE from_node_id = ?
            AND timestamp >= ?
            ORDER BY timestamp
        """,
            (node_id, baseline_cutoff),
        )
        baseline_timestamps = [row["timestamp"] for row in cursor.fetchall()]

        if len(baseline_timestamps) < 5:  # Insufficient data
            return {
                "avg_packets_per_day": 0,
                "total_days": 0,
                "total_packets": 0,
                "confidence": 0,
                "has_baseline": False,
            }

        # Calculate metrics
        total_days = (current_time - baseline_cutoff) / 86400
        total_packets = len(baseline_timestamps)
        avg_packets_per_day = total_packets / total_days if total_days > 0 else 0

        # Calculate activity distribution (packets per day)
        from collections import defaultdict

        packets_by_day = defaultdict(int)
        for ts in baseline_timestamps:
            day_key = int(ts / 86400)  # Days since epoch
            packets_by_day[day_key] += 1

        # Calculate standard deviation of daily packet counts
        if len(packets_by_day) > 1:
            daily_counts = list(packets_by_day.values())
            mean_daily = sum(daily_counts) / len(daily_counts)
            variance = sum((x - mean_daily) ** 2 for x in daily_counts) / len(
                daily_counts
            )
            std_dev = variance**0.5
            coefficient_of_variation = (std_dev / mean_daily) if mean_daily > 0 else 0
        else:
            coefficient_of_variation = 0

        # Calculate confidence in baseline (0-100)
        # Factors: amount of data, consistency of behavior, time period
        confidence = 100

        # Reduce confidence if limited data points
        if total_packets < 10:
            confidence -= 40
        elif total_packets < 30:
            confidence -= 20
        elif total_packets < 100:
            confidence -= 10

        # Reduce confidence if limited time period
        if total_days < 7:
            confidence -= 30
        elif total_days < 14:
            confidence -= 15

        # Reduce confidence if behavior is highly variable (inconsistent)
        if coefficient_of_variation > 1.5:  # Very inconsistent
            confidence -= 20
        elif coefficient_of_variation > 1.0:  # Moderately inconsistent
            confidence -= 10

        confidence = max(0, confidence)

        return {
            "avg_packets_per_day": avg_packets_per_day,
            "total_days": total_days,
            "total_packets": total_packets,
            "days_with_activity": len(packets_by_day),
            "coefficient_of_variation": coefficient_of_variation,
            "confidence": confidence,
            "has_baseline": True,
        }

    @staticmethod
    def analyze_node_health(node_id: int, hours: int = 24) -> dict[str, Any] | None:
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
            # Node not in node_info, check if it exists in packet_history
            cursor.execute(
                """
                SELECT COUNT(*) as packet_count
                FROM packet_history
                WHERE from_node_id = ?
                AND timestamp >= ?
            """,
                (node_id, cutoff_time),
            )
            packet_check = cursor.fetchone()

            if not packet_check or packet_check["packet_count"] == 0:
                conn.close()
                return None

            # Create temporary node info for analysis
            node_info = {
                "node_id": node_id,
                "long_name": f"Node !{node_id:08x}",
                "short_name": f"!{node_id:08x}",
                "hw_model": None,
                "role": None,
            }
        else:
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

        # Get baseline behavior for this node (before closing connection)
        baseline = NodeHealthService._calculate_baseline_behavior(node_id, cursor)

        conn.close()

        # Analyze health issues
        issues = []
        health_score = 100  # Start with perfect score

        # Calculate confidence score for health assessment
        confidence_score = 100
        confidence_factors = []

        # Baseline confidence affects overall confidence
        if baseline["has_baseline"]:
            confidence_score = min(confidence_score, baseline["confidence"])
            if baseline["confidence"] < 70:
                confidence_factors.append(
                    f"Limited historical data (baseline confidence: {baseline['confidence']}%)"
                )
        else:
            confidence_score = 30  # Very low confidence without baseline
            confidence_factors.append(
                "No baseline data available (new node or insufficient history)"
            )

        # Reduce confidence if analysis period is too short
        if hours < 12:
            confidence_score -= 20
            confidence_factors.append(
                f"Short analysis period ({hours}h < 12h recommended)"
            )

        # Reduce confidence if very few packets in analysis period
        if packet_stats["total_packets"] > 0 and packet_stats["total_packets"] < 5:
            confidence_score -= 25
            confidence_factors.append(
                f"Limited data points ({packet_stats['total_packets']} packets)"
            )
        elif packet_stats["total_packets"] == 0:
            confidence_score -= 15  # Some confidence still remains from baseline
            confidence_factors.append("No packets in analysis period")

        confidence_score = max(0, min(100, confidence_score))

        # Check for behavioral anomalies (only if we have baseline)
        if baseline["has_baseline"] and baseline["avg_packets_per_day"] > 0:
            # Expected packets in the analysis period
            expected_packets = baseline["avg_packets_per_day"] * (hours / 24)
            actual_packets = packet_stats["total_packets"]

            # Calculate deviation from baseline
            if expected_packets > 0:
                deviation_ratio = actual_packets / expected_packets

                # Significant drop in activity (less than 50% of expected)
                if deviation_ratio < 0.5 and expected_packets >= 2:
                    severity = "warning" if deviation_ratio >= 0.25 else "critical"
                    issues.append(
                        {
                            "severity": severity,
                            "category": "behavior",
                            "message": f"Abnormally low activity: {actual_packets} packets vs {expected_packets:.1f} expected (baseline: {baseline['avg_packets_per_day']:.1f}/day)",
                            "expected": expected_packets,
                            "actual": actual_packets,
                            "deviation_percent": (1 - deviation_ratio) * 100,
                        }
                    )
                    if severity == "critical":
                        health_score -= 30
                    else:
                        health_score -= 15

                # Unusually high activity (more than 3x expected) - might indicate issues
                elif (
                    deviation_ratio > 3.0 and baseline["coefficient_of_variation"] < 1.0
                ):
                    # Only flag if baseline behavior is relatively consistent
                    issues.append(
                        {
                            "severity": "info",
                            "category": "behavior",
                            "message": f"Unusually high activity: {actual_packets} packets vs {expected_packets:.1f} expected (possible retransmission issues)",
                            "expected": expected_packets,
                            "actual": actual_packets,
                            "deviation_percent": (deviation_ratio - 1) * 100,
                        }
                    )

        # Note: RSSI and SNR are informational only and do NOT affect health score
        # These metrics are distance-dependent and would bias against remote nodes
        # Signal quality is tracked but not penalized to ensure fair health assessment
        if packet_stats["avg_rssi"] is not None and packet_stats["avg_rssi"] < -110:
            issues.append(
                {
                    "severity": "info",
                    "category": "signal",
                    "message": f"Average RSSI: {packet_stats['avg_rssi']:.1f} dBm (informational, distance-dependent)",
                }
            )

        if packet_stats["avg_snr"] is not None and packet_stats["avg_snr"] < -5:
            issues.append(
                {
                    "severity": "info",
                    "category": "signal",
                    "message": f"Average SNR: {packet_stats['avg_snr']:.1f} dB (informational, distance-dependent)",
                }
            )

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
        outage_details = []
        if long_gaps:
            total_gap_time = sum(g["duration_minutes"] for g in long_gaps)

            # Format detailed outage information
            for gap in long_gaps:
                from datetime import UTC, datetime

                start_time = datetime.fromtimestamp(gap["start"], tz=UTC)
                end_time = datetime.fromtimestamp(gap["end"], tz=UTC)
                duration_hours = gap["duration_minutes"] / 60

                outage_details.append(
                    {
                        "start_timestamp": gap["start"],
                        "end_timestamp": gap["end"],
                        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "duration_minutes": gap["duration_minutes"],
                        "duration_hours": duration_hours,
                        "duration_formatted": f"{int(duration_hours)}h {int(gap['duration_minutes'] % 60)}m"
                        if duration_hours >= 1
                        else f"{int(gap['duration_minutes'])}m",
                    }
                )

            issues.append(
                {
                    "severity": "warning",
                    "category": "reliability",
                    "message": f"{len(long_gaps)} significant outage(s) totaling {total_gap_time:.0f} minutes",
                    "outage_details": outage_details,
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

        result = {
            "node_id": node_id,
            "node_info": node_info,
            "health_score": health_score,
            "health_status": health_status,
            "confidence_score": confidence_score,
            "confidence_factors": confidence_factors,
            "baseline": baseline,
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

        # Cache the result
        NodeHealthService._set_cached_health(node_id, hours, result)
        return result

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

        # Analyze each node (each opens its own connection)
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

        # Get all active nodes for health distribution (before closing conn)
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

        # Analyze a LIMITED sample of nodes for health distribution (performance optimization)
        # Only analyze up to 100 nodes to avoid slow page loads
        sample_size = min(100, len(active_nodes))
        problematic_nodes = []
        for node_id in active_nodes[:sample_size]:
            health_data = NodeHealthService.analyze_node_health(node_id, hours)
            if health_data:
                problematic_nodes.append(health_data)

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
            network_health_score = max(
                0,
                100
                - (health_distribution["critical"] * 10)
                - (health_distribution["poor"] * 5)
                - (health_distribution["degraded"] * 2),
            )
        else:
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
