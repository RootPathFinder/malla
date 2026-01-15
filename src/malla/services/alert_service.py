"""
Alert Service - Mesh Network Alert System

Provides:
- Alert generation from various health checks
- Alert history and management
- Configurable thresholds
- Anomaly detection for node behavior
"""

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from ..database.connection import get_db_connection
from ..utils.node_utils import get_bulk_node_names

logger = logging.getLogger(__name__)


def _execute_with_retry(func, max_retries: int = 3, initial_delay: float = 0.1):
    """Execute a database operation with exponential backoff retry logic.

    Args:
        func: Callable that performs the database operation
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds between retries

    Returns:
        Result from func

    Raises:
        sqlite3.OperationalError: If all retries fail
    """
    delay = initial_delay
    last_error: sqlite3.OperationalError | None = None

    for attempt in range(max_retries):
        try:
            return func()
        except sqlite3.OperationalError as e:
            if "database is locked" not in str(e).lower():
                raise
            last_error = e
            if attempt < max_retries - 1:
                logger.debug(
                    f"Database locked, retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(delay)
                delay *= 2  # Exponential backoff

    logger.error(f"Database operation failed after {max_retries} retries")
    if last_error:
        raise last_error
    raise sqlite3.OperationalError("Database operation failed after retries")


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of alerts the system can generate."""

    NODE_OFFLINE = "node_offline"
    NODE_BACK_ONLINE = "node_back_online"
    LOW_BATTERY = "low_battery"
    CRITICAL_BATTERY = "critical_battery"
    SIGNAL_DEGRADED = "signal_degraded"
    NEIGHBOR_LOST = "neighbor_lost"
    NEIGHBOR_GAINED = "neighbor_gained"
    ACTIVITY_ANOMALY = "activity_anomaly"
    GATEWAY_OFFLINE = "gateway_offline"
    HIGH_PACKET_LOSS = "high_packet_loss"


@dataclass
class Alert:
    """Represents a single alert."""

    alert_type: AlertType
    severity: AlertSeverity
    node_id: int | None
    title: str
    message: str
    timestamp: float = field(default_factory=time.time)
    resolved: bool = False
    resolved_at: float | None = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "node_id": self.node_id,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "resolved": self.resolved,
            "resolved_at": self.resolved_at,
            "resolved_at_iso": datetime.fromtimestamp(self.resolved_at).isoformat()
            if self.resolved_at
            else None,
            "metadata": self.metadata,
        }


# Node role classification for monitoring purposes
INFRASTRUCTURE_ROLES = {"ROUTER", "ROUTER_CLIENT", "ROUTER_LATE"}
CLIENT_ROLES = {"CLIENT", "CLIENT_MUTE", "CLIENT_BASE", "SENSOR", "TRACKER", None}

# Stale node threshold (2 weeks in seconds)
STALE_NODE_THRESHOLD_SECONDS = 14 * 24 * 60 * 60  # 2 weeks


@dataclass
class AlertThresholds:
    """Configurable thresholds for alert generation."""

    # Battery thresholds - 40% warning since batteries drop fast below that
    battery_warning_voltage: float = 3.4
    battery_critical_voltage: float = 3.2
    battery_warning_percent: int = 40
    battery_critical_percent: int = 20

    # Activity thresholds - differentiated by node role
    # Client nodes: 6 hours (360 minutes) - they may not transmit often
    node_offline_minutes: int = 360
    # Infrastructure nodes (routers/gateways): 2 hours (120 minutes) - must be always on
    infrastructure_offline_minutes: int = 120
    activity_anomaly_threshold: float = 0.3  # 30% of expected activity = anomaly

    # Signal thresholds
    rssi_warning: int = -110
    snr_warning: float = -5.0

    # Packet thresholds
    packet_loss_warning_percent: int = 20

    # Stale node threshold (days) - nodes inactive longer are archived
    stale_node_days: int = 14

    @classmethod
    def from_dict(cls, data: dict) -> "AlertThresholds":
        """Create thresholds from dictionary."""
        return cls(
            battery_warning_voltage=data.get("battery_warning_voltage", 3.4),
            battery_critical_voltage=data.get("battery_critical_voltage", 3.2),
            battery_warning_percent=data.get("battery_warning_percent", 40),
            battery_critical_percent=data.get("battery_critical_percent", 20),
            node_offline_minutes=data.get("node_offline_minutes", 360),
            infrastructure_offline_minutes=data.get(
                "infrastructure_offline_minutes", 120
            ),
            activity_anomaly_threshold=data.get("activity_anomaly_threshold", 0.3),
            rssi_warning=data.get("rssi_warning", -110),
            snr_warning=data.get("snr_warning", -5.0),
            packet_loss_warning_percent=data.get("packet_loss_warning_percent", 20),
            stale_node_days=data.get("stale_node_days", 14),
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "battery_warning_voltage": self.battery_warning_voltage,
            "battery_critical_voltage": self.battery_critical_voltage,
            "battery_warning_percent": self.battery_warning_percent,
            "battery_critical_percent": self.battery_critical_percent,
            "node_offline_minutes": self.node_offline_minutes,
            "infrastructure_offline_minutes": self.infrastructure_offline_minutes,
            "activity_anomaly_threshold": self.activity_anomaly_threshold,
            "rssi_warning": self.rssi_warning,
            "snr_warning": self.snr_warning,
            "packet_loss_warning_percent": self.packet_loss_warning_percent,
            "stale_node_days": self.stale_node_days,
        }


class AlertService:
    """Service for managing mesh network alerts."""

    # In-memory alert storage (in production, this would use the database)
    _alerts: list[Alert] = []
    _thresholds: AlertThresholds = AlertThresholds()
    _last_check: float = 0
    _CHECK_INTERVAL = 300  # 5 minutes between checks

    # Track node baselines for anomaly detection
    _node_baselines: dict[int, dict[str, Any]] = {}

    @classmethod
    def is_infrastructure_node(cls, role: str | None) -> bool:
        """
        Determine if a node is infrastructure (router/gateway) vs client.

        Infrastructure nodes have stricter monitoring requirements.
        """
        return role in INFRASTRUCTURE_ROLES

    @classmethod
    def get_node_category(cls, role: str | None) -> str:
        """
        Get human-readable category for a node role.

        Returns:
            'Mesh Infrastructure' for routers/gateways
            'Client' for other nodes
        """
        if role in INFRASTRUCTURE_ROLES:
            return "Mesh Infrastructure"
        return "Client"

    @classmethod
    def get_node_roles(cls) -> dict[int, str]:
        """
        Get role for all nodes from the database.

        Returns:
            Dictionary mapping node_id to role string
        """

        def _fetch_roles():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT node_id, role FROM node_info")
                return {row["node_id"]: row["role"] for row in cursor.fetchall()}
            finally:
                conn.close()

        try:
            return _execute_with_retry(_fetch_roles)
        except Exception as e:
            logger.warning(f"Failed to get node roles: {e}")
            return {}

    @classmethod
    def set_thresholds(cls, thresholds: AlertThresholds) -> None:
        """Update alert thresholds and save to database."""
        cls._thresholds = thresholds

        # Save to database
        def _save_thresholds():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                current_time = time.time()
                threshold_dict = thresholds.to_dict()

                # Save each threshold value
                for key, value in threshold_dict.items():
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO alert_thresholds (key, value, updated_at)
                        VALUES (?, ?, ?)
                    """,
                        (key, value, current_time),
                    )

                conn.commit()
                logger.info(f"Alert thresholds saved to database: {threshold_dict}")
            finally:
                conn.close()

        _execute_with_retry(_save_thresholds)

    @classmethod
    def get_thresholds(cls) -> AlertThresholds:
        """Get alert thresholds from database or return cached defaults."""

        # Try to load from database first
        def _load_thresholds():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT key, value FROM alert_thresholds")
                rows = cursor.fetchall()

                if rows:
                    threshold_dict = {row[0]: row[1] for row in rows}
                    cls._thresholds = AlertThresholds.from_dict(threshold_dict)
                    logger.debug(f"Loaded thresholds from database: {threshold_dict}")
                    return cls._thresholds

                return cls._thresholds
            finally:
                conn.close()

        try:
            return _execute_with_retry(_load_thresholds)
        except Exception as e:
            logger.warning(
                f"Failed to load thresholds from database: {e}, using defaults"
            )
            return cls._thresholds

    @classmethod
    def add_alert(cls, alert: Alert) -> None:
        """Add a new alert to the database with retry logic."""

        def _insert_alert():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                # Check for existing active alert of the same type and node
                cursor.execute(
                    """
                    SELECT id FROM alerts
                    WHERE alert_type = ? AND COALESCE(node_id, -1) = COALESCE(?, -1) AND resolved = 0
                    LIMIT 1
                """,
                    (alert.alert_type.value, alert.node_id),
                )

                existing = cursor.fetchone()

                if existing:
                    # Update existing alert instead of creating duplicate
                    cursor.execute(
                        """
                        UPDATE alerts
                        SET timestamp = ?, message = ?, metadata = ?
                        WHERE id = ?
                    """,
                        (
                            alert.timestamp,
                            alert.message,
                            json.dumps(alert.metadata),
                            existing["id"],
                        ),
                    )
                    logger.debug(f"Updated existing alert: {alert.title}")
                else:
                    # Insert new alert
                    cursor.execute(
                        """
                        INSERT INTO alerts
                        (alert_type, severity, node_id, title, message, timestamp, resolved, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                        (
                            alert.alert_type.value,
                            alert.severity.value,
                            alert.node_id,
                            alert.title,
                            alert.message,
                            alert.timestamp,
                            json.dumps(alert.metadata),
                        ),
                    )
                    logger.info(f"New alert: [{alert.severity.value}] {alert.title}")

                conn.commit()
            finally:
                conn.close()

        try:
            _execute_with_retry(_insert_alert, max_retries=3, initial_delay=0.05)
        except Exception as e:
            logger.error(f"Error adding alert after retries: {e}", exc_info=True)

    @classmethod
    def resolve_alert(cls, alert_type: AlertType, node_id: int | None) -> bool:
        """Resolve an active alert in the database with retry logic."""

        def _resolve():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    UPDATE alerts
                    SET resolved = 1, resolved_at = ?
                    WHERE alert_type = ? AND COALESCE(node_id, -1) = COALESCE(?, -1) AND resolved = 0
                """,
                    (time.time(), alert_type.value, node_id),
                )

                success = cursor.rowcount > 0
                conn.commit()

                if success:
                    logger.info(
                        f"Alert resolved: {alert_type.value} for node {node_id}"
                    )
                return success
            finally:
                conn.close()

        try:
            return _execute_with_retry(_resolve, max_retries=3, initial_delay=0.05)
        except Exception as e:
            logger.error(f"Error resolving alert after retries: {e}", exc_info=True)
            return False

    @classmethod
    def get_alerts(
        cls,
        include_resolved: bool = False,
        severity: AlertSeverity | None = None,
        node_id: int | None = None,
        alert_type: AlertType | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """
        Get alerts from database with optional filtering.

        Args:
            include_resolved: Include resolved alerts
            severity: Filter by severity
            node_id: Filter by node
            alert_type: Filter by type
            limit: Maximum alerts to return

        Returns:
            List of alert dictionaries
        """

        def _fetch_alerts():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                # Build query
                query = "SELECT * FROM alerts WHERE 1=1"
                params = []

                if not include_resolved:
                    query += " AND resolved = 0"

                if severity:
                    query += " AND severity = ?"
                    params.append(severity.value)

                if node_id is not None:
                    query += " AND node_id = ?"
                    params.append(node_id)

                if alert_type:
                    query += " AND alert_type = ?"
                    params.append(alert_type.value)

                # Sort by timestamp (newest first) and limit
                query += " ORDER BY timestamp DESC LIMIT ?"
                params.append(limit)

                cursor.execute(query, params)
                rows = cursor.fetchall()

                # Get node names for display
                node_ids = [
                    row["node_id"] for row in rows if row["node_id"] is not None
                ]
                node_names = get_bulk_node_names(node_ids) if node_ids else {}

                # Convert rows to dictionaries
                result = []
                for row in rows:
                    alert_dict = {
                        "alert_type": row["alert_type"],
                        "severity": row["severity"],
                        "node_id": row["node_id"],
                        "title": row["title"],
                        "message": row["message"],
                        "timestamp": row["timestamp"],
                        "timestamp_iso": datetime.fromtimestamp(
                            row["timestamp"]
                        ).isoformat(),
                        "resolved": bool(row["resolved"]),
                        "resolved_at": row["resolved_at"],
                        "resolved_at_iso": datetime.fromtimestamp(
                            row["resolved_at"]
                        ).isoformat()
                        if row["resolved_at"]
                        else None,
                        "metadata": json.loads(row["metadata"])
                        if row["metadata"]
                        else {},
                    }
                    if row["node_id"]:
                        alert_dict["node_name"] = node_names.get(
                            row["node_id"], f"!{row['node_id']:08x}"
                        )
                        alert_dict["node_hex"] = f"!{row['node_id']:08x}"
                    result.append(alert_dict)

                return result
            finally:
                conn.close()

        try:
            return _execute_with_retry(_fetch_alerts, max_retries=3, initial_delay=0.05)
        except Exception as e:
            logger.error(f"Error getting alerts after retries: {e}", exc_info=True)
            return []

    @classmethod
    def get_alert_summary(cls) -> dict[str, Any]:
        """Get summary of current alert state from database."""

        def _fetch_summary():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                # Get counts by severity for active alerts
                cursor.execute("""
                    SELECT severity, COUNT(*) as count
                    FROM alerts
                    WHERE resolved = 0
                    GROUP BY severity
                """)

                by_severity = {}
                for row in cursor.fetchall():
                    by_severity[row["severity"]] = row["count"]

                # Get total active alerts
                cursor.execute(
                    "SELECT COUNT(*) as count FROM alerts WHERE resolved = 0"
                )
                total_active = cursor.fetchone()["count"]

                # Get total resolved alerts
                cursor.execute(
                    "SELECT COUNT(*) as count FROM alerts WHERE resolved = 1"
                )
                total_resolved = cursor.fetchone()["count"]

                return {
                    "total_active": total_active,
                    "total_resolved": total_resolved,
                    "by_severity": by_severity,
                    "by_type": {},  # Could add if needed
                    "last_check": cls._last_check,
                }
            finally:
                conn.close()

        try:
            return _execute_with_retry(
                _fetch_summary, max_retries=3, initial_delay=0.05
            )
        except Exception as e:
            logger.error(
                f"Error getting alert summary after retries: {e}", exc_info=True
            )
            return {
                "total_active": 0,
                "total_resolved": 0,
                "by_severity": {},
                "by_type": {},
                "last_check": cls._last_check,
            }

    @classmethod
    def run_health_checks(cls, force: bool = False) -> dict[str, Any]:
        """
        Run all health checks and generate alerts.

        Args:
            force: Run even if within check interval

        Returns:
            Dictionary with check results
        """
        now = time.time()

        if not force and (now - cls._last_check) < cls._CHECK_INTERVAL:
            return {"skipped": True, "reason": "within_check_interval"}

        cls._last_check = now
        results = {
            "timestamp": now,
            "checks_run": [],
            "alerts_generated": 0,
            "alerts_resolved": 0,
        }

        try:
            # Run individual checks
            results["checks_run"].append("battery")
            battery_results = cls._check_battery_health()
            results["alerts_generated"] += battery_results.get("alerts", 0)

            results["checks_run"].append("node_activity")
            activity_results = cls._check_node_activity()
            results["alerts_generated"] += activity_results.get("alerts", 0)
            results["alerts_resolved"] += activity_results.get("resolved", 0)

            results["checks_run"].append("anomaly_detection")
            anomaly_results = cls._check_activity_anomalies()
            results["alerts_generated"] += anomaly_results.get("alerts", 0)

            logger.info(
                f"Health checks complete: {results['alerts_generated']} alerts generated, "
                f"{results['alerts_resolved']} resolved"
            )

        except Exception as e:
            logger.error(f"Error running health checks: {e}", exc_info=True)
            results["error"] = str(e)

        return results

    @classmethod
    def _check_battery_health(cls) -> dict[str, Any]:
        """Check battery status across all nodes."""
        alerts_generated = 0

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Get latest battery telemetry for each node
            cursor.execute(
                """
                SELECT
                    t.node_id,
                    t.battery_level,
                    t.voltage,
                    t.timestamp
                FROM telemetry_data t
                INNER JOIN (
                    SELECT node_id, MAX(timestamp) as max_ts
                    FROM telemetry_data
                    WHERE battery_level IS NOT NULL OR voltage IS NOT NULL
                    GROUP BY node_id
                ) latest ON t.node_id = latest.node_id AND t.timestamp = latest.max_ts
                WHERE t.timestamp > ?
            """,
                (time.time() - 86400,),
            )  # Last 24 hours

            rows = cursor.fetchall()
            conn.close()

            for row in rows:
                node_id = row["node_id"]
                battery_level = row["battery_level"]
                voltage = row["voltage"]

                # Check battery percentage
                if battery_level is not None:
                    if battery_level <= cls._thresholds.battery_critical_percent:
                        cls.add_alert(
                            Alert(
                                alert_type=AlertType.CRITICAL_BATTERY,
                                severity=AlertSeverity.CRITICAL,
                                node_id=node_id,
                                title=f"Critical Battery: {battery_level}%",
                                message=f"Node battery is critically low at {battery_level}%. Device may shut down soon.",
                                metadata={
                                    "battery_level": battery_level,
                                    "voltage": voltage,
                                },
                            )
                        )
                        alerts_generated += 1
                    elif battery_level <= cls._thresholds.battery_warning_percent:
                        cls.add_alert(
                            Alert(
                                alert_type=AlertType.LOW_BATTERY,
                                severity=AlertSeverity.WARNING,
                                node_id=node_id,
                                title=f"Low Battery: {battery_level}%",
                                message=f"Node battery is getting low at {battery_level}%.",
                                metadata={
                                    "battery_level": battery_level,
                                    "voltage": voltage,
                                },
                            )
                        )
                        alerts_generated += 1

                # Check voltage
                if voltage is not None and battery_level is None:
                    if voltage <= cls._thresholds.battery_critical_voltage:
                        cls.add_alert(
                            Alert(
                                alert_type=AlertType.CRITICAL_BATTERY,
                                severity=AlertSeverity.CRITICAL,
                                node_id=node_id,
                                title=f"Critical Voltage: {voltage:.2f}V",
                                message=f"Node voltage is critically low at {voltage:.2f}V. Device may shut down.",
                                metadata={"voltage": voltage},
                            )
                        )
                        alerts_generated += 1
                    elif voltage <= cls._thresholds.battery_warning_voltage:
                        cls.add_alert(
                            Alert(
                                alert_type=AlertType.LOW_BATTERY,
                                severity=AlertSeverity.WARNING,
                                node_id=node_id,
                                title=f"Low Voltage: {voltage:.2f}V",
                                message=f"Node voltage is getting low at {voltage:.2f}V.",
                                metadata={"voltage": voltage},
                            )
                        )
                        alerts_generated += 1

        except Exception as e:
            logger.error(f"Error checking battery health: {e}")

        return {"alerts": alerts_generated}

    @classmethod
    def _check_node_activity(cls) -> dict[str, Any]:
        """
        Check for nodes that have gone offline or come back online.

        Uses role-based thresholds:
        - Infrastructure nodes (routers/gateways): 2 hours
        - Client nodes: 6 hours
        """
        alerts_generated = 0
        alerts_resolved = 0

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Get node roles for role-based threshold checking
            node_roles = cls.get_node_roles()

            # Use client threshold (6 hours) as the base - we'll filter by role after
            client_offline_threshold = time.time() - (
                cls._thresholds.node_offline_minutes * 60
            )
            infra_offline_threshold = time.time() - (
                cls._thresholds.infrastructure_offline_minutes * 60
            )
            active_threshold = time.time() - 3600  # Active in last hour

            # Get all nodes with their last seen time (went inactive in last 24h)
            cursor.execute(
                """
                SELECT DISTINCT from_node_id, MAX(timestamp) as last_seen
                FROM packet_history
                WHERE from_node_id IS NOT NULL
                GROUP BY from_node_id
                HAVING last_seen < ? AND last_seen > ?
            """,
                (client_offline_threshold, client_offline_threshold - 86400),
            )

            potentially_offline_nodes = cursor.fetchall()

            for row in potentially_offline_nodes:
                node_id = row["from_node_id"]
                last_seen = row["last_seen"]
                hours_offline = (time.time() - last_seen) / 3600

                # Get node role and determine appropriate threshold
                role = node_roles.get(node_id)
                is_infrastructure = cls.is_infrastructure_node(role)
                node_category = cls.get_node_category(role)

                # Apply role-based threshold
                if is_infrastructure:
                    # Infrastructure: offline if not seen for 2+ hours
                    threshold = infra_offline_threshold
                    severity = (
                        AlertSeverity.CRITICAL
                    )  # Infrastructure offline is critical
                else:
                    # Client: offline if not seen for 6+ hours
                    threshold = client_offline_threshold
                    severity = AlertSeverity.WARNING

                # Only alert if the node is past its role-specific threshold
                if last_seen < threshold:
                    cls.add_alert(
                        Alert(
                            alert_type=AlertType.NODE_OFFLINE,
                            severity=severity,
                            node_id=node_id,
                            title=f"{node_category} Offline ({hours_offline:.1f}h)",
                            message=f"{node_category} node has not transmitted for {hours_offline:.1f} hours.",
                            metadata={
                                "last_seen": last_seen,
                                "hours_offline": hours_offline,
                                "node_role": role,
                                "node_category": node_category,
                                "is_infrastructure": is_infrastructure,
                            },
                        )
                    )
                    alerts_generated += 1

            # Check for nodes that have come back online
            cursor.execute(
                """
                SELECT DISTINCT from_node_id
                FROM packet_history
                WHERE from_node_id IS NOT NULL
                AND timestamp > ?
            """,
                (active_threshold,),
            )

            active_nodes = {row["from_node_id"] for row in cursor.fetchall()}

            # Resolve offline alerts for nodes that are now active
            # Get all unresolved NODE_OFFLINE alerts
            cursor.execute(
                """
                SELECT node_id FROM alerts
                WHERE alert_type = ? AND resolved = 0
            """,
                (AlertType.NODE_OFFLINE.value,),
            )

            offline_alerts = cursor.fetchall()

            for alert_row in offline_alerts:
                node_id = alert_row["node_id"]
                if node_id in active_nodes:
                    # Resolve the offline alert
                    if cls.resolve_alert(AlertType.NODE_OFFLINE, node_id):
                        # Add a "back online" alert
                        cls.add_alert(
                            Alert(
                                alert_type=AlertType.NODE_BACK_ONLINE,
                                severity=AlertSeverity.INFO,
                                node_id=node_id,
                                title="Node Back Online",
                                message="Node has resumed transmitting after being offline.",
                                metadata={},
                            )
                        )
                        alerts_resolved += 1

            conn.close()

        except Exception as e:
            logger.error(f"Error checking node activity: {e}")

        return {"alerts": alerts_generated, "resolved": alerts_resolved}

    @classmethod
    def _check_activity_anomalies(cls) -> dict[str, Any]:
        """
        Detect nodes with anomalous activity patterns.

        Compares recent activity to historical baseline.
        """
        alerts_generated = 0

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Get nodes with enough history for baseline
            one_week_ago = time.time() - (7 * 86400)
            one_day_ago = time.time() - 86400

            # Calculate baseline activity (packets per day over last 7 days)
            cursor.execute(
                """
                SELECT
                    from_node_id,
                    COUNT(*) as total_packets,
                    COUNT(DISTINCT DATE(timestamp, 'unixepoch')) as days_active
                FROM packet_history
                WHERE from_node_id IS NOT NULL
                AND timestamp >= ?
                AND timestamp < ?
                GROUP BY from_node_id
                HAVING total_packets >= 10
            """,
                (one_week_ago, one_day_ago),
            )

            baselines = {}
            for row in cursor.fetchall():
                node_id = row["from_node_id"]
                avg_per_day = row["total_packets"] / max(row["days_active"], 1)
                baselines[node_id] = {
                    "avg_per_day": avg_per_day,
                    "total_packets": row["total_packets"],
                    "days_active": row["days_active"],
                }

            # Get recent activity (last 24 hours)
            cursor.execute(
                """
                SELECT from_node_id, COUNT(*) as recent_packets
                FROM packet_history
                WHERE from_node_id IS NOT NULL
                AND timestamp >= ?
                GROUP BY from_node_id
            """,
                (one_day_ago,),
            )

            recent_activity = {
                row["from_node_id"]: row["recent_packets"] for row in cursor.fetchall()
            }
            conn.close()

            # Compare recent to baseline
            for node_id, baseline in baselines.items():
                expected = baseline["avg_per_day"]
                actual = recent_activity.get(node_id, 0)

                if expected > 0:
                    ratio = actual / expected

                    if ratio < cls._thresholds.activity_anomaly_threshold:
                        # Activity dropped significantly
                        cls.add_alert(
                            Alert(
                                alert_type=AlertType.ACTIVITY_ANOMALY,
                                severity=AlertSeverity.WARNING,
                                node_id=node_id,
                                title=f"Activity Anomaly: {int(ratio * 100)}% of normal",
                                message=f"Node activity is unusually low. Expected ~{expected:.0f} packets/day, "
                                f"but only saw {actual} in the last 24 hours.",
                                metadata={
                                    "expected_per_day": expected,
                                    "actual_24h": actual,
                                    "ratio": ratio,
                                },
                            )
                        )
                        alerts_generated += 1

            # Store baselines for future reference
            cls._node_baselines = baselines

        except Exception as e:
            logger.error(f"Error checking activity anomalies: {e}")

        return {"alerts": alerts_generated}

    @classmethod
    def get_activity_heatmap(
        cls, node_id: int | None = None, days: int = 7, use_utc: bool = False
    ) -> dict[str, Any]:
        """
        Get activity heatmap data (packets by hour of day).

        Args:
            node_id: Optional specific node (None for all)
            days: Number of days to analyze
            use_utc: If True, use UTC timestamps; if False, use localtime

        Returns:
            Heatmap data structure
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cutoff = time.time() - (days * 86400)

            # Choose timezone modifier based on preference
            tz_modifier = "" if use_utc else ", 'localtime'"

            if node_id:
                cursor.execute(
                    f"""
                    SELECT
                        strftime('%w', timestamp, 'unixepoch'{tz_modifier}) as day_of_week,
                        strftime('%H', timestamp, 'unixepoch'{tz_modifier}) as hour,
                        COUNT(*) as packet_count
                    FROM packet_history
                    WHERE from_node_id = ?
                    AND timestamp >= ?
                    GROUP BY day_of_week, hour
                    ORDER BY day_of_week, hour
                """,
                    (node_id, cutoff),
                )
            else:
                cursor.execute(
                    f"""
                    SELECT
                        strftime('%w', timestamp, 'unixepoch'{tz_modifier}) as day_of_week,
                        strftime('%H', timestamp, 'unixepoch'{tz_modifier}) as hour,
                        COUNT(*) as packet_count
                    FROM packet_history
                    WHERE timestamp >= ?
                    GROUP BY day_of_week, hour
                    ORDER BY day_of_week, hour
                """,
                    (cutoff,),
                )

            rows = cursor.fetchall()
            conn.close()

            # Build heatmap matrix (7 days x 24 hours)
            heatmap = [[0 for _ in range(24)] for _ in range(7)]
            max_value = 0

            for row in rows:
                day = int(row["day_of_week"])
                hour = int(row["hour"])
                count = row["packet_count"]
                heatmap[day][hour] = count
                max_value = max(max_value, count)

            # Day names
            day_names = [
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]

            return {
                "heatmap": heatmap,
                "day_names": day_names,
                "hours": list(range(24)),
                "max_value": max_value,
                "node_id": node_id,
                "days_analyzed": days,
                "timezone": "utc" if use_utc else "local",
            }

        except Exception as e:
            logger.error(f"Error generating activity heatmap: {e}", exc_info=True)
            raise

    @classmethod
    def get_trend_data(
        cls, metric: str = "packets", hours: int = 168, use_utc: bool = False
    ) -> dict[str, Any]:
        """
        Get time-series trend data for a metric.

        Args:
            metric: The metric to trend (packets, nodes, signal)
            hours: Number of hours to analyze
            use_utc: If True, format timestamps in UTC; if False, use local time

        Returns:
            Time-series data for charting
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cutoff = time.time() - (hours * 3600)

            # Determine bucket size based on time range
            if hours <= 24:
                bucket_seconds = 3600  # 1 hour buckets
                bucket_format = "%Y-%m-%d %H:00"
            elif hours <= 168:
                bucket_seconds = 3600 * 4  # 4 hour buckets
                bucket_format = "%Y-%m-%d %H:00"
            else:
                bucket_seconds = 86400  # Daily buckets
                bucket_format = "%Y-%m-%d"

            # Helper to format timestamp based on timezone preference
            def format_ts(ts):
                if use_utc:
                    return datetime.utcfromtimestamp(ts).strftime(bucket_format)
                else:
                    return datetime.fromtimestamp(ts).strftime(bucket_format)

            if metric == "packets":
                cursor.execute(
                    f"""
                    SELECT
                        (CAST(timestamp AS INTEGER) / {bucket_seconds}) * {bucket_seconds} as bucket,
                        COUNT(*) as value,
                        SUM(CASE WHEN processed_successfully = 1 THEN 1 ELSE 0 END) as successful
                    FROM packet_history
                    WHERE timestamp >= ?
                    GROUP BY bucket
                    ORDER BY bucket
                """,
                    (cutoff,),
                )

                data = []
                for row in cursor.fetchall():
                    data.append(
                        {
                            "timestamp": row["bucket"],
                            "timestamp_str": format_ts(row["bucket"]),
                            "total": row["value"],
                            "successful": row["successful"],
                            "success_rate": round(
                                row["successful"] / row["value"] * 100, 1
                            )
                            if row["value"] > 0
                            else 0,
                        }
                    )

            elif metric == "nodes":
                cursor.execute(
                    f"""
                    SELECT
                        (CAST(timestamp AS INTEGER) / {bucket_seconds}) * {bucket_seconds} as bucket,
                        COUNT(DISTINCT from_node_id) as active_nodes
                    FROM packet_history
                    WHERE timestamp >= ?
                    AND from_node_id IS NOT NULL
                    GROUP BY bucket
                    ORDER BY bucket
                """,
                    (cutoff,),
                )

                data = []
                for row in cursor.fetchall():
                    data.append(
                        {
                            "timestamp": row["bucket"],
                            "timestamp_str": format_ts(row["bucket"]),
                            "active_nodes": row["active_nodes"],
                        }
                    )

            elif metric == "signal":
                cursor.execute(
                    f"""
                    SELECT
                        (CAST(timestamp AS INTEGER) / {bucket_seconds}) * {bucket_seconds} as bucket,
                        AVG(rssi) as avg_rssi,
                        AVG(snr) as avg_snr,
                        MIN(rssi) as min_rssi,
                        MAX(rssi) as max_rssi
                    FROM packet_history
                    WHERE timestamp >= ?
                    AND rssi IS NOT NULL
                    GROUP BY bucket
                    ORDER BY bucket
                """,
                    (cutoff,),
                )

                data = []
                for row in cursor.fetchall():
                    data.append(
                        {
                            "timestamp": row["bucket"],
                            "timestamp_str": format_ts(row["bucket"]),
                            "avg_rssi": round(row["avg_rssi"], 1)
                            if row["avg_rssi"]
                            else None,
                            "avg_snr": round(row["avg_snr"], 1)
                            if row["avg_snr"]
                            else None,
                            "min_rssi": row["min_rssi"],
                            "max_rssi": row["max_rssi"],
                        }
                    )

            else:
                data = []

            conn.close()

            return {
                "metric": metric,
                "hours": hours,
                "bucket_seconds": bucket_seconds,
                "data": data,
                "generated_at": time.time(),
                "timezone": "utc" if use_utc else "local",
            }

        except Exception as e:
            logger.error(f"Error getting trend data: {e}", exc_info=True)
            raise

    @classmethod
    def _ensure_archived_column(cls) -> None:
        """Ensure the archived column exists in node_info table."""

        def _add_column():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                # Add archived column if it doesn't exist
                cursor.execute(
                    """
                    ALTER TABLE node_info ADD COLUMN archived INTEGER DEFAULT 0
                """
                )
                conn.commit()
                logger.info("Added 'archived' column to node_info table")
            except Exception as e:
                if "duplicate column name" in str(e).lower():
                    pass  # Column already exists
                else:
                    logger.debug(f"Could not add archived column: {e}")
            finally:
                conn.close()

        try:
            _execute_with_retry(_add_column)
        except Exception:
            pass  # Ignore if column already exists

    @classmethod
    def get_stale_nodes(cls, days: int | None = None) -> list[dict]:
        """
        Get nodes that haven't transmitted within the threshold.

        Args:
            days: Override threshold days (default from settings)

        Returns:
            List of stale node dictionaries
        """
        threshold_days = days or cls._thresholds.stale_node_days
        threshold_seconds = threshold_days * 24 * 60 * 60
        cutoff = time.time() - threshold_seconds

        def _fetch_stale():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT
                        n.node_id,
                        COALESCE(n.long_name, n.short_name, printf('!%08x', n.node_id)) as name,
                        n.role,
                        n.hex_id,
                        n.last_updated,
                        MAX(p.timestamp) as last_packet
                    FROM node_info n
                    LEFT JOIN packet_history p ON p.from_node_id = n.node_id
                    WHERE COALESCE(n.archived, 0) = 0
                    GROUP BY n.node_id
                    HAVING last_packet < ? OR last_packet IS NULL
                    ORDER BY last_packet DESC NULLS LAST
                """,
                    (cutoff,),
                )

                nodes = []
                for row in cursor.fetchall():
                    last_seen = row["last_packet"] or row["last_updated"]
                    days_stale = (
                        (time.time() - last_seen) / 86400 if last_seen else None
                    )
                    nodes.append(
                        {
                            "node_id": row["node_id"],
                            "name": row["name"],
                            "hex_id": row["hex_id"],
                            "role": row["role"],
                            "category": cls.get_node_category(row["role"]),
                            "last_seen": last_seen,
                            "days_stale": round(days_stale, 1) if days_stale else None,
                        }
                    )

                return nodes
            finally:
                conn.close()

        try:
            cls._ensure_archived_column()
            return _execute_with_retry(_fetch_stale)
        except Exception as e:
            logger.error(f"Error getting stale nodes: {e}")
            return []

    @classmethod
    def archive_node(cls, node_id: int) -> bool:
        """
        Archive a stale node (hide from active views but preserve data).

        Args:
            node_id: The node to archive

        Returns:
            True if successful
        """

        def _archive():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE node_info SET archived = 1 WHERE node_id = ?
                """,
                    (node_id,),
                )
                conn.commit()
                success = cursor.rowcount > 0
                if success:
                    logger.info(f"Archived node {node_id}")
                return success
            finally:
                conn.close()

        try:
            cls._ensure_archived_column()
            return _execute_with_retry(_archive)
        except Exception as e:
            logger.error(f"Error archiving node: {e}")
            return False

    @classmethod
    def unarchive_node(cls, node_id: int) -> bool:
        """
        Restore an archived node to active status.

        Args:
            node_id: The node to unarchive

        Returns:
            True if successful
        """

        def _unarchive():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE node_info SET archived = 0 WHERE node_id = ?
                """,
                    (node_id,),
                )
                conn.commit()
                success = cursor.rowcount > 0
                if success:
                    logger.info(f"Unarchived node {node_id}")
                return success
            finally:
                conn.close()

        try:
            cls._ensure_archived_column()
            return _execute_with_retry(_unarchive)
        except Exception as e:
            logger.error(f"Error unarchiving node: {e}")
            return False

    @classmethod
    def archive_stale_nodes(cls, days: int | None = None) -> dict:
        """
        Archive all nodes that haven't transmitted within threshold.

        Args:
            days: Override threshold days (default: 14 days)

        Returns:
            Summary of archived nodes
        """
        stale_nodes = cls.get_stale_nodes(days)
        archived = []
        failed = []

        for node in stale_nodes:
            if cls.archive_node(node["node_id"]):
                archived.append(node)
            else:
                failed.append(node)

        logger.info(f"Archived {len(archived)} stale nodes, {len(failed)} failures")

        return {
            "archived_count": len(archived),
            "failed_count": len(failed),
            "archived_nodes": archived,
            "threshold_days": days or cls._thresholds.stale_node_days,
        }

    @classmethod
    def get_archived_nodes(cls) -> list[dict]:
        """
        Get all archived nodes.

        Returns:
            List of archived node dictionaries
        """

        def _fetch_archived():
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT
                        n.node_id,
                        COALESCE(n.long_name, n.short_name, printf('!%08x', n.node_id)) as name,
                        n.role,
                        n.hex_id,
                        n.last_updated,
                        MAX(p.timestamp) as last_packet
                    FROM node_info n
                    LEFT JOIN packet_history p ON p.from_node_id = n.node_id
                    WHERE n.archived = 1
                    GROUP BY n.node_id
                    ORDER BY last_packet DESC NULLS LAST
                """
                )

                nodes = []
                for row in cursor.fetchall():
                    last_seen = row["last_packet"] or row["last_updated"]
                    days_stale = (
                        (time.time() - last_seen) / 86400 if last_seen else None
                    )
                    nodes.append(
                        {
                            "node_id": row["node_id"],
                            "name": row["name"],
                            "hex_id": row["hex_id"],
                            "role": row["role"],
                            "category": cls.get_node_category(row["role"]),
                            "last_seen": last_seen,
                            "days_stale": round(days_stale, 1) if days_stale else None,
                        }
                    )

                return nodes
            finally:
                conn.close()

        try:
            cls._ensure_archived_column()
            return _execute_with_retry(_fetch_archived)
        except Exception as e:
            logger.error(f"Error getting archived nodes: {e}")
            return []
