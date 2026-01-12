"""
Live Activity Monitor Service

Tracks and broadcasts real-time mesh network activity including:
- New packets received
- Node status changes (online/offline)
- Network events (new nodes, traceroutes, alerts)
- Signal quality changes

Uses Server-Sent Events (SSE) for real-time updates to connected clients.
"""

import json
import logging
import queue
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ActivityEvent:
    """Represents a real-time activity event in the mesh network."""

    event_type: str  # packet, node_online, node_offline, alert, traceroute
    timestamp: float
    data: dict[str, Any]
    severity: str = "info"  # info, warning, critical
    id: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert event to dictionary for JSON serialization."""
        return {
            "id": self.id or f"{self.event_type}_{int(self.timestamp * 1000)}",
            "type": self.event_type,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "severity": self.severity,
            "data": self.data,
        }

    def to_sse_message(self) -> str:
        """Convert event to Server-Sent Events format."""
        data_json = json.dumps(self.to_dict())
        return f"data: {data_json}\n\n"


class LiveActivityMonitor:
    """
    Manages real-time activity monitoring and event broadcasting.
    
    Thread-safe service that collects network events and broadcasts them
    to connected clients via Server-Sent Events.
    """

    def __init__(self, max_history: int = 1000):
        """
        Initialize live activity monitor.

        Args:
            max_history: Maximum number of events to keep in history
        """
        self.max_history = max_history
        self._event_queue: queue.Queue = queue.Queue(maxsize=10000)
        self._event_history: deque = deque(maxlen=max_history)
        self._history_lock = threading.Lock()
        
        # Statistics
        self._stats = {
            "total_events": 0,
            "events_by_type": {},
            "events_by_severity": {},
            "clients_connected": 0,
            "started_at": time.time(),
        }
        self._stats_lock = threading.Lock()

        # Track recent activity for rate calculations
        self._recent_packets = deque(maxlen=100)
        self._recent_lock = threading.Lock()

        logger.info("Live Activity Monitor initialized")

    def add_event(
        self,
        event_type: str,
        data: dict[str, Any],
        severity: str = "info",
        event_id: Optional[str] = None,
    ) -> None:
        """
        Add a new activity event.

        Args:
            event_type: Type of event (packet, node_online, etc.)
            data: Event data dictionary
            severity: Event severity (info, warning, critical)
            event_id: Optional unique event ID
        """
        event = ActivityEvent(
            event_type=event_type,
            timestamp=time.time(),
            data=data,
            severity=severity,
            id=event_id,
        )

        try:
            # Add to queue for broadcasting
            self._event_queue.put_nowait(event)

            # Add to history
            with self._history_lock:
                self._event_history.append(event)

            # Update statistics
            with self._stats_lock:
                self._stats["total_events"] += 1
                self._stats["events_by_type"][event_type] = (
                    self._stats["events_by_type"].get(event_type, 0) + 1
                )
                self._stats["events_by_severity"][severity] = (
                    self._stats["events_by_severity"].get(severity, 0) + 1
                )

            # Track packet rate
            if event_type == "packet":
                with self._recent_lock:
                    self._recent_packets.append(event.timestamp)

            logger.debug(f"Added {event_type} event: {event.id}")

        except queue.Full:
            logger.warning("Event queue full, dropping event")

    def add_packet_event(self, packet: dict[str, Any]) -> None:
        """
        Add a packet reception event.

        Args:
            packet: Packet data dictionary
        """
        # Determine severity based on packet type and signal quality
        severity = "info"
        if packet.get("portnum_name") == "TRACEROUTE_APP":
            severity = "info"
        elif packet.get("rssi", 0) < -120:
            severity = "warning"

        event_data = {
            "from_node": packet.get("from_node_id"),
            "from_name": packet.get("from_node_name", "Unknown"),
            "to_node": packet.get("to_node_id"),
            "to_name": packet.get("to_node_name", "Unknown"),
            "type": packet.get("portnum_name", "UNKNOWN"),
            "rssi": packet.get("rssi"),
            "snr": packet.get("snr"),
            "hop_limit": packet.get("hop_limit"),
            "gateway": packet.get("gateway_id"),
            "text": packet.get("decoded_text"),
            "channel": packet.get("channel_id"),
        }

        self.add_event("packet", event_data, severity)

    def add_node_status_event(
        self, node_id: int, node_name: str, status: str, details: Optional[dict] = None
    ) -> None:
        """
        Add a node status change event.

        Args:
            node_id: Node ID
            node_name: Node name
            status: Status (online, offline)
            details: Optional additional details
        """
        event_type = f"node_{status}"
        severity = "info" if status == "online" else "warning"

        event_data = {
            "node_id": node_id,
            "node_name": node_name,
            "status": status,
            **(details or {}),
        }

        self.add_event(event_type, event_data, severity)

    def add_alert_event(self, alert_type: str, message: str, details: dict) -> None:
        """
        Add an alert event.

        Args:
            alert_type: Type of alert (battery_low, signal_weak, etc.)
            message: Alert message
            details: Alert details
        """
        event_data = {
            "alert_type": alert_type,
            "message": message,
            **details,
        }

        severity = "critical" if "critical" in alert_type else "warning"
        self.add_event("alert", event_data, severity)

    def get_recent_events(self, limit: int = 100, event_type: Optional[str] = None) -> list[dict]:
        """
        Get recent events from history.

        Args:
            limit: Maximum number of events to return
            event_type: Optional filter by event type

        Returns:
            list[dict]: Recent events
        """
        with self._history_lock:
            events = list(self._event_history)

        # Filter by type if specified
        if event_type:
            events = [e for e in events if e.event_type == event_type]

        # Return most recent events
        events = events[-limit:]
        return [e.to_dict() for e in events]

    def get_event_stream(self):
        """
        Generator for Server-Sent Events stream.

        Yields events as they occur for real-time streaming to clients.
        """
        # Send keepalive comments every 15 seconds
        last_keepalive = time.time()

        try:
            with self._stats_lock:
                self._stats["clients_connected"] += 1

            logger.info("Client connected to live activity stream")

            while True:
                try:
                    # Try to get event with timeout
                    event = self._event_queue.get(timeout=1.0)
                    yield event.to_sse_message()

                except queue.Empty:
                    # Send keepalive if needed
                    if time.time() - last_keepalive > 15:
                        yield ": keepalive\n\n"
                        last_keepalive = time.time()

        except GeneratorExit:
            logger.info("Client disconnected from live activity stream")
        finally:
            with self._stats_lock:
                self._stats["clients_connected"] = max(
                    0, self._stats["clients_connected"] - 1
                )

    def get_activity_stats(self) -> dict:
        """
        Get activity statistics.

        Returns:
            dict: Activity statistics
        """
        with self._stats_lock:
            stats = self._stats.copy()

        # Calculate packet rate
        with self._recent_lock:
            if len(self._recent_packets) >= 2:
                time_span = self._recent_packets[-1] - self._recent_packets[0]
                if time_span > 0:
                    stats["packets_per_second"] = len(self._recent_packets) / time_span
                else:
                    stats["packets_per_second"] = 0
            else:
                stats["packets_per_second"] = 0

        # Calculate uptime
        stats["uptime_seconds"] = time.time() - stats["started_at"]

        return stats

    def get_activity_summary(self, time_window: int = 60) -> dict:
        """
        Get summary of activity in recent time window.

        Args:
            time_window: Time window in seconds

        Returns:
            dict: Activity summary
        """
        cutoff_time = time.time() - time_window

        with self._history_lock:
            recent_events = [e for e in self._event_history if e.timestamp >= cutoff_time]

        summary = {
            "time_window": time_window,
            "total_events": len(recent_events),
            "events_by_type": {},
            "events_by_severity": {},
            "unique_nodes": set(),
        }

        for event in recent_events:
            # Count by type
            summary["events_by_type"][event.event_type] = (
                summary["events_by_type"].get(event.event_type, 0) + 1
            )

            # Count by severity
            summary["events_by_severity"][event.severity] = (
                summary["events_by_severity"].get(event.severity, 0) + 1
            )

            # Track unique nodes
            if "node_id" in event.data:
                summary["unique_nodes"].add(event.data["node_id"])
            if "from_node" in event.data:
                summary["unique_nodes"].add(event.data["from_node"])

        summary["unique_nodes"] = len(summary["unique_nodes"])

        return summary


# Global monitor instance
_monitor: Optional[LiveActivityMonitor] = None
_monitor_lock = threading.Lock()


def get_live_monitor() -> LiveActivityMonitor:
    """
    Get or create the global live activity monitor.

    Returns:
        LiveActivityMonitor: Global monitor instance
    """
    global _monitor

    with _monitor_lock:
        if _monitor is None:
            _monitor = LiveActivityMonitor()

    return _monitor


def track_packet(packet: dict[str, Any]) -> None:
    """
    Track a packet in the live monitor.

    Args:
        packet: Packet data
    """
    monitor = get_live_monitor()
    monitor.add_packet_event(packet)


def track_node_status(node_id: int, node_name: str, status: str, details: Optional[dict] = None) -> None:
    """
    Track a node status change.

    Args:
        node_id: Node ID
        node_name: Node name
        status: Status (online, offline)
        details: Optional details
    """
    monitor = get_live_monitor()
    monitor.add_node_status_event(node_id, node_name, status, details)


def track_alert(alert_type: str, message: str, details: dict) -> None:
    """
    Track an alert.

    Args:
        alert_type: Alert type
        message: Alert message
        details: Alert details
    """
    monitor = get_live_monitor()
    monitor.add_alert_event(alert_type, message, details)
