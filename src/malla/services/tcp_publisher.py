"""
TCP Publisher service for sending Meshtastic admin commands.

This module provides functionality to publish admin commands to a Meshtastic node
connected via TCP (network connection).
"""

import logging
import random
import threading
import time
from typing import Any

from meshtastic import admin_pb2, portnums_pb2
from meshtastic.tcp_interface import TCPInterface
from pubsub import pub

from ..config import get_config
from ..utils.telemetry_request import (
    apply_telemetry_request_type,
    complete_pending_telemetry,
    extract_from_node_id,
    extract_portnum,
    extract_request_id,
    extract_telemetry_raw_payload,
    find_matching_telemetry_request,
    is_routing_no_response,
    telemetry_has_requested_metrics,
    telemetry_to_dict,
)

logger = logging.getLogger(__name__)


# PKI/Session key related error codes from meshtastic.mesh_pb2.Routing.Error
# These indicate key synchronization issues that may be recoverable
class PKIErrorCodes:
    """Error codes related to PKI key synchronization issues."""

    NONE = "NONE"
    NOT_AUTHORIZED = "NOT_AUTHORIZED"  # 33 - General authorization failure
    PKI_FAILED = "PKI_FAILED"  # 34 - General PKI failure
    PKI_UNKNOWN_PUBKEY = "PKI_UNKNOWN_PUBKEY"  # 35 - Remote node doesn't have our key
    ADMIN_BAD_SESSION_KEY = "ADMIN_BAD_SESSION_KEY"  # 36 - Session passkey is stale
    ADMIN_PUBLIC_KEY_UNAUTHORIZED = (
        "ADMIN_PUBLIC_KEY_UNAUTHORIZED"  # 37 - Key not authorized for admin
    )

    # Error codes that indicate a stale session passkey that can be cleared and retried
    RECOVERABLE_SESSION_ERRORS = {
        "ADMIN_BAD_SESSION_KEY",
        "PKI_FAILED",
    }

    # Max send attempts when auto-renewing the admin session passkey
    # (1 initial attempt + up to 2 renew-and-retry cycles).
    MAX_SESSION_RECOVERY_ATTEMPTS = 3

    # Error codes that indicate the node doesn't have our public key configured
    # These require manual configuration on the remote node
    REQUIRES_KEY_CONFIGURATION = {
        "PKI_UNKNOWN_PUBKEY",
        "ADMIN_PUBLIC_KEY_UNAUTHORIZED",
        "NOT_AUTHORIZED",
    }

    @classmethod
    def normalize_error_reason(cls, error_reason: Any) -> str:
        """Normalize routing error reasons to canonical string names."""
        if error_reason is None or error_reason == "":
            return "NONE"
        if isinstance(error_reason, bytes):
            try:
                error_reason = error_reason.decode("utf-8")
            except Exception:
                error_reason = str(error_reason)
        if isinstance(error_reason, int):
            try:
                from meshtastic import mesh_pb2

                values_by_number = mesh_pb2.Routing.Error.items()
                for name, number in values_by_number:
                    if number == error_reason:
                        return str(name)
                return str(error_reason)
            except Exception:
                return str(error_reason)
        text = str(error_reason).strip()
        # Handle "Routing.Error.ADMIN_BAD_SESSION_KEY" / "Error.ADMIN_BAD_SESSION_KEY"
        if "." in text:
            text = text.rsplit(".", 1)[-1]
        return text

    @classmethod
    def is_recoverable_session_error(cls, error_reason: Any) -> bool:
        """Check if an error indicates a stale session that can be recovered."""
        return (
            cls.normalize_error_reason(error_reason) in cls.RECOVERABLE_SESSION_ERRORS
        )

    @classmethod
    def is_key_configuration_error(cls, error_reason: Any) -> bool:
        """Check if an error requires manual key configuration on remote node."""
        return (
            cls.normalize_error_reason(error_reason) in cls.REQUIRES_KEY_CONFIGURATION
        )

    @classmethod
    def is_pki_related(cls, error_reason: Any) -> bool:
        """Check if an error is related to PKI/key synchronization."""
        return cls.is_recoverable_session_error(
            error_reason
        ) or cls.is_key_configuration_error(error_reason)


def convert_to_dict(obj: Any) -> Any:
    """
    Recursively convert protobuf messages and other objects to JSON-serializable dicts.

    Args:
        obj: Any object that might be a protobuf, dict, list, or primitive

    Returns:
        JSON-serializable version of the object
    """
    # Handle protobuf messages
    if hasattr(obj, "DESCRIPTOR"):
        from google.protobuf.json_format import MessageToDict

        return MessageToDict(obj, preserving_proto_field_name=True)

    # Handle dicts - recursively convert values
    if isinstance(obj, dict):
        return {k: convert_to_dict(v) for k, v in obj.items()}

    # Handle lists - recursively convert items
    if isinstance(obj, list):
        return [convert_to_dict(item) for item in obj]

    # Handle bytes
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            import base64

            return base64.b64encode(obj).decode("ascii")

    # Return primitives as-is
    return obj


class TCPPublisher:
    """
    TCP client for sending Meshtastic admin commands.

    This class handles:
    - Connection to a Meshtastic node via TCP
    - Sending admin commands
    - Receiving responses
    """

    _instance: "TCPPublisher | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "TCPPublisher":
        """Singleton pattern to ensure only one TCP publisher exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the TCP publisher."""
        if self._initialized:
            return

        self._initialized = True
        self._config = get_config()
        self._interface: TCPInterface | None = None
        self._connected = False
        self._connect_lock = threading.Lock()

        # Track pending responses
        self._pending_responses: dict[int, dict[str, Any]] = {}
        self._response_lock = threading.Lock()
        self._response_events: dict[int, threading.Event] = {}

        # General admin response tracking
        self._last_admin_response: dict[str, Any] | None = None
        self._admin_response_event = threading.Event()

        # Pending telemetry requests tracking
        # Key: target_node_id (int), Value: dict with event and response data
        self._pending_telemetry_requests: dict[int, dict[str, Any]] = {}
        self._pending_telemetry_lock = threading.Lock()
        # Late replies that arrived after a wait was cleaned up (by request id)
        self._telemetry_late_by_request: dict[int, dict[str, Any]] = {}
        # Most recent good telemetry per node (for grace / last-known)
        self._telemetry_latest_by_node: dict[int, dict[str, Any]] = {}

        # Telemetry request statistics tracking
        self._telemetry_stats: dict[str, Any] = {
            "total_requests": 0,
            "successful_responses": 0,
            "timeouts": 0,
            "errors": 0,
            "last_request_time": None,
            "last_success_time": None,
            "per_node_stats": {},  # Key: node_id, Value: {requests, successes, timeouts}
        }
        self._telemetry_stats_lock = threading.Lock()

        # Session passkey storage per target node
        # Key: node_id (int), Value: session_passkey (bytes)
        # Session passkeys are returned by nodes in admin responses and must
        # be included in subsequent admin messages for authentication
        self._session_passkeys: dict[int, bytes] = {}
        self._session_passkey_lock = threading.Lock()

        # Connection health tracking
        self._last_activity_time: float = 0
        self._last_health_check_time: float = 0
        self._health_check_timeout: float = 30.0  # seconds

        # Keepalive thread
        self._keepalive_thread: threading.Thread | None = None
        self._keepalive_stop_event = threading.Event()
        self._keepalive_interval: float = 30.0  # Send heartbeat every 30 seconds
        self._missed_heartbeats: int = 0
        self._max_missed_heartbeats: int = 3  # Disconnect after 3 missed heartbeats

    @property
    def is_connected(self) -> bool:
        """Check if connected to the node."""
        if not self._connected or self._interface is None:
            return False

        # Also check the interface's internal connection state
        try:
            # The meshtastic library tracks connection state internally
            if hasattr(self._interface, "_connected"):
                interface_connected = getattr(self._interface, "_connected", None)
                if interface_connected is not None and hasattr(
                    interface_connected, "is_set"
                ):
                    # It's a threading.Event
                    return interface_connected.is_set()  # type: ignore[union-attr]
                return bool(interface_connected)
        except Exception:
            pass

        return self._connected

    def check_connection_health(self, send_heartbeat: bool = False) -> dict[str, Any]:
        """
        Perform a health check on the TCP connection.

        This verifies the connection is actually alive by checking the interface
        state and optionally sending a heartbeat to verify communication.

        Args:
            send_heartbeat: If True, send a heartbeat packet to verify the
                           connection is truly alive. This is more thorough but
                           takes slightly longer.

        Returns:
            Dictionary with health status info
        """
        if not self._connected or self._interface is None:
            return {
                "healthy": False,
                "connected": False,
                "reason": "Not connected",
            }

        try:
            # Check 1: Interface's internal connection state
            interface_connected = True
            if hasattr(self._interface, "_connected"):
                conn_state = getattr(self._interface, "_connected", None)
                if conn_state is not None and hasattr(conn_state, "is_set"):
                    interface_connected = conn_state.is_set()  # type: ignore[union-attr]
                else:
                    interface_connected = bool(conn_state)

            if not interface_connected:
                self._connected = False
                return {
                    "healthy": False,
                    "connected": False,
                    "reason": "TCP socket disconnected",
                    "suggestion": "The connection was lost. Click 'Reconnect' to restore it.",
                }

            # Check 2: Verify we have node info (indicates successful initial handshake)
            my_info = self._interface.myInfo
            local_node = self._interface.localNode

            if my_info is None and local_node is None:
                return {
                    "healthy": False,
                    "connected": True,
                    "reason": "Connection stale - no node info available",
                    "suggestion": "Try disconnecting and reconnecting",
                }

            # Check 3: Optionally send a heartbeat to verify the connection is truly alive
            if send_heartbeat:
                try:
                    self._interface.sendHeartbeat()
                    logger.debug("Heartbeat sent successfully")
                except Exception as e:
                    logger.warning(f"Heartbeat failed: {e}")
                    return {
                        "healthy": False,
                        "connected": True,
                        "reason": f"Heartbeat failed: {str(e)}",
                        "suggestion": "Connection may be stale - try reconnecting",
                    }

            # Update last activity time
            self._last_health_check_time = time.time()

            # Calculate time since last activity
            time_since_activity = (
                time.time() - self._last_activity_time
                if self._last_activity_time > 0
                else None
            )

            return {
                "healthy": True,
                "connected": True,
                "node_id": getattr(my_info, "my_node_num", None) if my_info else None,
                "last_activity": self._last_activity_time,
                "last_health_check": self._last_health_check_time,
                "keepalive_active": (
                    self._keepalive_thread is not None
                    and self._keepalive_thread.is_alive()
                ),
                "keepalive_interval": self._keepalive_interval,
                "missed_heartbeats": self._missed_heartbeats,
                "seconds_since_activity": time_since_activity,
            }

        except Exception as e:
            logger.warning(f"Connection health check failed: {e}")
            return {
                "healthy": False,
                "connected": True,
                "reason": f"Health check failed: {str(e)}",
                "suggestion": "Connection may be stale - try reconnecting",
            }

    def ensure_healthy_connection(self) -> bool:
        """
        Ensure the TCP connection is healthy before performing operations.

        This performs a thorough health check and attempts to reconnect if needed.

        Returns:
            True if connection is healthy, False otherwise
        """
        # First, quick check
        if not self.is_connected:
            logger.warning("TCP connection not established, attempting to connect...")
            if not self.connect():
                return False

        # Thorough health check with heartbeat
        health = self.check_connection_health(send_heartbeat=True)

        if not health.get("healthy", False):
            logger.warning(f"TCP connection unhealthy: {health.get('reason')}")

            # Attempt automatic reconnection
            logger.info("Attempting automatic reconnection...")
            if self.reconnect():
                # Verify the new connection
                health = self.check_connection_health(send_heartbeat=True)
                if health.get("healthy", False):
                    logger.info("Reconnection successful")
                    return True

            logger.error("Failed to restore TCP connection")
            return False

        return True

    def _start_keepalive(self) -> None:
        """Start the keepalive background thread."""
        if self._keepalive_thread is not None and self._keepalive_thread.is_alive():
            return  # Already running

        self._keepalive_stop_event.clear()
        self._missed_heartbeats = 0
        self._keepalive_thread = threading.Thread(
            target=self._keepalive_loop,
            name="TCPKeepalive",
            daemon=True,
        )
        self._keepalive_thread.start()
        logger.info(f"TCP keepalive started (interval: {self._keepalive_interval}s)")

    def _stop_keepalive(self) -> None:
        """Stop the keepalive background thread."""
        if self._keepalive_thread is None:
            return

        self._keepalive_stop_event.set()
        self._keepalive_thread.join(timeout=2.0)
        self._keepalive_thread = None
        logger.debug("TCP keepalive stopped")

    def _keepalive_loop(self) -> None:
        """Background loop that sends periodic heartbeats to keep connection alive."""
        logger.debug("Keepalive loop started")

        while not self._keepalive_stop_event.is_set():
            # Wait for the interval
            if self._keepalive_stop_event.wait(timeout=self._keepalive_interval):
                break  # Stop event was set

            # Skip if not connected
            if not self._connected or self._interface is None:
                continue

            try:
                # Send heartbeat to verify connection is alive
                self._interface.sendHeartbeat()
                self._missed_heartbeats = 0
                self._last_activity_time = time.time()
                logger.debug("TCP keepalive heartbeat sent successfully")

            except Exception as e:
                self._missed_heartbeats += 1
                logger.warning(
                    f"TCP keepalive heartbeat failed ({self._missed_heartbeats}/"
                    f"{self._max_missed_heartbeats}): {e}"
                )

                if self._missed_heartbeats >= self._max_missed_heartbeats:
                    logger.error(
                        f"TCP connection appears dead after {self._missed_heartbeats} "
                        "missed heartbeats, attempting auto-reconnect..."
                    )
                    self._connected = False

                    # Attempt automatic reconnection in a separate thread
                    # to avoid blocking the keepalive loop
                    self._attempt_auto_reconnect()

        logger.debug("Keepalive loop exited")

    def _attempt_auto_reconnect(self) -> None:
        """
        Attempt automatic reconnection after connection loss.

        This runs in the keepalive thread and uses timeout-protected operations.
        """
        max_attempts = 3
        retry_delay = 5.0

        for attempt in range(1, max_attempts + 1):
            if self._keepalive_stop_event.is_set():
                logger.info("Auto-reconnect cancelled - stop event set")
                return

            logger.info(f"Auto-reconnect attempt {attempt}/{max_attempts}...")

            # Force cleanup of dead connection (with timeout)
            self._disconnect_with_timeout(timeout=5.0)

            time.sleep(1.0)  # Brief pause before reconnecting

            try:
                if self.connect():
                    logger.info("Auto-reconnect successful!")
                    self._missed_heartbeats = 0
                    return
            except Exception as e:
                logger.warning(f"Auto-reconnect attempt {attempt} failed: {e}")

            if attempt < max_attempts:
                logger.info(f"Waiting {retry_delay}s before next reconnect attempt...")
                # Use the stop event for interruptible sleep
                if self._keepalive_stop_event.wait(timeout=retry_delay):
                    logger.info("Auto-reconnect cancelled during wait")
                    return

        logger.error(
            f"Auto-reconnect failed after {max_attempts} attempts. "
            "Manual reconnection required."
        )

    def reconnect(self) -> bool:
        """
        Force reconnection to the TCP node.

        This disconnects and reconnects, useful for recovering from stale connections.

        Returns:
            True if reconnection successful
        """
        logger.info("Forcing TCP reconnection...")

        # Disconnect with timeout to avoid hanging on dead connections
        self._disconnect_with_timeout(timeout=5.0)

        time.sleep(1.0)  # Brief pause before reconnecting

        success = self.connect()
        if success:
            logger.info("TCP reconnection successful")
        else:
            logger.error("TCP reconnection failed")
        return success

    def _disconnect_with_timeout(self, timeout: float = 5.0) -> None:
        """
        Disconnect with a timeout to avoid hanging on dead connections.

        Args:
            timeout: Maximum seconds to wait for graceful disconnect
        """
        import threading

        disconnect_done = threading.Event()

        def do_disconnect():
            try:
                self.disconnect()
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                disconnect_done.set()

        disconnect_thread = threading.Thread(target=do_disconnect, daemon=True)
        disconnect_thread.start()

        if not disconnect_done.wait(timeout=timeout):
            logger.warning(f"Disconnect timed out after {timeout}s, forcing cleanup...")
            # Force cleanup without waiting for close()
            self._stop_keepalive()
            try:
                pub.unsubscribe(self._on_receive, "meshtastic.receive")
            except Exception:
                pass
            try:
                pub.unsubscribe(self._on_receive, "meshtastic.receive.admin")
            except Exception:
                pass
            self._interface = None
            self._connected = False
            logger.info("Forced disconnect completed")

    @property
    def tcp_host(self) -> str:
        """Get the configured TCP host."""
        if hasattr(self, "_override_host") and self._override_host:
            return self._override_host
        return getattr(self._config, "admin_tcp_host", "192.168.1.1")

    @property
    def tcp_port(self) -> int:
        """Get the configured TCP port."""
        if hasattr(self, "_override_port") and self._override_port:
            return self._override_port
        return getattr(self._config, "admin_tcp_port", 4403)

    def set_connection_params(
        self, host: str | None = None, port: int | None = None
    ) -> None:
        """
        Set TCP connection parameters.

        Args:
            host: IP address or hostname of the node
            port: TCP port number
        """
        # Disconnect if currently connected with different params
        if self._connected:
            current_host = self.tcp_host
            current_port = self.tcp_port
            if (host and host != current_host) or (port and port != current_port):
                self.disconnect()

        if host:
            self._override_host = host
            logger.info(f"TCP host set to: {host}")
        if port:
            self._override_port = port
            logger.info(f"TCP port set to: {port}")

    def connect(self) -> bool:
        """
        Connect to the Meshtastic node via TCP.

        Returns:
            True if connection successful or already connected
        """
        with self._connect_lock:
            if self._connected and self._interface is not None:
                return True

            try:
                logger.info(
                    f"Connecting to Meshtastic node at {self.tcp_host}:{self.tcp_port}"
                )

                self._interface = TCPInterface(
                    hostname=self.tcp_host,
                    portNumber=self.tcp_port,
                    noProto=False,
                )

                # Set up callback for received packets using pubsub
                pub.subscribe(self._on_receive, "meshtastic.receive")

                self._connected = True
                self._last_activity_time = time.time()
                logger.info(
                    f"Connected to Meshtastic node at {self.tcp_host}:{self.tcp_port}"
                )

                # Get node info
                if self._interface.localNode:
                    node_id = self._interface.localNode.nodeNum
                    logger.info(f"Local node ID: {node_id} (!{node_id:08x})")
                    logger.info(
                        "This node will be used to send admin commands to other nodes"
                    )

                    # Also subscribe to all received packets for debugging
                    pub.subscribe(self._on_receive, "meshtastic.receive.admin")

                # Start keepalive thread to detect stale connections
                self._start_keepalive()

                return True

            except Exception as e:
                logger.error(f"Failed to connect to Meshtastic node: {e}")
                self._interface = None
                self._connected = False
                return False

    def disconnect(self) -> None:
        """Disconnect from the Meshtastic node."""
        logger.info("Disconnecting from Meshtastic node...")

        # Stop keepalive thread first (outside of lock to avoid deadlock)
        self._stop_keepalive()

        with self._connect_lock:
            if self._interface is not None:
                try:
                    # Unsubscribe from pubsub
                    try:
                        pub.unsubscribe(self._on_receive, "meshtastic.receive")
                    except Exception:
                        pass  # May not be subscribed
                    try:
                        pub.unsubscribe(self._on_receive, "meshtastic.receive.admin")
                    except Exception:
                        pass  # May not be subscribed

                    logger.debug("Closing TCP interface...")
                    self._interface.close()
                    logger.debug("TCP interface closed")
                except Exception as e:
                    logger.warning(f"Error closing TCP connection: {e}")
                finally:
                    self._interface = None
                    self._connected = False
                    logger.info("Disconnected from Meshtastic node")
            else:
                logger.debug("Already disconnected")

    def _on_receive(self, packet: dict[str, Any], interface: Any = None) -> None:
        """Handle received packets from pubsub."""
        try:
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")
            from_id = packet.get("from") or packet.get("fromId")
            to_id = packet.get("to") or packet.get("toId")
            channel = packet.get("channel", 0)

            # Log all received packets for debugging
            logger.debug(
                f"TCP received packet: portnum={portnum}, from={from_id}, "
                f"to={to_id}, channel={channel}"
            )

            # Check for encrypted packets that couldn't be decoded
            if not decoded or (not portnum and packet.get("encrypted")):
                logger.warning(
                    f"Received encrypted packet that couldn't be decoded: "
                    f"from={from_id}, to={to_id}, channel={channel}"
                )

            # Handle routing packets (ACK/NAK responses)
            if portnum == "ROUTING_APP":
                routing = decoded.get("routing", {})
                error_reason = PKIErrorCodes.normalize_error_reason(
                    routing.get("errorReason", "NONE")
                )
                # Meshtastic puts requestId on decoded; some paths also copy it top-level
                request_id = self._normalize_request_id(
                    packet.get("requestId") or decoded.get("requestId")
                )

                if request_id is not None:
                    logger.info(
                        f"Received routing response for request {request_id}: {error_reason}"
                    )

                    response_data = {
                        "packet": packet,
                        "received_at": time.time(),
                        "from_node": packet.get("fromId") or packet.get("from"),
                        "routing": routing,
                        "error_reason": error_reason,
                        "is_ack": error_reason == "NONE",
                        "is_nak": error_reason != "NONE",
                        "decoded": decoded,
                    }

                    with self._response_lock:
                        if request_id in self._pending_responses:
                            self._pending_responses[request_id] = response_data
                            if request_id in self._response_events:
                                self._response_events[request_id].set()

                    # Also update general response tracking
                    self._last_admin_response = response_data
                    self._admin_response_event.set()

            elif portnum == "TELEMETRY_APP":
                self._match_and_complete_telemetry(packet)

            elif portnum == "ADMIN_APP":
                from_node = packet.get("fromId") or packet.get("from")
                logger.info(
                    f"Received admin response from {from_node} (type: {type(from_node).__name__})"
                )

                # Parse the admin message from the payload
                admin_message = None
                session_passkey = None
                payload = decoded.get("payload")

                if payload:
                    try:
                        if isinstance(payload, bytes):
                            # Raw bytes - parse as protobuf
                            admin_message = admin_pb2.AdminMessage()
                            admin_message.ParseFromString(payload)
                            logger.debug(
                                f"Parsed admin message from bytes: {admin_message}"
                            )
                            # Get session_passkey from protobuf
                            if hasattr(admin_message, "session_passkey"):
                                session_passkey = admin_message.session_passkey
                        else:
                            # payload might already be decoded by meshtastic lib as dict
                            admin_message = decoded.get("admin")
                            logger.debug(
                                f"Got pre-decoded admin message: {type(admin_message)}"
                            )
                    except Exception as e:
                        logger.warning(f"Failed to parse admin message payload: {e}")
                        admin_message = decoded.get("admin")

                # Also check for session_passkey in the decoded dict directly
                # The meshtastic library might put it there
                if session_passkey is None:
                    # Check in admin dict if it's a dict
                    if (
                        isinstance(admin_message, dict)
                        and "session_passkey" in admin_message
                    ):
                        session_passkey = admin_message["session_passkey"]
                        logger.debug(
                            f"Got session_passkey from admin dict: {type(session_passkey)}"
                        )
                    # Also check in decoded directly
                    elif "session_passkey" in decoded:
                        session_passkey = decoded["session_passkey"]
                        logger.debug(
                            f"Got session_passkey from decoded: {type(session_passkey)}"
                        )
                    # Check in admin sub-dict of decoded
                    elif (
                        isinstance(decoded.get("admin"), dict)
                        and "session_passkey" in decoded["admin"]
                    ):
                        session_passkey = decoded["admin"]["session_passkey"]
                        logger.debug(
                            f"Got session_passkey from decoded.admin: {type(session_passkey)}"
                        )

                # Store session_passkey if we found one
                if session_passkey:
                    # Handle if session_passkey is a string (base64 or hex)
                    if isinstance(session_passkey, str):
                        passkey_str = session_passkey
                        try:
                            import base64

                            session_passkey = base64.b64decode(passkey_str)
                            logger.debug(
                                f"Decoded session_passkey from base64: {len(session_passkey)} bytes"
                            )
                        except Exception:
                            try:
                                session_passkey = bytes.fromhex(passkey_str)
                                logger.debug(
                                    f"Decoded session_passkey from hex: {len(session_passkey)} bytes"
                                )
                            except Exception:
                                session_passkey = passkey_str.encode()

                    if isinstance(session_passkey, bytes) and len(session_passkey) > 0:
                        # Convert from_node to int if it's a hex string
                        node_id: int | None = None
                        if isinstance(from_node, int):
                            node_id = from_node
                        elif isinstance(from_node, str):
                            if from_node.startswith("!"):
                                node_id = int(from_node[1:], 16)
                            else:
                                try:
                                    node_id = int(from_node, 16)
                                except ValueError:
                                    node_id = int(from_node)

                        if node_id is not None:
                            with self._session_passkey_lock:
                                self._session_passkeys[node_id] = session_passkey
                            logger.info(
                                f"Stored session_passkey for node !{node_id:08x} "
                                f"({len(session_passkey)} bytes)"
                            )

                response_data = {
                    "packet": packet,
                    "received_at": time.time(),
                    "from_node": from_node,
                    "admin_message": admin_message,
                    "decoded": decoded,
                    "is_ack": True,  # Admin response implies ACK
                    "is_nak": False,
                }

                # Signal any waiting requests
                # The meshtastic library uses 'requestId' for response correlation
                request_id = packet.get("requestId") or decoded.get("requestId")
                if request_id:
                    with self._response_lock:
                        # Store the response for any matching pending request
                        if request_id in self._pending_responses:
                            self._pending_responses[request_id] = response_data
                            if request_id in self._response_events:
                                self._response_events[request_id].set()

                # Store last admin response for general use
                self._last_admin_response = response_data
                self._admin_response_event.set()

        except Exception as e:
            logger.error(f"Error processing received packet: {e}")

    @staticmethod
    def _normalize_request_id(request_id: Any) -> int | None:
        """Normalize packet/request ids to unsigned 32-bit ints for dict keys."""
        if request_id is None or request_id == "":
            return None
        try:
            return int(request_id) & 0xFFFFFFFF
        except (TypeError, ValueError):
            return None

    def _store_telemetry_caches(
        self,
        *,
        from_node_id: int | None,
        request_id: int | None,
        telemetry_dict: dict[str, Any],
        telemetry_type: str = "device_metrics",
    ) -> None:
        """Cache late/unmatched replies for grace-period recovery."""
        if not telemetry_has_requested_metrics(telemetry_dict, telemetry_type):
            # Still cache if any metrics variant is present
            if not any(
                telemetry_has_requested_metrics(telemetry_dict, t)
                for t in (
                    "device_metrics",
                    "environment_metrics",
                    "power_metrics",
                    "local_stats",
                )
            ):
                return
        payload = {
            "telemetry": telemetry_dict,
            "timestamp": time.time(),
            "from_node": from_node_id,
            "request_id": request_id,
        }
        if request_id is not None:
            self._telemetry_late_by_request[request_id] = payload
            # Bound cache size
            if len(self._telemetry_late_by_request) > 64:
                oldest = next(iter(self._telemetry_late_by_request))
                self._telemetry_late_by_request.pop(oldest, None)
        if from_node_id is not None:
            self._telemetry_latest_by_node[from_node_id] = payload

    def _match_and_complete_telemetry(self, packet: dict[str, Any]) -> bool:
        """
        Correlate a TELEMETRY_APP packet with a pending live request.

        Used by both pubsub ``_on_receive`` and the meshtastic ``onResponse``
        callback so either delivery path can complete the wait.
        """
        portnum = extract_portnum(packet)
        from_node_id = extract_from_node_id(packet)
        request_id = extract_request_id(packet)
        decoded = packet.get("decoded") or {}

        # ROUTING callbacks (wantAck / NO_RESPONSE) must not complete as success
        if portnum == "ROUTING_APP" or decoded.get("routing") is not None:
            if is_routing_no_response(packet) and request_id is not None:
                with self._pending_telemetry_lock:
                    for pending in self._pending_telemetry_requests.values():
                        pending_rid = self._normalize_request_id(
                            pending.get("request_id")
                        )
                        if pending_rid != request_id or pending.get("completed"):
                            continue
                        pending.setdefault("response_data", {})["error"] = "NO_RESPONSE"
                        event = pending.get("event")
                        if event is not None:
                            event.set()
                        logger.warning(
                            "Telemetry NO_RESPONSE for request_id=%s "
                            "(node may lack telemetry reply support)",
                            request_id,
                        )
                        return True
            return False

        if portnum is not None and portnum != "TELEMETRY_APP":
            return False

        telemetry_dict = telemetry_to_dict(decoded.get("telemetry", {}))
        raw_payload = extract_telemetry_raw_payload(packet)

        logger.info(
            "Received TELEMETRY_APP packet from=%s node_id=%s request_id=%s "
            "metrics=%s",
            packet.get("fromId") or packet.get("from"),
            f"!{from_node_id:08x}" if from_node_id is not None else None,
            request_id,
            sorted(k for k in telemetry_dict.keys() if k != "time"),
        )

        with self._pending_telemetry_lock:
            pending_keys = list(self._pending_telemetry_requests.keys())
            match = find_matching_telemetry_request(
                self._pending_telemetry_requests,
                from_node_id=from_node_id,
                request_id=request_id,
                telemetry=telemetry_dict,
            )
            if match is None:
                # Keep for grace-period pickup after a premature timeout
                self._store_telemetry_caches(
                    from_node_id=from_node_id,
                    request_id=request_id,
                    telemetry_dict=telemetry_dict,
                )
                if pending_keys:
                    logger.debug(
                        "TELEMETRY_APP cached (no pending match); pending=%s "
                        "from=%s request_id=%s",
                        pending_keys,
                        from_node_id,
                        request_id,
                    )
                return False

            node_id, pending = match
            completed = complete_pending_telemetry(
                pending,
                telemetry=telemetry_dict,
                from_node_id=from_node_id if from_node_id is not None else node_id,
                request_id=request_id,
                raw_payload=raw_payload,
            )
            self._store_telemetry_caches(
                from_node_id=from_node_id if from_node_id is not None else node_id,
                request_id=request_id or pending.get("request_id"),
                telemetry_dict=telemetry_dict,
                telemetry_type=pending.get("telemetry_type") or "device_metrics",
            )
            # Keep raw bytes on the late/latest caches for DB persistence
            if raw_payload is not None:
                cache_node = from_node_id if from_node_id is not None else node_id
                if request_id is not None and request_id in self._telemetry_late_by_request:
                    self._telemetry_late_by_request[request_id]["raw_payload"] = (
                        raw_payload
                    )
                if cache_node is not None and cache_node in self._telemetry_latest_by_node:
                    self._telemetry_latest_by_node[cache_node]["raw_payload"] = (
                        raw_payload
                    )
            if completed:
                logger.info(
                    "Telemetry response matched pending request for !%08x "
                    "(request_id=%s)",
                    node_id,
                    request_id or pending.get("request_id"),
                )
            return completed

    def get_latest_node_telemetry(
        self, node_id: int, max_age_s: float = 30.0
    ) -> dict[str, Any] | None:
        """Return cached telemetry for a node if fresher than max_age_s."""
        with self._pending_telemetry_lock:
            cached = self._telemetry_latest_by_node.get(node_id)
            if not cached:
                return None
            age = time.time() - float(cached.get("timestamp") or 0)
            if age > max_age_s:
                return None
            return dict(cached)

    def get_response(
        self, packet_id: int, timeout: float = 30.0
    ) -> dict[str, Any] | None:
        """
        Wait for a response to a sent packet.

        Args:
            packet_id: The packet ID to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            Response data or None if timeout
        """
        # Clear any previous general response so we don't pick up a stale one
        self._admin_response_event.clear()
        self._last_admin_response = None

        event = threading.Event()

        with self._response_lock:
            # If ACK/response already arrived between send and wait, return it
            existing = self._pending_responses.get(packet_id)
            if existing:
                self._pending_responses.pop(packet_id, None)
                self._response_events.pop(packet_id, None)
                logger.info(f"Got buffered response for packet {packet_id}")
                return existing

            self._response_events[packet_id] = event
            self._pending_responses[packet_id] = {}  # Mark as pending

        logger.debug(f"Waiting for response to packet {packet_id}, timeout={timeout}s")

        # Wait for either specific packet response or general admin response
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check for specific packet ID match
            if event.wait(timeout=0.5):
                with self._response_lock:
                    if packet_id in self._pending_responses:
                        response = self._pending_responses.pop(packet_id)
                        if response:  # Has actual response data (not just empty dict)
                            self._response_events.pop(packet_id, None)
                            logger.info(f"Got specific response for packet {packet_id}")
                            return response

            # Check for general admin response
            if self._admin_response_event.is_set() and self._last_admin_response:
                response = self._last_admin_response
                self._last_admin_response = None
                self._admin_response_event.clear()
                logger.info(f"Got admin response from {response.get('from_node')}")
                # Cleanup
                with self._response_lock:
                    self._pending_responses.pop(packet_id, None)
                    self._response_events.pop(packet_id, None)
                return response

        # Cleanup on timeout
        logger.debug(f"Timeout waiting for response to packet {packet_id}")
        with self._response_lock:
            self._pending_responses.pop(packet_id, None)
            self._response_events.pop(packet_id, None)

        return None

    def get_local_node_id(self) -> int | None:
        """Get the local node ID from the connected interface."""
        if self._interface and self._interface.localNode:
            return self._interface.localNode.nodeNum
        return None

    def get_local_node_name(self) -> str | None:
        """Get the local node's long name from the connected interface."""
        if self._interface and self._interface.localNode:
            node_num = self._interface.localNode.nodeNum
            if node_num and self._interface.nodes:
                node_hex = f"!{node_num:08x}"
                node_info = self._interface.nodes.get(node_hex, {}) or {}
                if "user" in node_info:
                    return node_info["user"].get("longName")
        return None

    def get_session_passkey_nodes(self) -> list[str]:
        """
        Get a list of node IDs that have stored session passkeys.

        Returns:
            List of node IDs in hex format (e.g., ["!3ac468fa", "!4f4cf20b"])
        """
        with self._session_passkey_lock:
            return [f"!{node_id:08x}" for node_id in self._session_passkeys.keys()]

    def clear_session_passkey(self, node_id: int | str | None = None) -> int:
        """
        Clear stored session passkeys.

        Use this when a node's private key has changed and the old session
        passkey is no longer valid. After clearing, the next GET request
        to that node will fetch a fresh session passkey.

        Args:
            node_id: The node ID to clear the passkey for.
                     Can be an integer (e.g., 987654321) or
                     a hex string (e.g., "!3ac468fa" or "3ac468fa").
                     If None, clears ALL session passkeys.

        Returns:
            Number of session passkeys cleared
        """
        with self._session_passkey_lock:
            if node_id is None:
                # Clear all session passkeys
                count = len(self._session_passkeys)
                self._session_passkeys.clear()
                if count > 0:
                    logger.info(f"Cleared all {count} session passkeys")
                return count

            # Convert string node_id to int if needed
            if isinstance(node_id, str):
                node_id_str = node_id.lstrip("!")
                try:
                    node_id = int(node_id_str, 16)
                except ValueError:
                    logger.warning(f"Invalid node_id format: {node_id}")
                    return 0

            # Clear specific node's session passkey
            if node_id in self._session_passkeys:
                del self._session_passkeys[node_id]
                logger.info(f"Cleared session passkey for node !{node_id:08x}")
                return 1
            else:
                logger.debug(f"No session passkey found for node !{node_id:08x}")
                return 0

    def has_session_passkey(self, node_id: int) -> bool:
        """Return True if a session passkey is cached for the node."""
        with self._session_passkey_lock:
            return node_id in self._session_passkeys

    def refresh_session_passkey(
        self,
        target_node_id: int,
        timeout: float = 30.0,
    ) -> bool:
        """
        Fetch a fresh session passkey from a remote node.

        Remote admin *writes* require a session_passkey previously returned by
        any get_* AdminMessage response. When the key is missing or stale
        (ADMIN_BAD_SESSION_KEY), clients must GET again, store the new key,
        then retry the write.

        Args:
            target_node_id: Node to request a passkey from
            timeout: Seconds to wait for the metadata response

        Returns:
            True if a passkey is stored for the node after the request
        """
        self.clear_session_passkey(target_node_id)

        logger.info(
            f"Refreshing session passkey for !{target_node_id:08x} "
            f"via get_device_metadata"
        )
        packet_id = self.send_get_device_metadata(target_node_id)
        if packet_id is None:
            logger.warning(
                f"Failed to send get_device_metadata while refreshing "
                f"session passkey for !{target_node_id:08x}"
            )
            return False

        response = self.get_response(packet_id, timeout=timeout)
        if not response:
            logger.warning(
                f"Timeout refreshing session passkey for !{target_node_id:08x}"
            )
            return False

        if response.get("is_nak"):
            logger.warning(
                f"NAK refreshing session passkey for !{target_node_id:08x}: "
                f"{response.get('error_reason', 'NAK')}"
            )
            return False

        if self.has_session_passkey(target_node_id):
            logger.info(
                f"Refreshed session passkey for !{target_node_id:08x}"
            )
            return True

        logger.warning(
            f"get_device_metadata succeeded for !{target_node_id:08x} but "
            f"no session_passkey was present in the response"
        )
        return False

    def execute_with_session_recovery(
        self,
        target_node_id: int,
        send_fn: Any,
        timeout: float = 30.0,
        max_attempts: int | None = None,
        refresh_timeout: float | None = None,
        auto_recover: bool = True,
    ) -> dict[str, Any]:
        """
        Send an admin command and await a response, renewing the session passkey
        on ADMIN_BAD_SESSION_KEY / PKI_FAILED.

        Args:
            target_node_id: Destination node
            send_fn: Zero-arg callable that sends the command and returns packet_id
            timeout: Seconds to wait for each command response
            max_attempts: Max send attempts including the first (default 3)
            refresh_timeout: Seconds to wait while refreshing the passkey
            auto_recover: When False, do not renew/retry on session errors

        Returns:
            Dict with success, response, error, error_reason, recovered,
            needs_key_config, packet_id, attempts
        """
        if max_attempts is None:
            max_attempts = PKIErrorCodes.MAX_SESSION_RECOVERY_ATTEMPTS
        if refresh_timeout is None:
            refresh_timeout = timeout

        result: dict[str, Any] = {
            "success": False,
            "response": None,
            "error": None,
            "error_reason": None,
            "recovered": False,
            "needs_key_config": False,
            "packet_id": None,
            "attempts": 0,
        }

        for attempt in range(1, max_attempts + 1):
            result["attempts"] = attempt
            packet_id = send_fn()
            if packet_id is None:
                result["error"] = "Failed to send admin message"
                return result

            result["packet_id"] = packet_id
            response = self.get_response(packet_id, timeout=timeout)

            if not response:
                result["error"] = (
                    f"No response received (timeout after {timeout}s). "
                    "Node may be offline or unreachable."
                )
                return result

            error_reason = PKIErrorCodes.normalize_error_reason(
                response.get("error_reason", "NONE")
            )
            is_nak = bool(response.get("is_nak"))
            # Trust the canonical reason even if is_nak was computed from a
            # non-normalized value upstream.
            if error_reason != "NONE":
                is_nak = True
            response["error_reason"] = error_reason
            response["is_nak"] = is_nak
            response["is_ack"] = not is_nak

            if error_reason == "NONE" and not is_nak:
                result["success"] = True
                result["response"] = response
                if attempt > 1:
                    result["recovered"] = True
                return result

            result["error_reason"] = error_reason
            result["response"] = response

            if PKIErrorCodes.is_key_configuration_error(error_reason):
                result["needs_key_config"] = True
                result["error"] = (
                    f"Node !{target_node_id:08x} does not have this server's "
                    f"public key configured ({error_reason}). "
                    "Configure the server's public key on the remote node."
                )
                return result

            if (
                auto_recover
                and PKIErrorCodes.is_recoverable_session_error(error_reason)
                and attempt < max_attempts
            ):
                logger.warning(
                    f"PKI session error '{error_reason}' for node "
                    f"!{target_node_id:08x} (attempt {attempt}/{max_attempts}), "
                    f"refreshing session passkey and retrying..."
                )
                if not self.refresh_session_passkey(
                    target_node_id, timeout=refresh_timeout
                ):
                    result["error"] = (
                        f"PKI key sync error: {error_reason}. "
                        "Failed to refresh session passkey from the node."
                    )
                    return result
                continue

            if PKIErrorCodes.is_recoverable_session_error(error_reason):
                result["error"] = (
                    f"PKI key sync error: {error_reason} "
                    f"after {attempt} attempt(s). "
                    "Try clearing all session keys."
                )
                return result

            result["error"] = f"Routing error: {error_reason}"
            return result

        return result

    def send_admin_with_recovery(
        self,
        target_node_id: int,
        admin_message: admin_pb2.AdminMessage,
        timeout: float = 30.0,
        auto_recover: bool = True,
        max_attempts: int | None = None,
    ) -> dict[str, Any]:
        """
        Send an admin message and handle key synchronization errors automatically.

        This method detects PKI/session key errors (like ADMIN_BAD_SESSION_KEY,
        PKI_FAILED) and attempts recovery by fetching a fresh session passkey
        (via get_device_metadata) and retrying up to max_attempts times.

        Args:
            target_node_id: The destination node ID
            admin_message: The admin message protobuf
            timeout: Maximum time to wait for response in seconds
            auto_recover: Whether to attempt automatic recovery on key errors
            max_attempts: Max send attempts including the first (default 3)

        Returns:
            Dict with keys:
                - success: bool indicating if request succeeded
                - response: The response data if successful, None otherwise
                - error: Error message if failed, None otherwise
                - error_reason: Specific routing error code if available
                - recovered: bool indicating if recovery was needed and succeeded
                - needs_key_config: bool indicating remote node needs key config
                - packet_id / attempts: tracking metadata
        """

        def send_fn() -> int | None:
            # Drop any stale key baked into a reused message; send_admin_message
            # will re-attach the current cached passkey if one is available.
            admin_message.ClearField("session_passkey")
            return self.send_admin_message(
                target_node_id=target_node_id,
                admin_message=admin_message,
                want_response=True,
            )

        return self.execute_with_session_recovery(
            target_node_id=target_node_id,
            send_fn=send_fn,
            timeout=timeout,
            max_attempts=max_attempts,
            auto_recover=auto_recover,
        )

    def _get_admin_channel_index(self) -> int:
        """
        Get the admin channel index from the local node.

        Returns the channel index of a channel named "admin" (case-insensitive),
        or 0 if no admin channel is found.

        Returns:
            The admin channel index
        """
        if self._interface and self._interface.localNode:
            channels = self._interface.localNode.channels
            if channels:
                for channel in channels:
                    if channel.settings and channel.settings.name.lower() == "admin":
                        return channel.index
        return 0

    def send_admin_message(
        self,
        target_node_id: int,
        admin_message: admin_pb2.AdminMessage,
        want_response: bool = True,
        verify_connection: bool = True,
    ) -> int | None:
        """
        Send an admin message to a target node.

        Args:
            target_node_id: The destination node ID
            admin_message: The admin message protobuf
            want_response: Whether to wait for a response
            verify_connection: Whether to verify connection health before sending

        Returns:
            Packet ID if sent successfully, None otherwise
        """
        # Verify connection is healthy before attempting to send
        if verify_connection:
            if not self.ensure_healthy_connection():
                logger.error(
                    "Cannot send admin message: connection unhealthy and reconnection failed"
                )
                return None
        elif not self.connect():
            logger.error("Cannot send admin message: not connected to node")
            return None

        if self._interface is None:
            return None

        try:
            # Get admin channel index
            admin_channel_index = self._get_admin_channel_index()

            # Generate a random packet ID for tracking only if sendData does not
            # return one (it normally assigns meshPacket.id for ACK correlation)
            packet_id = random.getrandbits(32)

            # Set session_passkey on the admin message if we have one for this target.
            # Session passkeys are required for remote admin *writes*. Always clear a
            # stale key left on a reused message when we no longer have a cached one.
            with self._session_passkey_lock:
                if target_node_id in self._session_passkeys:
                    session_passkey = self._session_passkeys[target_node_id]
                    admin_message.session_passkey = session_passkey
                    logger.info(
                        f"Including session_passkey for !{target_node_id:08x} "
                        f"({len(session_passkey)} bytes)"
                    )
                else:
                    if admin_message.session_passkey:
                        admin_message.ClearField("session_passkey")
                    logger.info(
                        f"No session_passkey found for !{target_node_id:08x} "
                        f"(known nodes: {[f'!{n:08x}' for n in self._session_passkeys.keys()]})"
                    )

            # Send the packet using sendData
            # pkiEncrypted=True is required for admin messages to work on remote nodes
            # wantAck=True requests a routing ACK/NAK from the destination
            mesh_packet = self._interface.sendData(
                data=admin_message,
                destinationId=target_node_id,
                portNum=portnums_pb2.PortNum.ADMIN_APP,
                wantAck=True,
                wantResponse=want_response,
                channelIndex=admin_channel_index,
                pkiEncrypted=True,
            )

            # Prefer the real mesh packet id so ROUTING_APP ACKs can be matched
            real_id = getattr(mesh_packet, "id", None)
            if real_id:
                packet_id = int(real_id) & 0xFFFFFFFF

            logger.info(
                f"Sent admin message to !{target_node_id:08x}, packet_id={packet_id}"
            )

            # Update activity time on successful send
            self._last_activity_time = time.time()

            # Initialize pending response tracking keyed by the real packet id
            if want_response:
                with self._response_lock:
                    self._pending_responses[packet_id] = {}

            return packet_id

        except Exception as e:
            logger.error(f"Failed to send admin message: {e}")
            # Check if this was a connection error
            if "connection" in str(e).lower() or "socket" in str(e).lower():
                logger.warning("Connection error detected, marking as disconnected")
                self._connected = False
            return None

    def send_get_device_metadata(
        self,
        target_node_id: int,
    ) -> int | None:
        """
        Request device metadata from a target node.

        Args:
            target_node_id: The target node ID

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_device_metadata_request = True

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_get_config(
        self,
        target_node_id: int,
        config_type: int,
    ) -> int | None:
        """
        Request configuration from a target node.

        Args:
            target_node_id: The target node ID
            config_type: The config type (from ConfigType enum value)

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_config_request = config_type  # type: ignore[assignment]

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_get_module_config(
        self,
        target_node_id: int,
        module_config_type: int,
    ) -> int | None:
        """
        Request module configuration from a target node.

        Args:
            target_node_id: The target node ID
            module_config_type: The module config type (from ModuleConfigType enum value)

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_module_config_request = module_config_type  # type: ignore[assignment]

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_get_channel(
        self,
        target_node_id: int,
        channel_index: int,
    ) -> int | None:
        """
        Request channel configuration from a target node.

        Args:
            target_node_id: The target node ID
            channel_index: The channel index (0-7)

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_channel_request = channel_index + 1

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_begin_edit_settings(
        self,
        target_node_id: int,
    ) -> int | None:
        """
        Begin a settings edit transaction on a target node.

        This should be called before making multiple config changes.
        The node will hold changes in memory until commit_edit_settings is called.

        This follows the Meshtastic protocol for atomic config updates.
        See: https://github.com/meshtastic/web packages/core/src/meshDevice.ts

        Args:
            target_node_id: The target node ID

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.begin_edit_settings = True

        logger.info(f"Sending begin_edit_settings to !{target_node_id:08x}")

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_commit_edit_settings(
        self,
        target_node_id: int,
    ) -> int | None:
        """
        Commit a settings edit transaction on a target node.

        This should be called after making config changes to apply them.
        The node will save all pending changes to flash and apply them.

        This follows the Meshtastic protocol for atomic config updates.
        See: https://github.com/meshtastic/web packages/core/src/meshDevice.ts

        Args:
            target_node_id: The target node ID

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.commit_edit_settings = True

        logger.info(f"Sending commit_edit_settings to !{target_node_id:08x}")

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_set_config(
        self,
        target_node_id: int,
        config_type: str,
        config_data: dict,
    ) -> int | None:
        """
        Set configuration on a target node.

        Args:
            target_node_id: The target node ID
            config_type: The config type (device, position, etc.)
            config_data: Dictionary of config values to set

        Returns:
            Packet ID if sent successfully
        """
        from meshtastic import config_pb2

        admin_msg = admin_pb2.AdminMessage()

        # Build the config protobuf based on type
        config = config_pb2.Config()

        if config_type == "device":
            for key, value in config_data.items():
                if hasattr(config.device, key):
                    setattr(config.device, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "position":
            for key, value in config_data.items():
                # Map gps_enabled to gps_mode
                if key == "gps_enabled":
                    config.position.gps_mode = value
                elif hasattr(config.position, key):
                    setattr(config.position, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "power":
            for key, value in config_data.items():
                if hasattr(config.power, key):
                    setattr(config.power, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "network":
            for key, value in config_data.items():
                if hasattr(config.network, key):
                    setattr(config.network, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "display":
            for key, value in config_data.items():
                if hasattr(config.display, key):
                    setattr(config.display, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "lora":
            for key, value in config_data.items():
                if hasattr(config.lora, key):
                    setattr(config.lora, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "bluetooth":
            logger.info(
                f"Processing bluetooth config with keys: {list(config_data.keys())}"
            )
            for key, value in config_data.items():
                if hasattr(config.bluetooth, key):
                    # Ensure mode is an integer (enum field)
                    if key == "mode" and not isinstance(value, int):
                        try:
                            value = int(value)
                            logger.info(f"Bluetooth {key}: converted to int {value}")
                        except (ValueError, TypeError) as e:
                            logger.error(
                                f"Bluetooth {key}: cannot convert {value!r} to int - {e}"
                            )
                            continue
                    # Ensure fixed_pin is an integer
                    if key == "fixed_pin" and not isinstance(value, int):
                        try:
                            value = int(value)
                            logger.info(f"Bluetooth {key}: converted to int {value}")
                        except (ValueError, TypeError) as e:
                            logger.error(
                                f"Bluetooth {key}: cannot convert {value!r} to int - {e}"
                            )
                            continue

                    logger.info(
                        f"Bluetooth {key}: setting to {value} (type: {type(value).__name__})"
                    )
                    try:
                        setattr(config.bluetooth, key, value)
                        actual = getattr(config.bluetooth, key)
                        logger.info(f"Bluetooth {key}: verified as {actual}")
                    except Exception as e:
                        logger.error(f"Bluetooth {key}: FAILED to set - {e}")
                else:
                    logger.warning(
                        f"Bluetooth config: unknown field '{key}' - skipping"
                    )
            admin_msg.set_config.CopyFrom(config)
            # Log each field explicitly to avoid multi-line log issues
            logger.info(
                f"Bluetooth config prepared: enabled={config.bluetooth.enabled}, "
                f"mode={config.bluetooth.mode}, fixed_pin={config.bluetooth.fixed_pin}"
            )

        elif config_type == "security":
            logger.info(
                f"Processing security config with keys: {list(config_data.keys())}"
            )

            # CRITICAL: Never send private_key or public_key in SET commands
            # These are device-generated and sending them (even unchanged values)
            # can corrupt the node's identity. The firmware may interpret any
            # value as "set this key" which can wipe the node's actual keys.
            FORBIDDEN_SECURITY_FIELDS = {"private_key", "public_key"}
            forbidden_found = [
                k for k in config_data.keys() if k in FORBIDDEN_SECURITY_FIELDS
            ]
            if forbidden_found:
                logger.warning(
                    f"Security config: BLOCKING forbidden fields {forbidden_found} - "
                    "these are device-generated and must not be sent"
                )
                # Filter them out
                config_data = {
                    k: v
                    for k, v in config_data.items()
                    if k not in FORBIDDEN_SECURITY_FIELDS
                }
                logger.info(
                    f"Security config: proceeding with safe fields: {list(config_data.keys())}"
                )

            def _decode_key_to_bytes(item: Any, field_name: str) -> bytes | None:
                """Decode a key value (string, bytes, etc.) to bytes.

                Returns None if the value is empty or invalid.
                """
                if not item:
                    return None
                if isinstance(item, bytes):
                    return item if item else None
                if isinstance(item, str):
                    item = item.strip()
                    if not item:
                        return None
                    # Try hex FIRST - admin keys are typically 64 hex chars (32 bytes)
                    if len(item) == 64 and all(
                        c in "0123456789abcdefABCDEF" for c in item
                    ):
                        try:
                            return bytes.fromhex(item)
                        except Exception:
                            pass
                    # Try base64
                    try:
                        import base64

                        return base64.b64decode(item)
                    except Exception:
                        pass
                    # Try hex for other lengths
                    try:
                        return bytes.fromhex(item)
                    except Exception:
                        pass
                    # Last resort: raw encoding
                    logger.warning(f"Security {field_name}: using raw string encoding")
                    return item.encode()
                return None

            def _deduplicate_keys(
                keys: list[Any] | tuple[Any, ...], field_name: str
            ) -> tuple[list[bytes], int]:
                """Deduplicate a list of keys, preserving order of first occurrence.

                Returns (deduplicated_keys, duplicate_count).
                """
                seen: set[bytes] = set()
                unique_keys: list[bytes] = []
                duplicates = 0

                for item in keys:
                    key_bytes = _decode_key_to_bytes(item, field_name)
                    if key_bytes is None:
                        continue
                    if key_bytes in seen:
                        duplicates += 1
                        logger.info(
                            f"Security {field_name}: skipping duplicate key {key_bytes.hex()[:16]}..."
                        )
                    else:
                        seen.add(key_bytes)
                        unique_keys.append(key_bytes)

                return unique_keys, duplicates

            for key, value in config_data.items():
                if hasattr(config.security, key):
                    # Handle repeated fields (admin_key, etc.) specially
                    field = getattr(config.security, key)
                    if hasattr(field, "extend"):
                        # This is a repeated field - clear and extend
                        field.clear()
                        if isinstance(value, (list, tuple)):
                            # Deduplicate keys to prevent issues when restoring
                            unique_keys, dup_count = _deduplicate_keys(value, key)
                            for key_bytes in unique_keys:
                                # Validate admin_key length - must be exactly 32 bytes
                                if key == "admin_key" and len(key_bytes) != 32:
                                    logger.warning(
                                        f"Security admin_key: SKIPPING invalid key - "
                                        f"expected 32 bytes, got {len(key_bytes)} bytes "
                                        f"(hex: {key_bytes.hex()[:32]}...)"
                                    )
                                    continue
                                logger.info(
                                    f"Security {key}: adding {len(key_bytes)} bytes"
                                )
                                field.append(key_bytes)
                            if dup_count > 0:
                                logger.warning(
                                    f"Security {key}: removed {dup_count} duplicate key(s)"
                                )
                        logger.info(f"Security {key}: added {len(field)} unique items")
                    else:
                        # Regular scalar field (is_managed, serial_enabled, etc.)
                        # Note: private_key and public_key are already filtered out above
                        logger.info(f"Security {key}: setting to {value}")
                        setattr(config.security, key, value)
                else:
                    logger.warning(f"Security config: unknown field '{key}' - skipping")

            # CRITICAL SAFETY CHECK: If admin_key was requested but we have NO valid keys,
            # ABORT the entire operation to avoid wiping existing admin keys on the device
            if "admin_key" in config_data:
                admin_key_count = len(config.security.admin_key)
                if admin_key_count == 0:
                    logger.error(
                        "Security config: ABORTING - admin_key was requested but "
                        "no valid 32-byte keys were found. This would wipe existing admin keys!"
                    )
                    return None
                logger.info(f"Security config admin_key count: {admin_key_count}")
                for i, k in enumerate(config.security.admin_key):
                    logger.info(f"  admin_key[{i}]: {k.hex()}")

            admin_msg.set_config.CopyFrom(config)

            # Log the exact fields being sent in the security config
            security_fields_sent = [
                f.name for f, _ in admin_msg.set_config.security.ListFields()
            ]

            # CRITICAL SAFETY CHECK: If no security fields are being sent, ABORT
            # An empty security config could have unintended side effects
            if not security_fields_sent:
                logger.error(
                    "Security config: ABORTING - no fields to send! "
                    f"Original config_data keys were: {list(config_data.keys())}"
                )
                return None

            logger.info(
                f"Security config prepared - fields being sent: {security_fields_sent}"
            )
            logger.info(
                f"Security config admin_key count: {len(admin_msg.set_config.security.admin_key)}"
            )
            # Log serialized bytes for debugging protocol issues
            serialized = admin_msg.SerializeToString()
            logger.debug(
                f"Security config serialized ({len(serialized)} bytes): {serialized.hex()}"
            )

        else:
            logger.error(f"Unknown config type: {config_type}")
            return None

        logger.info(f"Sending set_config for {config_type} to !{target_node_id:08x}")

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_set_module_config(
        self,
        target_node_id: int,
        module_type: str,
        module_data: dict,
    ) -> int | None:
        """
        Set module configuration on a target node.

        Args:
            target_node_id: The target node ID
            module_type: The module type (mqtt, serial, telemetry, etc.)
            module_data: Dictionary of module config values to set

        Returns:
            Packet ID if sent successfully
        """
        from meshtastic.protobuf import module_config_pb2

        admin_msg = admin_pb2.AdminMessage()
        module_config = module_config_pb2.ModuleConfig()

        module_type_lower = module_type.lower()

        if module_type_lower == "mqtt":
            for key, value in module_data.items():
                if hasattr(module_config.mqtt, key):
                    setattr(module_config.mqtt, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "serial":
            for key, value in module_data.items():
                if hasattr(module_config.serial, key):
                    setattr(module_config.serial, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "extnotif":
            for key, value in module_data.items():
                if hasattr(module_config.external_notification, key):
                    setattr(module_config.external_notification, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "storeforward":
            for key, value in module_data.items():
                if hasattr(module_config.store_forward, key):
                    setattr(module_config.store_forward, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "rangetest":
            for key, value in module_data.items():
                if hasattr(module_config.range_test, key):
                    setattr(module_config.range_test, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "telemetry":
            for key, value in module_data.items():
                if hasattr(module_config.telemetry, key):
                    setattr(module_config.telemetry, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "cannedmsg":
            for key, value in module_data.items():
                if hasattr(module_config.canned_message, key):
                    setattr(module_config.canned_message, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "audio":
            for key, value in module_data.items():
                if hasattr(module_config.audio, key):
                    setattr(module_config.audio, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "remotehardware":
            for key, value in module_data.items():
                if hasattr(module_config.remote_hardware, key):
                    setattr(module_config.remote_hardware, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "neighborinfo":
            for key, value in module_data.items():
                if hasattr(module_config.neighbor_info, key):
                    setattr(module_config.neighbor_info, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "ambientlighting":
            for key, value in module_data.items():
                if hasattr(module_config.ambient_lighting, key):
                    setattr(module_config.ambient_lighting, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "detectionsensor":
            for key, value in module_data.items():
                if hasattr(module_config.detection_sensor, key):
                    setattr(module_config.detection_sensor, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        elif module_type_lower == "paxcounter":
            for key, value in module_data.items():
                if hasattr(module_config.paxcounter, key):
                    setattr(module_config.paxcounter, key, value)
            admin_msg.set_module_config.CopyFrom(module_config)

        else:
            logger.error(f"Unknown module type: {module_type}")
            return None

        logger.info(
            f"Sending set_module_config for {module_type} to !{target_node_id:08x}"
        )

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_set_channel(
        self,
        target_node_id: int,
        channel_index: int,
        channel_data: dict,
    ) -> int | None:
        """
        Set channel configuration on a target node.

        Args:
            target_node_id: The target node ID
            channel_index: The channel index (0-7)
            channel_data: Dictionary of channel settings. PSK can be:
                - bytes: raw PSK bytes
                - str (hex): hex-encoded PSK (e.g., "deadbeef...")
                - str (base64): base64-encoded PSK (standard Meshtastic format)

        Returns:
            Packet ID if sent successfully
        """
        import base64

        from meshtastic import channel_pb2

        admin_msg = admin_pb2.AdminMessage()

        channel = channel_pb2.Channel()
        channel.index = channel_index

        # Set channel role
        if "role" in channel_data:
            channel.role = channel_data["role"]

        # Set channel settings
        if "name" in channel_data:
            channel.settings.name = channel_data["name"]

        if "psk" in channel_data:
            psk = channel_data["psk"]
            if isinstance(psk, bytes):
                channel.settings.psk = psk
            elif isinstance(psk, str):
                # Try base64 first (Meshtastic web client format), then hex
                try:
                    # Base64 strings typically have specific characteristics:
                    # - Only contain A-Za-z0-9+/= characters
                    # - May have padding with =
                    # - Are often NOT valid hex (odd length or invalid chars)
                    decoded = base64.b64decode(psk)
                    channel.settings.psk = decoded
                    logger.debug(f"Channel PSK decoded as base64: {len(decoded)} bytes")
                except Exception:
                    # Try hex encoding
                    try:
                        decoded = bytes.fromhex(psk)
                        channel.settings.psk = decoded
                        logger.debug(
                            f"Channel PSK decoded as hex: {len(decoded)} bytes"
                        )
                    except Exception:
                        # Use as raw bytes if all else fails
                        channel.settings.psk = psk.encode("utf-8")
                        logger.warning(
                            f"Channel PSK could not be decoded, using raw: {psk[:10]}..."
                        )

        if "position_precision" in channel_data:
            channel.settings.module_settings.position_precision = channel_data[
                "position_precision"
            ]

        # Handle uplink/downlink enabled flags
        if "uplink_enabled" in channel_data:
            channel.settings.uplink_enabled = channel_data["uplink_enabled"]
        if "downlink_enabled" in channel_data:
            channel.settings.downlink_enabled = channel_data["downlink_enabled"]

        admin_msg.set_channel.CopyFrom(channel)

        logger.info(f"Sending set_channel {channel_index} to !{target_node_id:08x}")

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_set_owner(
        self,
        target_node_id: int,
        long_name: str,
        short_name: str,
        is_licensed: bool = False,
    ) -> int | None:
        """
        Set owner/user settings on a target node.

        Args:
            target_node_id: The target node ID
            long_name: The owner's long name (max 39 characters)
            short_name: The owner's short name (max 4 characters)
            is_licensed: Whether the owner is a licensed HAM operator

        Returns:
            Packet ID if sent successfully
        """
        from meshtastic import admin_pb2, mesh_pb2

        if not self.connect():
            return None

        admin_msg = admin_pb2.AdminMessage()
        user = mesh_pb2.User()

        user.long_name = long_name[:39]  # Enforce max length
        user.short_name = short_name[:4]  # Enforce max length
        user.is_licensed = is_licensed

        admin_msg.set_owner.CopyFrom(user)

        logger.info(
            f"Sending set_owner to !{target_node_id:08x}: "
            f"long_name={long_name}, short_name={short_name}, is_licensed={is_licensed}"
        )

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_reboot(
        self,
        target_node_id: int,
        seconds: int = 5,
    ) -> int | None:
        """
        Send a reboot command to a target node.

        Uses the Meshtastic library's built-in reboot method which properly
        handles session key negotiation for remote admin commands.

        Args:
            target_node_id: The target node ID
            seconds: Seconds until reboot

        Returns:
            Packet ID if sent successfully
        """
        if not self.connect():
            logger.error("Cannot send reboot: not connected to node")
            return None

        if self._interface is None:
            return None

        try:
            # Get or create a Node object for the target
            # The Meshtastic library's Node.reboot() handles session key negotiation
            node_id_str = f"!{target_node_id:08x}"
            node = self._interface.getNode(node_id_str, requestChannels=False)

            if node is None:
                logger.error(f"Could not get node object for !{target_node_id:08x}")
                return None

            # Use the library's reboot method which handles session keys properly
            logger.info(
                f"Sending reboot command to !{target_node_id:08x} via library method"
            )
            result = node.reboot(secs=seconds)

            # Generate a tracking packet ID
            packet_id = random.getrandbits(32)

            if result:
                logger.info(
                    f"Reboot command sent to !{target_node_id:08x}, "
                    f"delay={seconds}s, packet_id={packet_id}"
                )
                return packet_id
            else:
                logger.warning(
                    f"Reboot command may have been sent to !{target_node_id:08x}, "
                    f"but no confirmation received"
                )
                return packet_id

        except Exception as e:
            logger.error(f"Failed to send reboot command: {e}")
            return None

    def send_shutdown(
        self,
        target_node_id: int,
        seconds: int = 5,
    ) -> int | None:
        """
        Send a shutdown command to a target node.

        Uses the Meshtastic library's built-in shutdown method which properly
        handles session key negotiation for remote admin commands.

        Args:
            target_node_id: The target node ID
            seconds: Seconds until shutdown

        Returns:
            Packet ID if sent successfully
        """
        if not self.connect():
            logger.error("Cannot send shutdown: not connected to node")
            return None

        if self._interface is None:
            return None

        try:
            # Get or create a Node object for the target
            node_id_str = f"!{target_node_id:08x}"
            node = self._interface.getNode(node_id_str, requestChannels=False)

            if node is None:
                logger.error(f"Could not get node object for !{target_node_id:08x}")
                return None

            # Use the library's shutdown method which handles session keys properly
            logger.info(
                f"Sending shutdown command to !{target_node_id:08x} via library method"
            )
            result = node.shutdown(secs=seconds)

            # Generate a tracking packet ID
            packet_id = random.getrandbits(32)

            if result:
                logger.info(
                    f"Shutdown command sent to !{target_node_id:08x}, "
                    f"delay={seconds}s, packet_id={packet_id}"
                )
                return packet_id
            else:
                logger.warning(
                    f"Shutdown command may have been sent to !{target_node_id:08x}, "
                    f"but no confirmation received"
                )
                return packet_id

        except Exception as e:
            logger.error(f"Failed to send shutdown command: {e}")
            return None

    def send_remove_node(
        self,
        target_node_id: int,
        node_to_remove: int,
    ) -> int | None:
        """
        Remove a node from the target node's nodedb.

        This sends an admin message to the target node instructing it to
        remove the specified node from its local node database. This is
        useful for cleaning up stale entries or removing nodes that are
        no longer part of the mesh.

        Args:
            target_node_id: The node to send the command to (gateway)
            node_to_remove: The node number to remove from the nodedb

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.remove_by_nodenum = node_to_remove

        logger.info(
            f"Sending remove_by_nodenum command to !{target_node_id:08x} "
            f"to remove !{node_to_remove:08x}"
        )

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_set_favorite_node(
        self,
        target_node_id: int,
        node_to_favorite: int,
    ) -> int | None:
        """
        Set a node as a favorite on the target node's device.

        Sends an admin message to the target node instructing it to mark
        the specified node as a favorite in its local node database.

        Args:
            target_node_id: The node to send the command to
            node_to_favorite: The node number to mark as favorite

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.set_favorite_node = node_to_favorite

        logger.info(
            f"Sending set_favorite_node command to !{target_node_id:08x} "
            f"to favorite !{node_to_favorite:08x}"
        )

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_remove_favorite_node(
        self,
        target_node_id: int,
        node_to_unfavorite: int,
    ) -> int | None:
        """
        Remove a node from the target node's favorites.

        Sends an admin message to the target node instructing it to
        unmark the specified node as a favorite in its local node database.

        Args:
            target_node_id: The node to send the command to
            node_to_unfavorite: The node number to remove from favorites

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.remove_favorite_node = node_to_unfavorite

        logger.info(
            f"Sending remove_favorite_node command to !{target_node_id:08x} "
            f"to unfavorite !{node_to_unfavorite:08x}"
        )

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_nodedb_reset(
        self,
        target_node_id: int,
    ) -> int | None:
        """
        Reset the nodedb on the target node.

        This clears all nodes from the target's database except itself.
        The node will need to rediscover other nodes on the mesh.

        Args:
            target_node_id: The node to reset the nodedb on

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.nodedb_reset = 1

        logger.info(f"Sending nodedb_reset command to !{target_node_id:08x}")

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_factory_reset(
        self,
        target_node_id: int,
        config_only: bool = True,
    ) -> int | None:
        """
        Factory reset the target node.

        Args:
            target_node_id: The node to reset
            config_only: If True, only reset config (preserves nodedb).
                        If False, full factory reset (everything).

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()

        if config_only:
            admin_msg.factory_reset_config = 1
            logger.info(
                f"Sending factory_reset_config command to !{target_node_id:08x}"
            )
        else:
            admin_msg.factory_reset_device = 1
            logger.info(
                f"Sending factory_reset_device command to !{target_node_id:08x}"
            )

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=True,
        )

    def send_telemetry_request(
        self,
        target_node_id: int,
        telemetry_type: str = "device_metrics",
        timeout: float = 25.0,
        hop_limit: int | None = None,
        want_ack: bool = False,
    ) -> dict[str, Any] | None:
        """
        Request telemetry from a target node and wait for response.

        This sends a telemetry request to the target node and waits for
        the response containing current device metrics.

        Correlation uses the mesh packet id (requestId) plus metric-type
        validation. Responses are accepted via pubsub ``_on_receive`` and/or
        the meshtastic ``onResponse`` callback (dual-path for reliability).

        Args:
            target_node_id: The destination node ID
            telemetry_type: Type of telemetry to request
                           (device_metrics, environment_metrics, etc.)
            timeout: Timeout in seconds to wait for response (default 25s for mesh)
            hop_limit: Optional mesh hopLimit for multi-hop reachability
            want_ack: Request mesh-layer ACK/retries (helpful beyond 0 hops)

        Returns:
            Dictionary with telemetry data if successful, None otherwise
        """
        from meshtastic import telemetry_pb2

        if not self.ensure_healthy_connection():
            logger.error("Cannot send telemetry request: connection unhealthy")
            return None

        if self._interface is None:
            return None

        try:
            # Create telemetry request
            telemetry = telemetry_pb2.Telemetry()
            telemetry_type = apply_telemetry_request_type(telemetry, telemetry_type)

            # Set up class-level tracking for this request
            # Generation token ensures cleanup cannot remove a newer request
            # for the same node if polls overlap.
            response_event = threading.Event()
            response_data: dict[str, Any] = {}
            generation = time.time_ns()

            with self._pending_telemetry_lock:
                self._pending_telemetry_requests[target_node_id] = {
                    "event": response_event,
                    "response_data": response_data,
                    "telemetry_type": telemetry_type,
                    "requested_at": time.time(),
                    "generation": generation,
                    "request_id": None,
                    "completed": False,
                }

            # Track statistics - increment request count
            request_time = time.time()
            with self._telemetry_stats_lock:
                self._telemetry_stats["total_requests"] += 1
                self._telemetry_stats["last_request_time"] = request_time

                # Per-node stats
                node_key = str(target_node_id)
                if node_key not in self._telemetry_stats["per_node_stats"]:
                    self._telemetry_stats["per_node_stats"][node_key] = {
                        "requests": 0,
                        "successes": 0,
                        "timeouts": 0,
                        "errors": 0,
                        "last_request": None,
                        "last_success": None,
                    }
                self._telemetry_stats["per_node_stats"][node_key]["requests"] += 1
                self._telemetry_stats["per_node_stats"][node_key]["last_request"] = (
                    request_time
                )

            try:
                # Dual-path delivery: meshtastic onResponse can drop callbacks,
                # and pubsub can race; either path completes the pending wait.
                logger.info(
                    f"Sending telemetry request to !{target_node_id:08x} "
                    f"(type={telemetry_type}, hop_limit={hop_limit}, "
                    f"want_ack={want_ack})"
                )

                send_kwargs: dict[str, Any] = {
                    "data": telemetry,
                    "destinationId": target_node_id,
                    "portNum": portnums_pb2.PortNum.TELEMETRY_APP,
                    "wantAck": want_ack,
                    "wantResponse": True,
                    "onResponse": self._match_and_complete_telemetry,
                }
                if hop_limit is not None:
                    send_kwargs["hopLimit"] = int(hop_limit)

                mesh_packet = self._interface.sendData(**send_kwargs)

                request_id = self._normalize_request_id(
                    getattr(mesh_packet, "id", None)
                )
                with self._pending_telemetry_lock:
                    pending = self._pending_telemetry_requests.get(target_node_id)
                    if pending and pending.get("generation") == generation:
                        pending["request_id"] = request_id

                logger.info(
                    f"Telemetry request sent, packet_id={request_id} "
                    f"(target=!{target_node_id:08x})"
                )

                # Update activity time
                self._last_activity_time = time.time()

                # Wait for response with timeout, then a short grace for late RF
                if response_event.wait(timeout=timeout):
                    pass
                else:
                    # Late reply often arrives just after the wait ends
                    response_event.wait(timeout=0.75)

                if not response_data.get("telemetry"):
                    with self._pending_telemetry_lock:
                        late = None
                        if request_id is not None:
                            late = self._telemetry_late_by_request.pop(request_id, None)
                        if not late or not late.get("telemetry"):
                            cached = self._telemetry_latest_by_node.get(target_node_id)
                            if cached and cached.get("telemetry"):
                                age = time.time() - float(
                                    cached.get("timestamp") or 0
                                )
                                if age <= 2.5:
                                    late = cached
                    if late and late.get("telemetry"):
                        response_data.update(
                            {
                                "telemetry": late["telemetry"],
                                "timestamp": late.get("timestamp", time.time()),
                                "from_node": late.get("from_node", target_node_id),
                                "request_id": request_id,
                                "late_cache": True,
                            }
                        )
                        if late.get("raw_payload") is not None:
                            response_data["raw_payload"] = late["raw_payload"]

                # App telemetry wins over routing NO_RESPONSE — some firmware
                # emits both, and health solicits must not discard real metrics.
                if response_data.get("telemetry"):
                    routing_error = response_data.pop("error", None)
                    if routing_error:
                        response_data["routing_warning"] = routing_error
                        logger.info(
                            "Telemetry metrics received for !%08x despite "
                            "routing %s — treating as success",
                            target_node_id,
                            routing_error,
                        )
                    logger.info(
                        f"Telemetry request successful for !{target_node_id:08x}"
                        + (" (late cache)" if response_data.get("late_cache") else "")
                    )
                    success_time = time.time()
                    with self._telemetry_stats_lock:
                        self._telemetry_stats["successful_responses"] += 1
                        self._telemetry_stats["last_success_time"] = success_time
                        self._telemetry_stats["per_node_stats"][node_key][
                            "successes"
                        ] += 1
                        self._telemetry_stats["per_node_stats"][node_key][
                            "last_success"
                        ] = success_time

                    response_data["stats"] = self.get_telemetry_stats(target_node_id)
                    return response_data

                if "error" in response_data:
                    logger.warning(
                        f"Telemetry request failed for !{target_node_id:08x}: "
                        f"{response_data.get('error')}"
                    )
                    with self._telemetry_stats_lock:
                        self._telemetry_stats["errors"] += 1
                        self._telemetry_stats["per_node_stats"][node_key][
                            "errors"
                        ] += 1
                    return None

                logger.warning(
                    f"Telemetry request timeout for !{target_node_id:08x} "
                    f"(packet_id={request_id})"
                )
                with self._telemetry_stats_lock:
                    self._telemetry_stats["timeouts"] += 1
                    self._telemetry_stats["per_node_stats"][node_key][
                        "timeouts"
                    ] += 1
                return None

            finally:
                # Only remove our generation — never clobber a newer poll
                with self._pending_telemetry_lock:
                    pending = self._pending_telemetry_requests.get(target_node_id)
                    if pending and pending.get("generation") == generation:
                        self._pending_telemetry_requests.pop(target_node_id, None)

        except Exception as e:
            logger.error(f"Failed to send telemetry request: {e}")
            # Track error
            with self._telemetry_stats_lock:
                self._telemetry_stats["errors"] += 1
            return None

    def get_telemetry_stats(self, node_id: int | None = None) -> dict[str, Any]:
        """
        Get telemetry request statistics.

        Args:
            node_id: Optional node ID to get per-node stats for

        Returns:
            Dictionary with statistics
        """
        with self._telemetry_stats_lock:
            total = self._telemetry_stats["total_requests"]
            successes = self._telemetry_stats["successful_responses"]
            timeouts = self._telemetry_stats["timeouts"]
            errors = self._telemetry_stats["errors"]

            # Calculate success rate
            success_rate = (successes / total * 100) if total > 0 else 0
            failure_rate = 100 - success_rate

            stats = {
                "total_requests": total,
                "successful_responses": successes,
                "timeouts": timeouts,
                "errors": errors,
                "success_rate": round(success_rate, 1),
                "failure_rate": round(failure_rate, 1),
                "last_request_time": self._telemetry_stats["last_request_time"],
                "last_success_time": self._telemetry_stats["last_success_time"],
            }

            # Add per-node stats if requested
            if node_id is not None:
                node_key = str(node_id)
                node_stats = self._telemetry_stats["per_node_stats"].get(node_key, {})
                if node_stats:
                    node_total = node_stats.get("requests", 0)
                    node_successes = node_stats.get("successes", 0)
                    node_success_rate = (
                        (node_successes / node_total * 100) if node_total > 0 else 0
                    )
                    stats["node_stats"] = {
                        "node_id": node_id,
                        "hex_id": f"!{node_id:08x}",
                        "requests": node_total,
                        "successes": node_successes,
                        "timeouts": node_stats.get("timeouts", 0),
                        "errors": node_stats.get("errors", 0),
                        "success_rate": round(node_success_rate, 1),
                        "last_request": node_stats.get("last_request"),
                        "last_success": node_stats.get("last_success"),
                    }

            return stats

    def reset_telemetry_stats(self) -> None:
        """Reset all telemetry statistics."""
        with self._telemetry_stats_lock:
            self._telemetry_stats = {
                "total_requests": 0,
                "successful_responses": 0,
                "timeouts": 0,
                "errors": 0,
                "last_request_time": None,
                "last_success_time": None,
                "per_node_stats": {},
            }
            logger.info("Telemetry statistics reset")


# Module-level singleton accessor
_tcp_publisher: TCPPublisher | None = None
_tcp_publisher_lock = threading.Lock()


def get_tcp_publisher() -> TCPPublisher:
    """Get the singleton TCPPublisher instance."""
    global _tcp_publisher
    if _tcp_publisher is None:
        with _tcp_publisher_lock:
            if _tcp_publisher is None:
                _tcp_publisher = TCPPublisher()
    return _tcp_publisher
