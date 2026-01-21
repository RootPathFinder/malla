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

logger = logging.getLogger(__name__)


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
                error_reason = routing.get("errorReason", "NONE")
                request_id = packet.get("requestId")

                if request_id:
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

            elif portnum == "ADMIN_APP":
                from_node = packet.get("fromId") or packet.get("from")
                logger.info(f"Received admin response from {from_node}")

                # Parse the admin message from the payload
                admin_message = None
                payload = decoded.get("payload")
                if payload:
                    try:
                        admin_message = admin_pb2.AdminMessage()
                        if isinstance(payload, bytes):
                            admin_message.ParseFromString(payload)
                        else:
                            # payload might already be decoded by meshtastic lib
                            admin_message = decoded.get("admin")
                        logger.debug(f"Parsed admin message: {admin_message}")

                        # Extract and store session_passkey from the response
                        # Session passkeys are required for admin authentication
                        # on subsequent requests (like Android app does)
                        if admin_message and hasattr(admin_message, "session_passkey"):
                            session_passkey = admin_message.session_passkey
                            if session_passkey and len(session_passkey) > 0:
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
                                        self._session_passkeys[node_id] = (
                                            session_passkey
                                        )
                                    logger.info(
                                        f"Stored session_passkey for node !{node_id:08x} "
                                        f"({len(session_passkey)} bytes)"
                                    )

                    except Exception as e:
                        logger.warning(f"Failed to parse admin message: {e}")
                        # Try to get pre-parsed admin message
                        admin_message = decoded.get("admin")

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
                request_id = packet.get("requestId")
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
        # Clear any previous response
        self._admin_response_event.clear()
        self._last_admin_response = None

        event = threading.Event()

        with self._response_lock:
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

            # Generate a random packet ID for tracking
            packet_id = random.getrandbits(32)

            # Set session_passkey on the admin message if we have one for this target
            # Session passkeys are required for admin authentication (like Android app)
            with self._session_passkey_lock:
                if target_node_id in self._session_passkeys:
                    session_passkey = self._session_passkeys[target_node_id]
                    admin_message.session_passkey = session_passkey
                    logger.debug(
                        f"Including session_passkey for !{target_node_id:08x} "
                        f"({len(session_passkey)} bytes)"
                    )

            # Send the packet using sendData
            # Note: sendData returns a MeshPacket, we use our own packet_id for tracking
            # pkiEncrypted=True is required for admin messages to work on remote nodes
            self._interface.sendData(
                data=admin_message,
                destinationId=target_node_id,
                portNum=portnums_pb2.PortNum.ADMIN_APP,
                wantAck=True,
                wantResponse=want_response,
                channelIndex=admin_channel_index,
                pkiEncrypted=True,
            )

            logger.info(
                f"Sent admin message to !{target_node_id:08x}, packet_id={packet_id}"
            )

            # Update activity time on successful send
            self._last_activity_time = time.time()

            # Initialize pending response tracking
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
            for key, value in config_data.items():
                if hasattr(config.security, key):
                    # Handle repeated fields (admin_key, etc.) specially
                    field = getattr(config.security, key)
                    if hasattr(field, "extend"):
                        # This is a repeated field - clear and extend
                        field.clear()
                        if isinstance(value, (list, tuple)):
                            for item in value:
                                # Skip empty strings/None values
                                if not item:
                                    logger.debug(
                                        f"Security {key}: skipping empty value"
                                    )
                                    continue
                                if isinstance(item, str):
                                    # Convert hex or base64 string to bytes
                                    # Try hex FIRST - admin keys are typically 64 hex chars (32 bytes)
                                    # base64 decode can succeed on hex strings but produces garbage
                                    decoded = None
                                    if len(item) == 64 and all(
                                        c in "0123456789abcdefABCDEF" for c in item
                                    ):
                                        # Looks like 32-byte hex key
                                        try:
                                            decoded = bytes.fromhex(item)
                                            logger.info(
                                                f"Security {key}: decoded hex to {len(decoded)} bytes"
                                            )
                                        except Exception:
                                            pass
                                    if decoded is None:
                                        # Try base64
                                        try:
                                            import base64

                                            decoded = base64.b64decode(item)
                                            logger.info(
                                                f"Security {key}: decoded base64 to {len(decoded)} bytes"
                                            )
                                        except Exception:
                                            # Try hex (for non-64 char hex strings)
                                            try:
                                                decoded = bytes.fromhex(item)
                                                logger.info(
                                                    f"Security {key}: decoded hex to {len(decoded)} bytes"
                                                )
                                            except Exception:
                                                logger.warning(
                                                    f"Security {key}: using raw string encoding"
                                                )
                                                decoded = item.encode()
                                    field.append(decoded)
                                elif isinstance(item, bytes):
                                    logger.info(
                                        f"Security {key}: adding {len(item)} raw bytes"
                                    )
                                    field.append(item)
                                else:
                                    logger.info(
                                        f"Security {key}: adding value of type {type(item)}"
                                    )
                                    field.append(item)
                        logger.info(f"Security {key}: added {len(field)} items")
                    else:
                        # Regular field - may need bytes conversion for key fields
                        if key in ("public_key", "private_key") and isinstance(
                            value, str
                        ):
                            # Convert hex or base64 string to bytes
                            # Try hex FIRST for 64-char strings (32 bytes)
                            decoded = None
                            if len(value) == 64 and all(
                                c in "0123456789abcdefABCDEF" for c in value
                            ):
                                try:
                                    decoded = bytes.fromhex(value)
                                    logger.info(
                                        f"Security {key}: decoded hex to {len(decoded)} bytes"
                                    )
                                except Exception:
                                    pass
                            if decoded is None:
                                try:
                                    import base64

                                    decoded = base64.b64decode(value)
                                    logger.info(
                                        f"Security {key}: decoded base64 to {len(decoded)} bytes"
                                    )
                                except Exception:
                                    try:
                                        decoded = bytes.fromhex(value)
                                        logger.info(
                                            f"Security {key}: decoded hex to {len(decoded)} bytes"
                                        )
                                    except Exception:
                                        logger.warning(
                                            f"Security {key}: using raw string encoding"
                                        )
                                        decoded = value.encode()
                            setattr(config.security, key, decoded)
                        else:
                            logger.info(f"Security {key}: setting to {value}")
                            setattr(config.security, key, value)
                else:
                    logger.warning(f"Security config: unknown field '{key}' - skipping")
            # Log the actual count of admin_key entries before sending
            if hasattr(config.security, "admin_key"):
                logger.info(
                    f"Security config admin_key count: {len(config.security.admin_key)}"
                )
                for i, k in enumerate(config.security.admin_key):
                    logger.info(f"  admin_key[{i}]: {k.hex()}")
            admin_msg.set_config.CopyFrom(config)
            logger.info(
                f"Security config prepared, admin_msg admin_key count: {len(admin_msg.set_config.security.admin_key)}"
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

    def send_telemetry_request(
        self,
        target_node_id: int,
        telemetry_type: str = "device_metrics",
        timeout: float = 10.0,
    ) -> dict[str, Any] | None:
        """
        Request telemetry from a target node and wait for response.

        This sends a telemetry request to the target node and waits for
        the response containing current device metrics.

        Args:
            target_node_id: The destination node ID
            telemetry_type: Type of telemetry to request
                           (device_metrics, environment_metrics, etc.)
            timeout: Timeout in seconds to wait for response

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

            # Set the appropriate metrics type to request
            if telemetry_type == "environment_metrics":
                telemetry.environment_metrics.CopyFrom(
                    telemetry_pb2.EnvironmentMetrics()
                )
            elif telemetry_type == "power_metrics":
                telemetry.power_metrics.CopyFrom(telemetry_pb2.PowerMetrics())
            elif telemetry_type == "local_stats":
                telemetry.local_stats.CopyFrom(telemetry_pb2.LocalStats())
            else:
                # Default to device_metrics
                telemetry.device_metrics.CopyFrom(telemetry_pb2.DeviceMetrics())

            # Track response
            response_received = threading.Event()
            response_data: dict[str, Any] = {}

            def on_telemetry_response(packet: dict, interface: Any = None) -> None:
                """Handle telemetry response."""
                try:
                    decoded = packet.get("decoded", {})
                    if decoded.get("portnum") == "TELEMETRY_APP":
                        telemetry_data = decoded.get("telemetry", {})
                        from_id = packet.get("fromId", "")

                        # Check if this is from our target node
                        target_hex = f"!{target_node_id:08x}"
                        if from_id == target_hex or str(from_id) == str(target_node_id):
                            # Convert to JSON-serializable dict
                            telemetry_dict = convert_to_dict(telemetry_data)

                            response_data["telemetry"] = telemetry_dict
                            response_data["from_id"] = from_id
                            response_data["timestamp"] = time.time()
                            response_received.set()
                            logger.info(f"Received telemetry response from {from_id}")
                except Exception as e:
                    logger.error(f"Error processing telemetry response: {e}")

            # Subscribe to packet reception
            pub.subscribe(on_telemetry_response, "meshtastic.receive")

            try:
                # Send telemetry request with wantResponse=True
                logger.info(
                    f"Sending telemetry request to !{target_node_id:08x} "
                    f"(type={telemetry_type})"
                )

                self._interface.sendData(
                    data=telemetry,
                    destinationId=target_node_id,
                    portNum=portnums_pb2.PortNum.TELEMETRY_APP,
                    wantAck=True,
                    wantResponse=True,
                )

                # Update activity time
                self._last_activity_time = time.time()

                # Wait for response with timeout
                if response_received.wait(timeout=timeout):
                    logger.info(
                        f"Telemetry request successful for !{target_node_id:08x}"
                    )
                    return response_data
                else:
                    logger.warning(
                        f"Telemetry request timeout for !{target_node_id:08x}"
                    )
                    return None

            finally:
                # Unsubscribe from events
                try:
                    pub.unsubscribe(on_telemetry_response, "meshtastic.receive")
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"Failed to send telemetry request: {e}")
            return None


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
