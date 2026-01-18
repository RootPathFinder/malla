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
                        "missed heartbeats, marking as disconnected"
                    )
                    self._connected = False
                    # Don't try to close the interface here, it may hang
                    # Just mark as disconnected so next operation triggers reconnect

        logger.debug("Keepalive loop exited")

    def reconnect(self) -> bool:
        """
        Force reconnection to the TCP node.

        This disconnects and reconnects, useful for recovering from stale connections.

        Returns:
            True if reconnection successful
        """
        logger.info("Forcing TCP reconnection...")
        self.disconnect()
        time.sleep(0.5)  # Brief pause before reconnecting
        return self.connect()

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
                    self._interface.close()
                except Exception as e:
                    logger.warning(f"Error closing TCP connection: {e}")
                finally:
                    self._interface = None
                    self._connected = False
                    logger.info("Disconnected from Meshtastic node")

    def _on_receive(self, packet: dict[str, Any], interface: Any = None) -> None:
        """Handle received packets from pubsub."""
        try:
            logger.debug(f"Received packet: {packet.get('decoded', {}).get('portnum')}")

            # Check for admin responses
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")

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
            for key, value in config_data.items():
                if hasattr(config.bluetooth, key):
                    setattr(config.bluetooth, key, value)
            admin_msg.set_config.CopyFrom(config)

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
                                if isinstance(item, str):
                                    # Convert base64 or hex string to bytes
                                    try:
                                        import base64

                                        decoded = base64.b64decode(item)
                                        logger.info(
                                            f"Security {key}: decoded base64 to {len(decoded)} bytes"
                                        )
                                        field.append(decoded)
                                    except Exception:
                                        try:
                                            decoded = bytes.fromhex(item)
                                            logger.info(
                                                f"Security {key}: decoded hex to {len(decoded)} bytes"
                                            )
                                            field.append(decoded)
                                        except Exception:
                                            logger.warning(
                                                f"Security {key}: using raw string encoding"
                                            )
                                            field.append(item.encode())
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
                        # Regular field
                        logger.info(f"Security {key}: setting to {value}")
                        setattr(config.security, key, value)
                else:
                    logger.warning(f"Security config: unknown field '{key}' - skipping")
            admin_msg.set_config.CopyFrom(config)
            logger.info(f"Security config prepared: {config.security}")

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
            channel_data: Dictionary of channel settings

        Returns:
            Packet ID if sent successfully
        """
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
            if isinstance(psk, str):
                channel.settings.psk = bytes.fromhex(psk)
            elif isinstance(psk, bytes):
                channel.settings.psk = psk

        if "position_precision" in channel_data:
            channel.settings.module_settings.position_precision = channel_data[
                "position_precision"
            ]

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
