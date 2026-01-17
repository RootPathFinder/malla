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

    @property
    def is_connected(self) -> bool:
        """Check if connected to the node."""
        return self._connected and self._interface is not None

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
                logger.info(
                    f"Connected to Meshtastic node at {self.tcp_host}:{self.tcp_port}"
                )

                # Get node info
                if self._interface.localNode:
                    node_id = self._interface.localNode.nodeNum
                    logger.info(f"Local node ID: {node_id} (!{node_id:08x})")

                return True

            except Exception as e:
                logger.error(f"Failed to connect to Meshtastic node: {e}")
                self._interface = None
                self._connected = False
                return False

    def disconnect(self) -> None:
        """Disconnect from the Meshtastic node."""
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

    def _on_receive(self, packet: dict[str, Any], interface: Any) -> None:
        """Handle received packets."""
        try:
            # Check if this is a response to a pending request
            request_id = packet.get("requestId")
            if request_id and request_id in self._pending_responses:
                with self._response_lock:
                    self._pending_responses[request_id] = {
                        "packet": packet,
                        "received_at": time.time(),
                    }
                    if request_id in self._response_events:
                        self._response_events[request_id].set()

            # Also check decoded for admin messages
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")
            if portnum == "ADMIN_APP":
                from_node = packet.get("fromId") or packet.get("from")
                logger.debug(f"Received admin response from {from_node}")

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
        event = threading.Event()

        with self._response_lock:
            self._response_events[packet_id] = event

            # Check if already received
            if packet_id in self._pending_responses:
                response = self._pending_responses.pop(packet_id)
                del self._response_events[packet_id]
                return response

        # Wait for response
        if event.wait(timeout=timeout):
            with self._response_lock:
                if packet_id in self._pending_responses:
                    response = self._pending_responses.pop(packet_id)
                    if packet_id in self._response_events:
                        del self._response_events[packet_id]
                    return response

        # Cleanup on timeout
        with self._response_lock:
            self._pending_responses.pop(packet_id, None)
            self._response_events.pop(packet_id, None)

        return None

    def get_local_node_id(self) -> int | None:
        """Get the local node ID from the connected interface."""
        if self._interface and self._interface.localNode:
            return self._interface.localNode.nodeNum
        return None

    def send_admin_message(
        self,
        target_node_id: int,
        admin_message: admin_pb2.AdminMessage,
        want_response: bool = True,
    ) -> int | None:
        """
        Send an admin message to a target node.

        Args:
            target_node_id: The destination node ID
            admin_message: The admin message protobuf
            want_response: Whether to wait for a response

        Returns:
            Packet ID if sent successfully, None otherwise
        """
        if not self.connect():
            logger.error("Cannot send admin message: not connected to node")
            return None

        if self._interface is None:
            return None

        try:
            # Serialize the admin message
            payload = admin_message.SerializeToString()

            # Generate a random packet ID for tracking
            packet_id = random.getrandbits(32)

            # Send the packet using sendData
            # Note: sendData returns a MeshPacket, we use our own packet_id for tracking
            self._interface.sendData(
                data=payload,
                destinationId=target_node_id,
                portNum=portnums_pb2.PortNum.ADMIN_APP,
                wantAck=True,
                wantResponse=want_response,
            )

            logger.info(
                f"Sent admin message to !{target_node_id:08x}, packet_id={packet_id}"
            )

            # Initialize pending response tracking
            if want_response:
                with self._response_lock:
                    self._pending_responses[packet_id] = {}

            return packet_id

        except Exception as e:
            logger.error(f"Failed to send admin message: {e}")
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

    def send_reboot(
        self,
        target_node_id: int,
        seconds: int = 5,
    ) -> int | None:
        """
        Send a reboot command to a target node.

        Args:
            target_node_id: The target node ID
            seconds: Seconds until reboot

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.reboot_seconds = seconds

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=False,
        )

    def send_shutdown(
        self,
        target_node_id: int,
        seconds: int = 5,
    ) -> int | None:
        """
        Send a shutdown command to a target node.

        Args:
            target_node_id: The target node ID
            seconds: Seconds until shutdown

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.shutdown_seconds = seconds

        return self.send_admin_message(
            target_node_id=target_node_id,
            admin_message=admin_msg,
            want_response=False,
        )


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
