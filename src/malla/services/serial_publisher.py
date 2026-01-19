"""
Serial Publisher service for sending Meshtastic admin commands.

This module provides functionality to publish admin commands to a Meshtastic node
connected via USB/Serial connection.
"""

import logging
import random
import threading
import time
from typing import Any

from meshtastic import admin_pb2, portnums_pb2
from meshtastic.serial_interface import SerialInterface
from pubsub import pub
from serial.tools import list_ports

from ..config import get_config

# Import get_hardware_model_name - avoid circular import by importing inside function
# from .admin_service import get_hardware_model_name

logger = logging.getLogger(__name__)


# Known Meshtastic device VID:PID pairs
KNOWN_MESHTASTIC_DEVICES = {
    # Silicon Labs CP210x (common on many boards)
    (0x10C4, 0xEA60): "CP210x USB-UART (Heltec, RAK, etc.)",
    # WCH CH340/CH341 (common on budget boards)
    (0x1A86, 0x7523): "CH340/CH341 (T-Beam, etc.)",
    (0x1A86, 0x55D4): "CH9102 (T-Beam V1.2+, Heltec V3)",
    # FTDI (some boards)
    (0x0403, 0x6001): "FTDI FT232R",
    (0x0403, 0x6015): "FTDI FT230X",
    # Espressif native USB (ESP32-S2/S3)
    (0x303A, 0x1001): "ESP32-S2/S3 Native USB",
    (0x303A, 0x0002): "ESP32-S2 JTAG",
    # Nordic nRF52 (RAK4631, T-Echo, etc.)
    (0x239A, 0x8029): "nRF52840 (T-Echo, RAK4631)",
    (0x239A, 0x0029): "nRF52840 Bootloader",
    # RAK Wireless
    (0x239A, 0x8082): "RAK4631",
    # LilyGo
    (0x303A, 0x80C2): "LilyGo T-Deck",
}


def discover_serial_ports(probe_devices: bool = False) -> list[dict[str, Any]]:
    """
    Discover available serial ports that might be Meshtastic devices.

    Args:
        probe_devices: If True, attempt to connect briefly to identify the device

    Returns:
        List of dictionaries with port info
    """
    ports = []
    for port in list_ports.comports():
        port_info: dict[str, Any] = {
            "device": port.device,
            "name": port.name,
            "description": port.description,
            "hwid": port.hwid,
            "vid": port.vid,
            "pid": port.pid,
            "serial_number": port.serial_number,
            "manufacturer": port.manufacturer,
            "product": port.product,
            "interface": port.interface,
        }

        # Check known VID:PID pairs first
        known_device = None
        is_meshtastic = False

        if port.vid and port.pid:
            known_device = KNOWN_MESHTASTIC_DEVICES.get((port.vid, port.pid))
            if known_device:
                is_meshtastic = True
                port_info["chip_type"] = known_device

        # Fallback to keyword matching
        if not is_meshtastic:
            description_lower = (port.description or "").lower()
            product_lower = (port.product or "").lower()
            manufacturer_lower = (port.manufacturer or "").lower()

            meshtastic_keywords = [
                "cp210",
                "ch340",
                "ch341",
                "ch910",
                "ft232",
                "silicon labs",
                "wch",
                "meshtastic",
                "esp32",
                "nrf52",
                "rak",
                "heltec",
                "lilygo",
                "t-beam",
                "t-echo",
            ]

            for keyword in meshtastic_keywords:
                if (
                    keyword in description_lower
                    or keyword in product_lower
                    or keyword in manufacturer_lower
                ):
                    is_meshtastic = True
                    break

        port_info["is_meshtastic_likely"] = is_meshtastic
        port_info["device_info"] = None  # Will be populated if probed

        ports.append(port_info)

    # Sort with likely Meshtastic devices first
    ports.sort(key=lambda x: (not x["is_meshtastic_likely"], x["device"]))

    # Optionally probe devices to get actual Meshtastic info
    if probe_devices:
        for port_info in ports:
            if port_info["is_meshtastic_likely"]:
                device_info = probe_meshtastic_device(port_info["device"])
                if device_info:
                    port_info["device_info"] = device_info
                    port_info["is_meshtastic_confirmed"] = True

    return ports


def probe_meshtastic_device(port: str, timeout: float = 8.0) -> dict[str, Any] | None:
    """
    Attempt to briefly connect to a serial port and identify the Meshtastic device.

    Args:
        port: Serial port device path
        timeout: Maximum time to wait for device info

    Returns:
        Dictionary with device info or None if not a Meshtastic device
    """
    interface = None
    try:
        logger.info(f"Probing device on {port}...")

        # Check if port exists first
        import os

        if not os.path.exists(port):
            logger.debug(f"Port {port} does not exist")
            return None

        # Try to connect
        try:
            interface = SerialInterface(port, noProto=False, debugOut=None)
        except Exception as e:
            logger.debug(f"Failed to open serial interface on {port}: {e}")
            return None

        # Wait for the interface to initialize
        start_time = time.time()
        while time.time() - start_time < timeout:
            if interface.myInfo and interface.myInfo.my_node_num:
                break
            time.sleep(0.3)

        if not interface.myInfo or not interface.myInfo.my_node_num:
            logger.debug(f"No node info received from {port} within {timeout}s")
            return None

        # Extract device information
        my_node_num = interface.myInfo.my_node_num
        logger.info(f"Got node info from {port}: !{my_node_num:08x}")

        nodes = interface.nodes or {}
        node_info = nodes.get(f"!{my_node_num:08x}", {}) or {}

        device_info: dict[str, Any] = {
            "node_id": my_node_num,
            "node_hex": f"!{my_node_num:08x}",
        }

        # Get hardware model
        if "user" in node_info:
            user = node_info["user"]
            device_info["short_name"] = user.get("shortName", "")
            device_info["long_name"] = user.get("longName", "")
            hw_model = user.get("hwModel", 0)
            device_info["hardware_model"] = hw_model
            # Import here to avoid circular import
            from .admin_service import get_hardware_model_name

            device_info["hardware_model_name"] = get_hardware_model_name(hw_model)

        # Get firmware version
        if "deviceMetrics" in node_info:
            device_metrics = node_info["deviceMetrics"]
            device_info["firmware_version"] = device_metrics.get("firmwareVersion", "")

        # Try to get from metadata if available
        if hasattr(interface, "metadata") and interface.metadata:
            device_info["firmware_version"] = getattr(
                interface.metadata, "firmware_version", ""
            )

        logger.info(
            f"Identified device on {port}: {device_info.get('hardware_model_name', 'Unknown')} "
            f"({device_info.get('short_name', '')})"
        )

        return device_info

    except Exception as e:
        logger.warning(f"Failed to probe device on {port}: {e}")
        return None

    finally:
        if interface:
            try:
                interface.close()
            except Exception:
                pass


class SerialPublisher:
    """
    Serial client for sending Meshtastic admin commands.

    This class handles:
    - Connection to a Meshtastic node via USB/Serial
    - Sending admin commands
    - Receiving responses

    Note: Multiple instances are now supported for multi-connection scenarios.
    Use get_serial_publisher() for the default singleton instance.
    """

    def __init__(self, connection_id: str = "default") -> None:
        """
        Initialize the Serial publisher.

        Args:
            connection_id: Unique identifier for this connection instance
        """
        self._connection_id = connection_id
        self._config = get_config()
        self._interface: SerialInterface | None = None
        self._connected = False
        self._connect_lock = threading.Lock()
        self._serial_port: str | None = None

        # Track pending responses
        self._pending_responses: dict[int, dict[str, Any]] = {}
        self._response_lock = threading.Lock()
        self._response_events: dict[int, threading.Event] = {}

        # General admin response tracking
        self._last_admin_response: dict[str, Any] | None = None
        self._admin_response_event = threading.Event()

    @property
    def is_connected(self) -> bool:
        """Check if connected to the node."""
        return self._connected and self._interface is not None

    @property
    def serial_port(self) -> str | None:
        """Get the current serial port."""
        return self._serial_port

    def set_serial_port(self, port: str | None) -> None:
        """
        Set the serial port to connect to.

        Args:
            port: Serial port device path (e.g., /dev/ttyUSB0, COM3)
        """
        # Disconnect if currently connected with different port
        if self._connected and port != self._serial_port:
            self.disconnect()
        self._serial_port = port

    def connect(self, port: str | None = None) -> bool:
        """
        Connect to a Meshtastic node via serial.

        Args:
            port: Serial port to connect to (optional, uses stored port if not provided)

        Returns:
            True if connected successfully
        """
        if port:
            self._serial_port = port

        if not self._serial_port:
            logger.error("No serial port specified")
            return False

        with self._connect_lock:
            if self._connected:
                logger.info(f"Already connected to {self._serial_port}")
                return True

            try:
                logger.info(f"Connecting to Meshtastic node via {self._serial_port}...")

                # Check if port exists first
                import os

                if not os.path.exists(self._serial_port):
                    logger.error(f"Serial port {self._serial_port} does not exist")
                    return False

                # Subscribe to receive messages before connecting
                pub.subscribe(self._on_receive, "meshtastic.receive")

                # Use a timeout for connection
                self._interface = SerialInterface(devPath=self._serial_port)

                # Wait a bit for connection to stabilize (with timeout)
                start_time = time.time()
                timeout = 10.0
                while time.time() - start_time < timeout:
                    if self._interface and self._interface.localNode:
                        break
                    time.sleep(0.2)

                if self._interface and self._interface.localNode:
                    self._connected = True
                    logger.info(
                        f"Connected to Meshtastic node via {self._serial_port}, "
                        f"local node: !{self._interface.localNode.nodeNum:08x}"
                    )
                    return True
                else:
                    logger.error(
                        "Failed to establish connection - no local node (timeout)"
                    )
                    self._cleanup_connection()
                    return False

            except PermissionError:
                logger.error(
                    f"Permission denied accessing {self._serial_port}. "
                    f"Try: sudo chmod 666 {self._serial_port} or add user to dialout group"
                )
                self._cleanup_connection()
                return False
            except Exception as e:
                error_msg = str(e)
                if "Timed out" in error_msg:
                    logger.error(
                        f"Connection to {self._serial_port} timed out. "
                        "The device may be busy, unresponsive, or not a Meshtastic device. "
                        "Try unplugging and replugging the device."
                    )
                else:
                    logger.error(f"Failed to connect via serial: {e}")
                self._cleanup_connection()
                return False

    def disconnect(self) -> None:
        """Disconnect from the serial port."""
        with self._connect_lock:
            self._cleanup_connection()
            logger.info("Disconnected from serial port")

    def _cleanup_connection(self) -> None:
        """Clean up the connection resources."""
        try:
            pub.unsubscribe(self._on_receive, "meshtastic.receive")
        except Exception:
            pass

        if self._interface:
            try:
                # Close the interface - this releases the serial port
                self._interface.close()
                logger.debug("Serial interface closed")
            except Exception as e:
                logger.warning(f"Error closing serial interface: {e}")
            finally:
                # Ensure we clear the reference even if close() fails
                self._interface = None

        self._interface = None
        self._connected = False

    def _on_receive(self, packet: dict[str, Any], interface: Any = None) -> None:
        """
        Handle received packets from the Meshtastic node.

        Args:
            packet: The received packet
            interface: The interface that received the packet (optional)
        """
        try:
            # Only process packets from our interface
            if interface is not None and interface != self._interface:
                return

            port_num = packet.get("decoded", {}).get("portnum")

            # Handle admin responses
            if port_num == "ADMIN_APP" or port_num == portnums_pb2.ADMIN_APP:
                self._handle_admin_response(packet)

        except Exception as e:
            logger.error(f"Error handling received packet: {e}")

    def _handle_admin_response(self, packet: dict[str, Any]) -> None:
        """Handle admin app responses."""
        try:
            decoded = packet.get("decoded", {})
            request_id = decoded.get("requestId", 0)

            admin_message = None
            if "admin" in decoded:
                admin_message = decoded["admin"]

            response_data = {
                "packet": packet,
                "admin_message": admin_message,
                "from_node": packet.get("fromId"),
                "received_at": time.time(),
            }

            # Check if this is a response to a specific request
            with self._response_lock:
                if request_id in self._pending_responses:
                    self._pending_responses[request_id] = response_data
                    if request_id in self._response_events:
                        self._response_events[request_id].set()
                else:
                    # Store as general admin response
                    self._last_admin_response = response_data
                    self._admin_response_event.set()

            logger.debug(f"Received admin response, request_id={request_id}")

        except Exception as e:
            logger.error(f"Error handling admin response: {e}")

    def get_local_node_id(self) -> int | None:
        """Get the local node ID from the connected device."""
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

    def get_response(
        self, request_id: int | None = None, timeout: float = 30.0
    ) -> dict[str, Any] | None:
        """
        Wait for and get a response.

        Args:
            request_id: Specific request ID to wait for (None for any admin response)
            timeout: Timeout in seconds

        Returns:
            Response data if received, None if timeout
        """
        if request_id:
            # Wait for specific request
            with self._response_lock:
                if request_id not in self._response_events:
                    self._response_events[request_id] = threading.Event()
                    self._pending_responses[request_id] = {}

            event = self._response_events[request_id]
            if event.wait(timeout=timeout):
                with self._response_lock:
                    response = self._pending_responses.pop(request_id, None)
                    self._response_events.pop(request_id, None)
                return response
            else:
                # Timeout - clean up
                with self._response_lock:
                    self._pending_responses.pop(request_id, None)
                    self._response_events.pop(request_id, None)
                return None
        else:
            # Wait for any admin response
            self._admin_response_event.clear()
            if self._admin_response_event.wait(timeout=timeout):
                response = self._last_admin_response
                self._last_admin_response = None
                return response
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
            target_node_id: The target node ID
            admin_message: The admin message to send
            want_response: Whether to request a response

        Returns:
            Packet ID if sent successfully
        """
        if not self.connect():
            logger.error("Cannot send admin message: not connected")
            return None

        if self._interface is None:
            return None

        try:
            packet_id = random.getrandbits(32)

            # Register for response
            with self._response_lock:
                self._response_events[packet_id] = threading.Event()
                self._pending_responses[packet_id] = {}

            # Send the admin message
            self._interface.sendData(
                data=admin_message.SerializeToString(),
                destinationId=target_node_id,
                portNum=portnums_pb2.ADMIN_APP,
                wantResponse=want_response,
                channelIndex=0,
            )

            logger.info(
                f"Sent admin message to !{target_node_id:08x}, packet_id={packet_id}"
            )
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
        if not self.connect():
            logger.error("Cannot send reboot: not connected to node")
            return None

        if self._interface is None:
            return None

        try:
            node_id_str = f"!{target_node_id:08x}"
            node = self._interface.getNode(node_id_str, requestChannels=False)

            if node is None:
                logger.error(f"Could not get node object for !{target_node_id:08x}")
                return None

            logger.info(f"Sending reboot command to !{target_node_id:08x} via serial")
            result = node.reboot(secs=seconds)

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
            node_id_str = f"!{target_node_id:08x}"
            node = self._interface.getNode(node_id_str, requestChannels=False)

            if node is None:
                logger.error(f"Could not get node object for !{target_node_id:08x}")
                return None

            logger.info(f"Sending shutdown command to !{target_node_id:08x} via serial")
            result = node.shutdown(secs=seconds)

            packet_id = random.getrandbits(32)

            if result:
                logger.info(
                    f"Shutdown command sent to !{target_node_id:08x}, "
                    f"delay={seconds}s, packet_id={packet_id}"
                )
                return packet_id
            else:
                logger.warning(
                    f"Shutdown command may have been sent to !{target_node_id:08x}"
                )
                return packet_id

        except Exception as e:
            logger.error(f"Failed to send shutdown command: {e}")
            return None


# Global serial publisher instance for backward compatibility
_serial_publisher: SerialPublisher | None = None


def get_serial_publisher() -> SerialPublisher:
    """
    Get the default global serial publisher instance.

    This is maintained for backward compatibility with existing code.
    For multi-connection scenarios, create SerialPublisher instances directly
    and register them with the ConnectionManager.
    """
    global _serial_publisher
    if _serial_publisher is None:
        _serial_publisher = SerialPublisher(connection_id="default_serial")
    return _serial_publisher
