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
    """

    _instance: "SerialPublisher | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "SerialPublisher":
        """Singleton pattern to ensure only one Serial publisher exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the Serial publisher."""
        if self._initialized:
            return

        self._initialized = True
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

        # Pending telemetry requests tracking
        # Key: target_node_id (int), Value: dict with event and response data
        self._pending_telemetry_requests: dict[int, dict[str, Any]] = {}
        self._pending_telemetry_lock = threading.Lock()

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

            # Handle telemetry responses
            if port_num == "TELEMETRY_APP" or port_num == portnums_pb2.TELEMETRY_APP:
                self._handle_telemetry_response(packet)

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

    def _handle_telemetry_response(self, packet: dict[str, Any]) -> None:
        """Handle telemetry app responses."""
        try:
            from_id = packet.get("from")
            if from_id is None:
                from_id_str = packet.get("fromId", "")
                if from_id_str.startswith("!"):
                    from_id = int(from_id_str[1:], 16)
                else:
                    return

            decoded = packet.get("decoded", {})
            telemetry_data = decoded.get("telemetry", {})

            if not telemetry_data:
                return

            # Check if this is a response to a pending request
            with self._pending_telemetry_lock:
                if from_id in self._pending_telemetry_requests:
                    pending = self._pending_telemetry_requests[from_id]
                    pending["response_data"]["telemetry"] = telemetry_data
                    pending["response_data"]["timestamp"] = time.time()
                    pending["response_data"]["from_node"] = from_id
                    pending["event"].set()

                    logger.debug(f"Received telemetry response from !{from_id:08x}")

        except Exception as e:
            logger.error(f"Error handling telemetry response: {e}")

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

    def send_begin_edit_settings(
        self,
        target_node_id: int,
    ) -> int | None:
        """
        Begin a settings edit transaction on a target node.

        This should be called before making multiple config changes.
        The node will hold changes in memory until commit_edit_settings is called.

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
        config = config_pb2.Config()

        if config_type == "device":
            for key, value in config_data.items():
                if hasattr(config.device, key):
                    setattr(config.device, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "position":
            for key, value in config_data.items():
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
                    if key == "mode" and not isinstance(value, int):
                        try:
                            value = int(value)
                        except (ValueError, TypeError):
                            continue
                    if key == "fixed_pin" and not isinstance(value, int):
                        try:
                            value = int(value)
                        except (ValueError, TypeError):
                            continue
                    setattr(config.bluetooth, key, value)
            admin_msg.set_config.CopyFrom(config)

        elif config_type == "security":
            # Filter out forbidden fields
            FORBIDDEN_SECURITY_FIELDS = {"private_key", "public_key"}
            config_data = {
                k: v
                for k, v in config_data.items()
                if k not in FORBIDDEN_SECURITY_FIELDS
            }

            for key, value in config_data.items():
                if hasattr(config.security, key):
                    field = getattr(config.security, key)
                    if hasattr(field, "extend"):
                        field.clear()
                        if isinstance(value, (list, tuple)):
                            for item in value:
                                if isinstance(item, bytes):
                                    field.append(item)
                                elif isinstance(item, str):
                                    try:
                                        field.append(bytes.fromhex(item))
                                    except Exception:
                                        import base64

                                        try:
                                            field.append(base64.b64decode(item))
                                        except Exception:
                                            pass
                    else:
                        setattr(config.security, key, value)

            if not config_data:
                return None
            admin_msg.set_config.CopyFrom(config)

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

        module_map = {
            "mqtt": "mqtt",
            "serial": "serial",
            "extnotif": "external_notification",
            "storeforward": "store_forward",
            "rangetest": "range_test",
            "telemetry": "telemetry",
            "cannedmsg": "canned_message",
            "audio": "audio",
            "remotehardware": "remote_hardware",
            "neighborinfo": "neighbor_info",
            "ambientlighting": "ambient_lighting",
            "detectionsensor": "detection_sensor",
            "paxcounter": "paxcounter",
        }

        if module_type_lower not in module_map:
            logger.error(f"Unknown module type: {module_type}")
            return None

        module_attr = module_map[module_type_lower]
        module_obj = getattr(module_config, module_attr)

        for key, value in module_data.items():
            if hasattr(module_obj, key):
                setattr(module_obj, key, value)

        admin_msg.set_module_config.CopyFrom(module_config)

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
        import base64

        from meshtastic import channel_pb2

        admin_msg = admin_pb2.AdminMessage()

        channel = channel_pb2.Channel()
        channel.index = channel_index

        if "role" in channel_data:
            channel.role = channel_data["role"]

        if "name" in channel_data:
            channel.settings.name = channel_data["name"]

        if "psk" in channel_data:
            psk = channel_data["psk"]
            if isinstance(psk, bytes):
                channel.settings.psk = psk
            elif isinstance(psk, str):
                try:
                    decoded = base64.b64decode(psk)
                    channel.settings.psk = decoded
                except Exception:
                    try:
                        decoded = bytes.fromhex(psk)
                        channel.settings.psk = decoded
                    except Exception:
                        channel.settings.psk = psk.encode("utf-8")

        if "position_precision" in channel_data:
            channel.settings.module_settings.position_precision = channel_data[
                "position_precision"
            ]

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
        from meshtastic import mesh_pb2

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

    def send_remove_node(
        self,
        target_node_id: int,
        node_to_remove: int,
    ) -> int | None:
        """
        Remove a node from the target node's nodedb.

        Args:
            target_node_id: The node to send the command to
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

    def send_nodedb_reset(
        self,
        target_node_id: int,
    ) -> int | None:
        """
        Reset the nodedb on the target node.

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
            config_only: If True, only reset config. If False, full factory reset.

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
    ) -> dict[str, Any] | None:
        """
        Request telemetry from a target node and wait for response.

        This sends a telemetry request to the target node and waits for
        the response containing current device metrics.

        Args:
            target_node_id: The destination node ID
            telemetry_type: Type of telemetry to request
                           (device_metrics, environment_metrics, etc.)
            timeout: Timeout in seconds to wait for response (default 25s for mesh)

        Returns:
            Dictionary with telemetry data if successful, None otherwise
        """
        from meshtastic import telemetry_pb2

        if not self.is_connected:
            logger.error("Cannot send telemetry request: not connected")
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

            # Set up tracking for this request
            response_event = threading.Event()
            response_data: dict[str, Any] = {}

            with self._pending_telemetry_lock:
                self._pending_telemetry_requests[target_node_id] = {
                    "event": response_event,
                    "response_data": response_data,
                    "telemetry_type": telemetry_type,
                    "requested_at": time.time(),
                }

            # Track statistics
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
                logger.info(
                    f"Sending telemetry request to !{target_node_id:08x} "
                    f"(type={telemetry_type}) via serial"
                )

                # Send telemetry request
                self._interface.sendData(
                    data=telemetry,
                    destinationId=target_node_id,
                    portNum=portnums_pb2.TELEMETRY_APP,
                    wantAck=False,
                    wantResponse=True,
                )

                # Wait for response
                if response_event.wait(timeout=timeout):
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

                    logger.info(
                        f"Telemetry request successful for !{target_node_id:08x}"
                    )
                    # Track success
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

                    # Include stats in response
                    response_data["stats"] = self.get_telemetry_stats(target_node_id)
                    return response_data
                else:
                    logger.warning(
                        f"Telemetry request timeout for !{target_node_id:08x}"
                    )
                    with self._telemetry_stats_lock:
                        self._telemetry_stats["timeouts"] += 1
                        self._telemetry_stats["per_node_stats"][node_key][
                            "timeouts"
                        ] += 1
                    return None

            finally:
                # Always clean up the pending request
                with self._pending_telemetry_lock:
                    self._pending_telemetry_requests.pop(target_node_id, None)

        except Exception as e:
            logger.error(f"Failed to send telemetry request: {e}")
            with self._telemetry_stats_lock:
                self._telemetry_stats["errors"] += 1
            return None

    def get_telemetry_stats(self, node_id: int | None = None) -> dict[str, Any]:
        """
        Get telemetry request statistics.

        Args:
            node_id: Optional node ID to get per-node stats for

        Returns:
            Dictionary with telemetry statistics
        """
        with self._telemetry_stats_lock:
            if node_id is not None:
                node_key = str(node_id)
                node_stats = self._telemetry_stats["per_node_stats"].get(node_key, {})
                return {
                    "node_id": node_id,
                    "requests": node_stats.get("requests", 0),
                    "successes": node_stats.get("successes", 0),
                    "timeouts": node_stats.get("timeouts", 0),
                    "errors": node_stats.get("errors", 0),
                    "success_rate": (
                        node_stats.get("successes", 0)
                        / max(node_stats.get("requests", 0), 1)
                        * 100
                    ),
                    "last_request": node_stats.get("last_request"),
                    "last_success": node_stats.get("last_success"),
                }
            else:
                return {
                    "total_requests": self._telemetry_stats["total_requests"],
                    "successful_responses": self._telemetry_stats[
                        "successful_responses"
                    ],
                    "timeouts": self._telemetry_stats["timeouts"],
                    "errors": self._telemetry_stats["errors"],
                    "success_rate": (
                        self._telemetry_stats["successful_responses"]
                        / max(self._telemetry_stats["total_requests"], 1)
                        * 100
                    ),
                    "last_request_time": self._telemetry_stats["last_request_time"],
                    "last_success_time": self._telemetry_stats["last_success_time"],
                }

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


# Global serial publisher instance
_serial_publisher: SerialPublisher | None = None


def get_serial_publisher() -> SerialPublisher:
    """Get the global serial publisher instance."""
    global _serial_publisher
    if _serial_publisher is None:
        _serial_publisher = SerialPublisher()
    return _serial_publisher
