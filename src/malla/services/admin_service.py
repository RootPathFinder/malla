"""
Admin service for remote node administration.

This service provides high-level operations for administering Meshtastic nodes
remotely via MQTT, TCP, or serial connection.
"""

import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from meshtastic.protobuf import config_pb2, mesh_pb2, module_config_pb2

from ..config import get_config
from ..database.admin_repository import AdminRepository
from .config_metadata import get_all_config_schemas, get_config_schema
from .mqtt_publisher import get_mqtt_publisher
from .serial_publisher import get_serial_publisher
from .tcp_publisher import get_tcp_publisher

logger = logging.getLogger(__name__)


def get_hardware_model_name(hw_model_value: int | None) -> str | None:
    """Convert hardware model enum value to human-readable name."""
    if hw_model_value is None:
        return None

    try:
        # Get the enum name from the value - cast to the enum type
        hw_enum = mesh_pb2.HardwareModel.ValueType(hw_model_value)
        enum_name = mesh_pb2.HardwareModel.Name(hw_enum)

        # Map to friendly display names
        display_name_map: dict[str, str | None] = {
            "UNSET": None,
            "TLORA_V2": "T-LoRa V2",
            "TLORA_V1": "T-LoRa V1",
            "TLORA_V2_1_1P6": "T-LoRa V2.1.1.6",
            "TLORA_V2_1_1P8": "T-LoRa V2.1.1.8",
            "TLORA_T3_S3": "T-LoRa T3 S3",
            "TBEAM": "T-Beam",
            "TBEAM_V0P7": "T-Beam V0.7",
            "T_ECHO": "T-Echo",
            "T_DECK": "T-Deck",
            "T_WATCH_S3": "T-Watch S3",
            "HELTEC_V1": "Heltec V1",
            "HELTEC_V2_0": "Heltec V2.0",
            "HELTEC_V2_1": "Heltec V2.1",
            "HELTEC_V3": "Heltec V3",
            "HELTEC_WSL_V3": "Heltec WSL V3",
            "HELTEC_WIRELESS_PAPER": "Heltec Wireless Paper",
            "HELTEC_WIRELESS_TRACKER": "Heltec Wireless Tracker",
            "HELTEC_MESH_NODE_T114": "Heltec Mesh Node T114",
            "HELTEC_CAPSULE_SENSOR_V3": "Heltec Capsule Sensor V3",
            "HELTEC_VISION_MASTER_T190": "Heltec Vision Master T190",
            "HELTEC_VISION_MASTER_E213": "Heltec Vision Master E213",
            "HELTEC_VISION_MASTER_E290": "Heltec Vision Master E290",
            "HELTEC_MESH_POCKET": "Heltec Mesh Pocket",
            "RAK4631": "RAK4631",
            "RAK11200": "RAK11200",
            "RAK11310": "RAK11310",
            "RAK2560": "RAK2560",
            "LILYGO_TBEAM_S3_CORE": "LilyGO T-Beam S3 Core",
            "STATION_G1": "Station G1",
            "STATION_G2": "Station G2",
            "LORA_TYPE": "LoRa Type",
            "WIPHONE": "WiPhone",
            "WIO_WM1110": "Wio WM1110",
            "NANO_G1": "Nano G1",
            "NANO_G1_EXPLORER": "Nano G1 Explorer",
            "NANO_G2_ULTRA": "Nano G2 Ultra",
            "NRF52_PROMICRO_DIY": "nRF52 Pro Micro DIY",
            "NRF52840_PCA10059": "nRF52840 PCA10059",
            "NRF52840DK": "nRF52840 DK",
            "RPI_PICO": "Raspberry Pi Pico",
            "RPI_PICO2": "Raspberry Pi Pico 2",
            "SEEED_XIAO_S3": "Seeed XIAO S3",
            "M5STACK": "M5Stack",
            "M5STACK_COREINK": "M5Stack CoreInk",
            "M5STACK_CORES3": "M5Stack CoreS3",
            "PORTDUINO": "Portduino",
            "SENSECAP_INDICATOR": "SenseCAP Indicator",
            "TRACKER_T1000_E": "Tracker T1000-E",
            "CANARYONE": "CanaryOne",
            "RP2040_LORA": "RP2040 LoRa",
            "PPR": "PPR",
            "RADIOMASTER_900_BANDIT": "RadioMaster 900 Bandit",
            "RADIOMASTER_900_BANDIT_NANO": "RadioMaster 900 Bandit Nano",
            "PRIVATE_HW": "Private Hardware",
            "DIY_V1": "DIY V1",
            "XIAO_NRF52_KIT": "XIAO nRF52 Kit",
            "WISMESH_TAP": "WisMesh TAP",
            "THINKNODE_M1": "ThinkNode M1",
            "THINKNODE_M2": "ThinkNode M2",
            "CHATTER_2": "Chatter 2",
            "PICOMPUTER_S3": "PiComputer S3",
        }

        if enum_name in display_name_map:
            return display_name_map[enum_name]

        # Fallback: convert enum name to title case
        return enum_name.replace("_", " ").title()
    except ValueError:
        # Unknown enum value, return as-is
        return str(hw_model_value)


class AdminConnectionType(Enum):
    """Connection type for admin operations."""

    MQTT = "mqtt"
    TCP = "tcp"
    SERIAL = "serial"


class AdminPublisher(Protocol):
    """Protocol for admin publishers (MQTT, TCP, Serial)."""

    @property
    def is_connected(self) -> bool: ...

    def connect(self) -> bool: ...

    def get_response(
        self, packet_id: int, timeout: float = 30.0
    ) -> dict[str, Any] | None: ...

    def send_get_device_metadata(
        self,
        target_node_id: int,
        from_node_id: int | None = None,
    ) -> int | None: ...

    def send_get_config(
        self,
        target_node_id: int,
        from_node_id: int | None = None,
        config_type: int = 1,
    ) -> int | None: ...

    def send_get_module_config(
        self,
        target_node_id: int,
        from_node_id: int | None = None,
        module_config_type: int = 0,
    ) -> int | None: ...

    def send_get_channel(
        self,
        target_node_id: int,
        from_node_id: int | None = None,
        channel_index: int = 0,
    ) -> int | None: ...

    def send_reboot(
        self,
        target_node_id: int,
        from_node_id: int | None = None,
        seconds: int = 5,
    ) -> int | None: ...

    def send_shutdown(
        self,
        target_node_id: int,
        from_node_id: int | None = None,
        seconds: int = 5,
    ) -> int | None: ...

    def send_set_config(
        self,
        target_node_id: int,
        config_type: str,
        config_data: dict,
    ) -> int | None: ...

    def send_set_channel(
        self,
        target_node_id: int,
        channel_index: int,
        channel_data: dict,
    ) -> int | None: ...

    def send_begin_edit_settings(
        self,
        target_node_id: int,
    ) -> int | None: ...

    def send_commit_edit_settings(
        self,
        target_node_id: int,
    ) -> int | None: ...


class ConfigType(Enum):
    """Configuration types that can be requested.

    Values must match meshtastic.admin_pb2.AdminMessage.ConfigType enum:
    DEVICE_CONFIG = 0, POSITION_CONFIG = 1, POWER_CONFIG = 2, NETWORK_CONFIG = 3,
    DISPLAY_CONFIG = 4, LORA_CONFIG = 5, BLUETOOTH_CONFIG = 6, SECURITY_CONFIG = 7,
    SESSIONKEY_CONFIG = 8, DEVICEUI_CONFIG = 9
    """

    DEVICE = 0
    POSITION = 1
    POWER = 2
    NETWORK = 3
    DISPLAY = 4
    LORA = 5
    BLUETOOTH = 6
    SECURITY = 7


class ModuleConfigType(Enum):
    """Module configuration types that can be requested.

    Values must match meshtastic.admin_pb2.AdminMessage.ModuleConfigType enum.
    """

    MQTT = 0
    SERIAL = 1
    EXTNOTIF = 2
    STOREFORWARD = 3
    RANGETEST = 4
    TELEMETRY = 5
    CANNEDMSG = 6
    AUDIO = 7
    REMOTEHARDWARE = 8
    NEIGHBORINFO = 9
    AMBIENTLIGHTING = 10
    DETECTIONSENSOR = 11
    PAXCOUNTER = 12


@dataclass
class AdminCommandResult:
    """Result of an admin command."""

    success: bool
    packet_id: int | None = None
    log_id: int | None = None
    response: dict[str, Any] | None = None
    error: str | None = None
    attempts: int = 1
    retry_info: list[dict[str, Any]] | None = None


class AdminService:
    """
    Service for remote node administration.

    Provides high-level methods for common admin operations like
    getting configuration, rebooting nodes, etc.
    """

    # Default retry configuration
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 2.0  # seconds between retries
    DEFAULT_TIMEOUT = 30.0  # seconds to wait for response

    def __init__(self) -> None:
        """Initialize the admin service."""
        self._config = get_config()
        self._gateway_node_id: int | None = None
        self._connection_type: AdminConnectionType | None = None

    @property
    def connection_type(self) -> AdminConnectionType:
        """Get the configured connection type."""
        if self._connection_type is None:
            conn_type = getattr(self._config, "admin_connection_type", "mqtt")
            try:
                self._connection_type = AdminConnectionType(conn_type)
            except ValueError:
                logger.warning(
                    f"Invalid connection type '{conn_type}', defaulting to MQTT"
                )
                self._connection_type = AdminConnectionType.MQTT
        return self._connection_type

    @connection_type.setter
    def connection_type(self, value: AdminConnectionType) -> None:
        """Set the connection type."""
        self._connection_type = value
        logger.info(f"Admin connection type set to {value.value}")

    def set_connection_type(self, conn_type: str) -> bool:
        """
        Set the connection type from string.

        Args:
            conn_type: Connection type string ("mqtt", "tcp", or "serial")

        Returns:
            True if successful, False if invalid type
        """
        try:
            self._connection_type = AdminConnectionType(conn_type)
            logger.info(f"Admin connection type set to {conn_type}")
            return True
        except ValueError:
            logger.error(f"Invalid connection type: {conn_type}")
            return False

    def _get_publisher(self) -> Any:
        """
        Get the appropriate publisher based on connection type.

        Returns:
            MQTT, TCP, or Serial publisher instance
        """
        conn_type = self.connection_type

        if conn_type == AdminConnectionType.TCP:
            return get_tcp_publisher()
        elif conn_type == AdminConnectionType.SERIAL:
            return get_serial_publisher()
        else:
            return get_mqtt_publisher()

    def _send_with_retry(
        self,
        send_func: Any,
        response_parser: Any,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        timeout: float = 30.0,
        command_name: str = "command",
    ) -> tuple[bool, Any, list[dict[str, Any]]]:
        """
        Send a command with retry logic for unreliable nodes.

        Args:
            send_func: Function to call to send the command, returns packet_id
            response_parser: Function to call to parse the response
            max_retries: Maximum number of retry attempts
            retry_delay: Seconds to wait between retries
            timeout: Seconds to wait for each response
            command_name: Name of the command for logging

        Returns:
            Tuple of (success, response_data, retry_info)
        """
        publisher = self._get_publisher()
        retry_info: list[dict[str, Any]] = []

        for attempt in range(1, max_retries + 1):
            attempt_info = {
                "attempt": attempt,
                "max_attempts": max_retries,
                "timestamp": time.time(),
                "status": "pending",
            }

            # Send the command
            packet_id = send_func()

            if packet_id is None:
                attempt_info["status"] = "send_failed"
                attempt_info["error"] = "Failed to send message"
                retry_info.append(attempt_info)

                if attempt < max_retries:
                    logger.warning(
                        f"{command_name} attempt {attempt}/{max_retries} failed to send, "
                        f"retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                continue

            attempt_info["packet_id"] = packet_id

            # Wait for response
            response = publisher.get_response(packet_id, timeout=timeout)

            if response:
                # Parse the response
                parsed = response_parser(response)
                if parsed is not None:
                    attempt_info["status"] = "success"
                    retry_info.append(attempt_info)
                    logger.info(
                        f"{command_name} succeeded on attempt {attempt}/{max_retries}"
                    )
                    return True, parsed, retry_info

                attempt_info["status"] = "parse_failed"
                attempt_info["error"] = "Failed to parse response"
            else:
                attempt_info["status"] = "timeout"
                attempt_info["error"] = f"No response within {timeout}s"

            retry_info.append(attempt_info)

            if attempt < max_retries:
                logger.warning(
                    f"{command_name} attempt {attempt}/{max_retries} timed out, "
                    f"retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)

        logger.error(f"{command_name} failed after {max_retries} attempts")
        return False, None, retry_info

    @property
    def gateway_node_id(self) -> int | None:
        """Get the configured gateway node ID."""
        # For TCP connections, we can get the local node ID from the interface
        if self.connection_type == AdminConnectionType.TCP:
            publisher = get_tcp_publisher()
            if publisher.is_connected:
                local_id = publisher.get_local_node_id()
                if local_id:
                    return local_id

        # For Serial connections, get the local node ID from the interface
        if self.connection_type == AdminConnectionType.SERIAL:
            publisher = get_serial_publisher()
            if publisher.is_connected:
                local_id = publisher.get_local_node_id()
                if local_id:
                    return local_id

        if self._gateway_node_id is None:
            # Try to get from config
            gateway_id = getattr(self._config, "admin_gateway_node_id", None)
            if gateway_id:
                self._gateway_node_id = gateway_id
        return self._gateway_node_id

    @gateway_node_id.setter
    def gateway_node_id(self, value: int) -> None:
        """Set the gateway node ID."""
        self._gateway_node_id = value

    def set_gateway_node(self, node_id: int) -> None:
        """
        Set the gateway node to use for admin operations.

        Args:
            node_id: The node ID of the gateway node
        """
        self._gateway_node_id = node_id
        logger.info(f"Admin gateway node set to {node_id} (!{node_id:08x})")

    def is_enabled(self) -> bool:
        """Check if admin functionality is enabled."""
        return getattr(self._config, "admin_enabled", True)

    def get_connection_status(self) -> dict[str, Any]:
        """
        Get the current admin connection status.

        Returns:
            Dictionary with connection status info
        """
        conn_type = self.connection_type
        publisher = self._get_publisher()
        gateway_id = self.gateway_node_id

        # Get local node ID and name from the connected publisher
        local_node_id = None
        local_node_name = None
        if hasattr(publisher, "get_local_node_id"):
            local_node_id = publisher.get_local_node_id()
        if hasattr(publisher, "get_local_node_name"):
            local_node_name = publisher.get_local_node_name()

        status: dict[str, Any] = {
            "enabled": self.is_enabled(),
            "connection_type": conn_type.value,
            "connected": publisher.is_connected,
            "gateway_node_id": gateway_id,
            "gateway_node_hex": f"!{gateway_id:08x}" if gateway_id else None,
            "local_node_id": local_node_id,
            "local_node_hex": f"!{local_node_id:08x}" if local_node_id else None,
            "local_node_name": local_node_name,
        }

        # Add connection-specific details
        if conn_type == AdminConnectionType.TCP:
            tcp_publisher = get_tcp_publisher()
            status["tcp_host"] = tcp_publisher.tcp_host
            status["tcp_port"] = tcp_publisher.tcp_port
        elif conn_type == AdminConnectionType.SERIAL:
            serial_publisher = get_serial_publisher()
            status["serial_port"] = serial_publisher.serial_port
        elif conn_type == AdminConnectionType.MQTT:
            mqtt_publisher = get_mqtt_publisher()
            status["mqtt_connected"] = mqtt_publisher.is_connected

        return status

    def get_node_admin_status(self, target_node_id: int) -> dict[str, Any]:
        """
        Get the admin session status for a specific node.

        This checks whether the admin channel is ready to send commands to the node,
        including connection status, gateway configuration, and node administrability.

        Args:
            target_node_id: The target node ID to check

        Returns:
            Dictionary with admin readiness status and helpful messages
        """
        conn_type = self.connection_type
        publisher = self._get_publisher()
        gateway_id = self.gateway_node_id

        # Build status response
        status: dict[str, Any] = {
            "node_id": target_node_id,
            "hex_id": f"!{target_node_id:08x}",
            "ready": False,
            "connection_type": conn_type.value,
            "checks": [],
            "issues": [],
            "suggestions": [],
        }

        # Check 1: Is the node marked as administrable?
        node_details = AdminRepository.get_administrable_node_details(target_node_id)
        is_administrable = node_details is not None

        if is_administrable:
            status["checks"].append(
                {
                    "name": "node_administrable",
                    "passed": True,
                    "message": "Node has previously responded to admin requests",
                    "details": {
                        "last_confirmed": node_details.get("last_confirmed"),
                        "firmware_version": node_details.get("firmware_version"),
                    },
                }
            )
        else:
            status["checks"].append(
                {
                    "name": "node_administrable",
                    "passed": False,
                    "message": "Node has not been confirmed as administrable",
                }
            )
            status["issues"].append("Node has not responded to admin requests yet")
            status["suggestions"].append(
                "Use 'Test Node Admin Access' to verify the node can receive admin commands"
            )

        # Check 2: Is there a valid connection?
        is_connected = publisher.is_connected

        if is_connected:
            status["checks"].append(
                {
                    "name": "connection",
                    "passed": True,
                    "message": f"Connected via {conn_type.value.upper()}",
                }
            )
        else:
            status["checks"].append(
                {
                    "name": "connection",
                    "passed": False,
                    "message": f"Not connected via {conn_type.value.upper()}",
                }
            )
            status["issues"].append(
                f"{conn_type.value.upper()} connection is not established"
            )
            if conn_type == AdminConnectionType.TCP:
                status["suggestions"].append(
                    "Connect to a Meshtastic node via TCP in the Connection Status section"
                )
            else:
                status["suggestions"].append(
                    "Ensure the MQTT broker is connected and configured"
                )

        # Check 3: Is a gateway node configured?
        if gateway_id:
            status["checks"].append(
                {
                    "name": "gateway",
                    "passed": True,
                    "message": f"Gateway node configured: !{gateway_id:08x}",
                    "gateway_node_hex": f"!{gateway_id:08x}",
                }
            )

            # Check if target is the gateway itself
            if gateway_id == target_node_id:
                status["checks"].append(
                    {
                        "name": "target_is_gateway",
                        "passed": True,
                        "message": "Target node is the gateway node (local administration)",
                    }
                )
        else:
            status["checks"].append(
                {
                    "name": "gateway",
                    "passed": False,
                    "message": "No gateway node configured",
                }
            )
            if conn_type == AdminConnectionType.MQTT:
                status["issues"].append("No gateway node is configured for MQTT mode")
                status["suggestions"].append(
                    "Set a gateway node ID in the Connection Status section"
                )

        # Check 4: For TCP, check if admin channel exists on local node
        if conn_type == AdminConnectionType.TCP and is_connected:
            try:
                tcp_publisher = get_tcp_publisher()
                admin_channel_index = tcp_publisher._get_admin_channel_index()

                if admin_channel_index > 0:
                    status["checks"].append(
                        {
                            "name": "admin_channel",
                            "passed": True,
                            "message": f"Admin channel found at index {admin_channel_index}",
                            "channel_index": admin_channel_index,
                        }
                    )
                else:
                    # Channel 0 is used (primary channel), which is fine for PKI
                    status["checks"].append(
                        {
                            "name": "admin_channel",
                            "passed": True,
                            "message": "Using primary channel with PKI encryption",
                            "channel_index": 0,
                        }
                    )
            except Exception as e:
                logger.warning(f"Failed to check admin channel: {e}")

        # Determine overall readiness
        all_critical_passed = all(
            check["passed"]
            for check in status["checks"]
            if check["name"] in ("connection", "gateway")
        )
        status["ready"] = all_critical_passed and is_connected

        # Add overall status message
        if status["ready"]:
            if is_administrable:
                status["status_message"] = "Ready to send admin commands"
                status["status_level"] = "success"
            else:
                status["status_message"] = (
                    "Connection ready, but node has not been tested. "
                    "Commands may work if the node has your public key configured."
                )
                status["status_level"] = "warning"
        else:
            status["status_message"] = "Admin session is not ready"
            status["status_level"] = "danger"

        return status

    def test_node_admin(self, target_node_id: int) -> AdminCommandResult:
        """
        Test if a node is administrable by requesting device metadata.

        Args:
            target_node_id: The node ID to test

        Returns:
            AdminCommandResult with test results
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured. Set admin_gateway_node_id in config or connect via TCP.",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="test_admin",
            command_data=json.dumps(
                {
                    "action": "get_device_metadata",
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the request using appropriate publisher
        publisher = self._get_publisher()

        # TCP publisher doesn't need from_node_id (uses local node)
        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_get_device_metadata(
                target_node_id=target_node_id,
            )
        else:
            packet_id = publisher.send_get_device_metadata(
                target_node_id=target_node_id,
                from_node_id=gateway_id,
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            # Update status to reflect the failed check
            AdminRepository.update_node_status(target_node_id, "error")
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send admin message via {conn_type.value}",
            )

        # Wait for response
        response = publisher.get_response(packet_id, timeout=30.0)

        if response:
            # Extract firmware version and metadata from device metadata response
            firmware_version = None
            device_metadata = None
            hw_model = None
            hw_model_name = None
            role = None
            has_wifi = None
            has_bluetooth = None
            can_shutdown = None
            admin_msg = response.get("admin_message")

            if admin_msg and hasattr(admin_msg, "HasField"):
                try:
                    if admin_msg.HasField("get_device_metadata_response"):
                        meta = admin_msg.get_device_metadata_response
                        firmware_version = meta.firmware_version
                        hw_model = meta.hw_model
                        hw_model_name = get_hardware_model_name(hw_model)
                        role = meta.role
                        has_wifi = meta.hasWifi
                        has_bluetooth = meta.hasBluetooth
                        can_shutdown = meta.canShutdown
                        device_metadata = str(meta)
                        logger.info(
                            f"Got device metadata from node {target_node_id}: "
                            f"firmware={firmware_version}, hw_model={hw_model_name}"
                        )
                except Exception as e:
                    logger.warning(f"Failed to extract device metadata: {e}")

            # Mark the node as administrable since it responded
            AdminRepository.mark_node_administrable(
                node_id=target_node_id,
                firmware_version=firmware_version,
                device_metadata=device_metadata,
                admin_channel_index=0,  # Default channel
            )

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {
                        "from_node": response.get("from_node"),
                        "firmware_version": firmware_version,
                        "hw_model": hw_model_name,
                        "role": role,
                    }
                ),
            )
            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "from_node": response.get("from_node"),
                    "administrable": True,
                    "firmware_version": firmware_version,
                    "hw_model": hw_model_name,
                    "role": role,
                    "has_wifi": has_wifi,
                    "has_bluetooth": has_bluetooth,
                    "can_shutdown": can_shutdown,
                },
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message="No response received within timeout",
            )
            # Update status to reflect the timeout
            AdminRepository.update_node_status(target_node_id, "timeout")
            return AdminCommandResult(
                success=False,
                packet_id=packet_id,
                log_id=log_id,
                error="No response received (timeout). Node may not have this server's public key configured.",
            )

    def get_config(
        self,
        target_node_id: int,
        config_type: ConfigType,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        timeout: float = 30.0,
    ) -> AdminCommandResult:
        """
        Request configuration from a remote node with retry support.

        Args:
            target_node_id: The target node ID
            config_type: The type of configuration to request
            max_retries: Maximum number of retry attempts (default 3)
            retry_delay: Seconds to wait between retries (default 2.0)
            timeout: Seconds to wait for each response (default 30.0)

        Returns:
            AdminCommandResult with config data and retry info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type
        publisher = self._get_publisher()

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="get_config",
            command_data=json.dumps(
                {
                    "config_type": config_type.name,
                    "connection_type": conn_type.value,
                    "max_retries": max_retries,
                }
            ),
        )

        # Define send function for retry helper
        def send_func() -> int | None:
            if conn_type == AdminConnectionType.TCP:
                return publisher.send_get_config(
                    target_node_id=target_node_id,
                    config_type=config_type.value,
                )
            else:
                return publisher.send_get_config(
                    target_node_id=target_node_id,
                    from_node_id=gateway_id,
                    config_type=config_type.value,
                )

        # Define response parser
        def parse_response(response: dict[str, Any]) -> dict[str, Any] | None:
            admin_msg = response.get("admin_message")
            if admin_msg and admin_msg.HasField("get_config_response"):
                config = admin_msg.get_config_response
                return self._config_to_dict(config)
            return None

        # Send with retry
        success, config_data, retry_info = self._send_with_retry(
            send_func=send_func,
            response_parser=parse_response,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            command_name=f"get_config({config_type.name})",
        )

        attempts = len(retry_info)

        if success and config_data:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {
                        "config": config_data,
                        "attempts": attempts,
                    }
                ),
            )

            return AdminCommandResult(
                success=True,
                log_id=log_id,
                response=config_data,
                attempts=attempts,
                retry_info=retry_info,
            )
        else:
            last_error = "No response received"
            if retry_info:
                last_attempt = retry_info[-1]
                last_error = last_attempt.get("error", "Unknown error")

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message=f"Failed after {attempts} attempts: {last_error}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"No response after {attempts} attempts (timeout)",
                attempts=attempts,
                retry_info=retry_info,
            )

    def get_module_config(
        self,
        target_node_id: int,
        module_config_type: ModuleConfigType,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        timeout: float = 30.0,
    ) -> AdminCommandResult:
        """
        Request module configuration from a remote node with retry support.

        Args:
            target_node_id: The target node ID
            module_config_type: The type of module configuration to request
            max_retries: Maximum number of retry attempts (default 3)
            retry_delay: Seconds to wait between retries (default 2.0)
            timeout: Seconds to wait for each response (default 30.0)

        Returns:
            AdminCommandResult with module config data and retry info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type
        publisher = self._get_publisher()

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="get_module_config",
            command_data=json.dumps(
                {
                    "module_config_type": module_config_type.name,
                    "connection_type": conn_type.value,
                    "max_retries": max_retries,
                }
            ),
        )

        # Define send function for retry helper
        def send_func() -> int | None:
            if conn_type == AdminConnectionType.TCP:
                return publisher.send_get_module_config(
                    target_node_id=target_node_id,
                    module_config_type=module_config_type.value,
                )
            else:
                return publisher.send_get_module_config(
                    target_node_id=target_node_id,
                    from_node_id=gateway_id,
                    module_config_type=module_config_type.value,
                )

        # Define response parser
        def parse_response(response: dict[str, Any]) -> dict[str, Any] | None:
            admin_msg = response.get("admin_message")
            if admin_msg and admin_msg.HasField("get_module_config_response"):
                module_config = admin_msg.get_module_config_response
                return self._module_config_to_dict(module_config)
            return None

        # Send with retry
        success, config_data, retry_info = self._send_with_retry(
            send_func=send_func,
            response_parser=parse_response,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            command_name=f"get_module_config({module_config_type.name})",
        )

        attempts = len(retry_info)

        if success and config_data:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {
                        "module_config": config_data,
                        "attempts": attempts,
                    }
                ),
            )

            return AdminCommandResult(
                success=True,
                log_id=log_id,
                response=config_data,
                attempts=attempts,
                retry_info=retry_info,
            )
        else:
            last_error = "No response received"
            if retry_info:
                last_attempt = retry_info[-1]
                last_error = last_attempt.get("error", "Unknown error")

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message=f"Failed after {attempts} attempts: {last_error}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"No response after {attempts} attempts (timeout)",
                attempts=attempts,
                retry_info=retry_info,
            )

    def get_channel(
        self,
        target_node_id: int,
        channel_index: int,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        timeout: float = 30.0,
    ) -> AdminCommandResult:
        """
        Request channel configuration from a remote node with retry support.

        Args:
            target_node_id: The target node ID
            channel_index: The channel index (0-7)
            max_retries: Maximum number of retry attempts (default 3)
            retry_delay: Seconds to wait between retries (default 2.0)
            timeout: Seconds to wait for each response (default 30.0)

        Returns:
            AdminCommandResult with channel data and retry info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type
        publisher = self._get_publisher()

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="get_channel",
            command_data=json.dumps(
                {
                    "channel_index": channel_index,
                    "connection_type": conn_type.value,
                    "max_retries": max_retries,
                }
            ),
        )

        # Define send function for retry helper
        def send_func() -> int | None:
            if conn_type == AdminConnectionType.TCP:
                return publisher.send_get_channel(
                    target_node_id=target_node_id,
                    channel_index=channel_index,
                )
            else:
                return publisher.send_get_channel(
                    target_node_id=target_node_id,
                    from_node_id=gateway_id,
                    channel_index=channel_index,
                )

        # Define response parser
        def parse_response(response: dict[str, Any]) -> dict[str, Any] | None:
            admin_msg = response.get("admin_message")
            if admin_msg and admin_msg.HasField("get_channel_response"):
                channel = admin_msg.get_channel_response
                return {
                    "index": channel.index,
                    "role": channel.role,
                    "settings": {
                        "name": channel.settings.name,
                        "psk": channel.settings.psk.hex()
                        if channel.settings.psk
                        else None,
                        "module_settings": {
                            "position_precision": channel.settings.module_settings.position_precision,
                        },
                    },
                }
            return None

        # Send with retry
        success, channel_data, retry_info = self._send_with_retry(
            send_func=send_func,
            response_parser=parse_response,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            command_name=f"get_channel({channel_index})",
        )

        attempts = len(retry_info)

        if success and channel_data:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {
                        "channel": channel_data,
                        "attempts": attempts,
                    }
                ),
            )

            return AdminCommandResult(
                success=True,
                log_id=log_id,
                response=channel_data,
                attempts=attempts,
                retry_info=retry_info,
            )
        else:
            last_error = "No response received"
            if retry_info:
                last_attempt = retry_info[-1]
                last_error = last_attempt.get("error", "Unknown error")

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message=f"Failed after {attempts} attempts: {last_error}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"No response after {attempts} attempts (timeout)",
                attempts=attempts,
                retry_info=retry_info,
            )

    def reboot_node(
        self,
        target_node_id: int,
        delay_seconds: int = 5,
    ) -> AdminCommandResult:
        """
        Reboot a remote node.

        Args:
            target_node_id: The target node ID
            delay_seconds: Seconds to wait before rebooting

        Returns:
            AdminCommandResult
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Gather diagnostic info
        diag_info = {
            "delay_seconds": delay_seconds,
            "connection_type": conn_type.value,
            "gateway_node_id": f"!{gateway_id:08x}",
            "target_node_id": f"!{target_node_id:08x}",
        }

        # Add TCP-specific diagnostics
        if conn_type == AdminConnectionType.TCP:
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            diag_info["tcp_host"] = publisher.tcp_host
            diag_info["tcp_port"] = publisher.tcp_port
            diag_info["tcp_connected"] = publisher.is_connected
            diag_info["method"] = "meshtastic_library_node_reboot"
            if publisher._interface and publisher._interface.localNode:
                local_node = publisher._interface.localNode
                diag_info["local_node_id"] = f"!{local_node.nodeNum:08x}"
                # Check if target is local node
                if local_node.nodeNum == target_node_id:
                    diag_info["is_local_node"] = True
                else:
                    diag_info["is_local_node"] = False
                    diag_info["session_key_note"] = (
                        "Session key will be negotiated automatically by library"
                    )

        # Log the command with diagnostic info
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="reboot",
            command_data=json.dumps(diag_info),
        )

        # Send the command using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_reboot(
                target_node_id=target_node_id,
                seconds=delay_seconds,
            )
        else:
            packet_id = publisher.send_reboot(
                target_node_id=target_node_id,
                from_node_id=gateway_id,
                delay_seconds=delay_seconds,
            )

        if packet_id is None:
            error_msg = f"Failed to send message via {conn_type.value}. "
            if conn_type == AdminConnectionType.TCP:
                error_msg += "Check TCP connection to the gateway node."
            else:
                error_msg += "Check MQTT connection and gateway configuration."

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=error_msg,
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=error_msg,
            )

        # Reboot doesn't send a response - we can only confirm the command was sent
        # The actual reboot success can only be verified by observing the node going offline/online
        response_info = {
            "message": f"Reboot command sent to !{target_node_id:08x}",
            "packet_id": packet_id,
            "delay_seconds": delay_seconds,
            "note": "Reboot commands do not receive acknowledgment. "
            "Verify by checking if the node goes offline and comes back online.",
        }

        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps(response_info),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={
                "message": f"Reboot command sent. Node will reboot in {delay_seconds} seconds."
            },
        )

    def shutdown_node(
        self,
        target_node_id: int,
        delay_seconds: int = 5,
    ) -> AdminCommandResult:
        """
        Shutdown a remote node.

        Args:
            target_node_id: The target node ID
            delay_seconds: Seconds to wait before shutdown

        Returns:
            AdminCommandResult
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="shutdown",
            command_data=json.dumps(
                {
                    "delay_seconds": delay_seconds,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the command using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_shutdown(
                target_node_id=target_node_id,
                seconds=delay_seconds,
            )
        else:
            packet_id = publisher.send_shutdown(
                target_node_id=target_node_id,
                from_node_id=gateway_id,
                delay_seconds=delay_seconds,
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send shutdown command via {conn_type.value}",
            )

        # Shutdown doesn't send a response, mark as success
        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps(
                {
                    "message": f"Shutdown command sent, node will shutdown in {delay_seconds}s"
                }
            ),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={
                "message": f"Shutdown command sent. Node will shutdown in {delay_seconds} seconds."
            },
        )

    def remove_node(
        self,
        target_node_id: int,
        node_to_remove: int,
    ) -> AdminCommandResult:
        """
        Remove a node from the target node's nodedb.

        This instructs the target node to remove the specified node from
        its local node database. Useful for cleaning up stale entries.

        Args:
            target_node_id: The node to send the command to
            node_to_remove: The node number to remove from the nodedb

        Returns:
            AdminCommandResult
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="remove_node",
            command_data=json.dumps(
                {
                    "node_to_remove": f"!{node_to_remove:08x}",
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the command using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_remove_node(
                target_node_id=target_node_id,
                node_to_remove=node_to_remove,
            )
        else:
            # Serial connection not yet supported for this command
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="remove_node not supported via serial connection yet",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="remove_node not supported via serial connection yet",
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send remove_node command via {conn_type.value}",
            )

        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps(
                {"message": f"Remove node command sent for !{node_to_remove:08x}"}
            ),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={
                "message": f"Node !{node_to_remove:08x} removal requested from !{target_node_id:08x}"
            },
        )

    def reset_nodedb(
        self,
        target_node_id: int,
    ) -> AdminCommandResult:
        """
        Reset the nodedb on the target node.

        This clears all nodes from the target's database except itself.
        The node will need to rediscover other nodes on the mesh.

        Args:
            target_node_id: The node to reset the nodedb on

        Returns:
            AdminCommandResult
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="nodedb_reset",
            command_data=json.dumps(
                {
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the command using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_nodedb_reset(
                target_node_id=target_node_id,
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="nodedb_reset not supported via serial connection yet",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="nodedb_reset not supported via serial connection yet",
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send nodedb_reset command via {conn_type.value}",
            )

        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps({"message": "NodeDB reset command sent"}),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={
                "message": f"NodeDB reset command sent to !{target_node_id:08x}. Node will rediscover mesh."
            },
        )

    def factory_reset(
        self,
        target_node_id: int,
        config_only: bool = True,
    ) -> AdminCommandResult:
        """
        Factory reset the target node.

        Args:
            target_node_id: The node to reset
            config_only: If True, only reset config (preserves nodedb).
                        If False, full factory reset (everything).

        Returns:
            AdminCommandResult
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type
        reset_type = "factory_reset_config" if config_only else "factory_reset_device"

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type=reset_type,
            command_data=json.dumps(
                {
                    "config_only": config_only,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the command using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_factory_reset(
                target_node_id=target_node_id,
                config_only=config_only,
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="factory_reset not supported via serial connection yet",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="factory_reset not supported via serial connection yet",
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send {reset_type} command via {conn_type.value}",
            )

        msg = "Config reset" if config_only else "Full factory reset"
        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps({"message": f"{msg} command sent"}),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={
                "message": f"{msg} command sent to !{target_node_id:08x}. Node will reboot with reset settings."
            },
        )

    def begin_edit_settings(
        self,
        target_node_id: int,
    ) -> AdminCommandResult:
        """
        Begin a settings edit transaction on a remote node.

        This should be called before making multiple config changes to enable
        atomic updates. The node will hold changes in memory until
        commit_edit_settings is called.

        This follows the Meshtastic protocol for atomic config updates.
        See: https://github.com/meshtastic/web packages/core/src/meshDevice.ts

        Args:
            target_node_id: The target node ID

        Returns:
            AdminCommandResult with success/failure info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type
        if conn_type != AdminConnectionType.TCP:
            return AdminCommandResult(
                success=False,
                error="begin_edit_settings is only supported via TCP connection",
            )

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="begin_edit_settings",
            command_data=json.dumps({"connection_type": conn_type.value}),
        )

        publisher = self._get_publisher()
        packet_id = publisher.send_begin_edit_settings(target_node_id=target_node_id)

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send admin message via {conn_type.value}",
            )

        # Wait for ACK
        response = publisher.get_response(packet_id, timeout=10.0)
        if response and response.get("is_nak"):
            error_msg = (
                f"Node rejected begin_edit_settings: {response.get('error_reason', '')}"
            )
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=error_msg,
            )
            return AdminCommandResult(
                success=False,
                packet_id=packet_id,
                log_id=log_id,
                error=error_msg,
            )

        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps({"message": "Edit session started"}),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={"message": "Edit session started"},
        )

    def commit_edit_settings(
        self,
        target_node_id: int,
    ) -> AdminCommandResult:
        """
        Commit a settings edit transaction on a remote node.

        This should be called after making config changes to apply them.
        The node will save all pending changes to flash and apply them.

        This follows the Meshtastic protocol for atomic config updates.
        See: https://github.com/meshtastic/web packages/core/src/meshDevice.ts

        Args:
            target_node_id: The target node ID

        Returns:
            AdminCommandResult with success/failure info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type
        if conn_type != AdminConnectionType.TCP:
            return AdminCommandResult(
                success=False,
                error="commit_edit_settings is only supported via TCP connection",
            )

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="commit_edit_settings",
            command_data=json.dumps({"connection_type": conn_type.value}),
        )

        publisher = self._get_publisher()
        packet_id = publisher.send_commit_edit_settings(target_node_id=target_node_id)

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send admin message via {conn_type.value}",
            )

        # Wait for ACK
        response = publisher.get_response(packet_id, timeout=10.0)
        if response and response.get("is_nak"):
            error_msg = f"Node rejected commit_edit_settings: {response.get('error_reason', '')}"
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=error_msg,
            )
            return AdminCommandResult(
                success=False,
                packet_id=packet_id,
                log_id=log_id,
                error=error_msg,
            )

        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps({"message": "Edit session committed"}),
        )

        return AdminCommandResult(
            success=True,
            packet_id=packet_id,
            log_id=log_id,
            response={"message": "Edit session committed - config changes saved"},
        )

    def set_config(
        self,
        target_node_id: int,
        config_type: ConfigType,
        config_data: dict[str, Any],
    ) -> AdminCommandResult:
        """
        Set configuration on a remote node.

        Args:
            target_node_id: The target node ID
            config_type: The type of configuration to set
            config_data: Dictionary of config values to set


        Returns:
            AdminCommandResult with success/failure info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="set_config",
            command_data=json.dumps(
                {
                    "config_type": config_type.name,
                    "config_data": config_data,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the request using appropriate publisher
        publisher = self._get_publisher()

        config_type_str = config_type.name.lower()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_set_config(
                target_node_id=target_node_id,
                config_type=config_type_str,
                config_data=config_data,
            )
        else:
            # MQTT set_config not yet implemented
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="set_config not supported via MQTT",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="set_config is only supported via TCP connection",
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send admin message via {conn_type.value}",
            )

        # Wait for response/acknowledgment with shorter timeout for write operations
        response = publisher.get_response(packet_id, timeout=10.0)

        if response:
            # Check if this was a NAK (negative acknowledgement)
            is_nak = response.get("is_nak", False)
            error_reason = response.get("error_reason", "")

            if is_nak:
                error_msg = f"Node rejected config: {error_reason}"
                AdminRepository.update_admin_log_status(
                    log_id=log_id,
                    status="failed",
                    error_message=error_msg,
                )
                return AdminCommandResult(
                    success=False,
                    packet_id=packet_id,
                    log_id=log_id,
                    error=error_msg,
                )

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps({"message": "Config updated - ACK received"}),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "message": "Config updated - ACK received",
                    "acknowledged": True,
                },
            )
        else:
            # No response - packet was sent but ACK not received
            # Treat as success since packet was sent; node may have applied config
            # but ACK was lost or delayed (common in mesh networks)
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {"message": "Config sent - no ACK (likely applied)"}
                ),
            )
            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "message": "Config sent - no ACK received (likely applied)",
                    "acknowledged": False,
                },
            )

    def set_module_config(
        self,
        target_node_id: int,
        module_config_type: ModuleConfigType,
        module_data: dict[str, Any],
    ) -> AdminCommandResult:
        """
        Set module configuration on a remote node.

        Args:
            target_node_id: The target node ID
            module_config_type: The type of module configuration to set
            module_data: Dictionary of module config values to set

        Returns:
            AdminCommandResult with success/failure info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="set_module_config",
            command_data=json.dumps(
                {
                    "module_config_type": module_config_type.name,
                    "module_data": module_data,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the request using appropriate publisher
        publisher = self._get_publisher()

        module_type_str = module_config_type.name.lower()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_set_module_config(
                target_node_id=target_node_id,
                module_type=module_type_str,
                module_data=module_data,
            )
        else:
            # MQTT set_module_config not yet implemented
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="set_module_config not supported via MQTT",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="set_module_config is only supported via TCP connection",
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send admin message via {conn_type.value}",
            )

        # Wait for response/acknowledgment with shorter timeout for write operations
        response = publisher.get_response(packet_id, timeout=10.0)

        if response:
            # Check if this was a NAK (negative acknowledgement)
            is_nak = response.get("is_nak", False)
            error_reason = response.get("error_reason", "")

            if is_nak:
                error_msg = f"Node rejected module config: {error_reason}"
                AdminRepository.update_admin_log_status(
                    log_id=log_id,
                    status="failed",
                    error_message=error_msg,
                )
                return AdminCommandResult(
                    success=False,
                    packet_id=packet_id,
                    log_id=log_id,
                    error=error_msg,
                )

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {"message": "Module config updated - ACK received"}
                ),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "message": "Module config updated - ACK received",
                    "acknowledged": True,
                },
            )
        else:
            # No response - packet was sent but ACK not received
            # Treat as success since packet was sent; node may have applied config
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {"message": "Module config sent - no ACK (likely applied)"}
                ),
            )
            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "message": "Module config sent - no ACK received (likely applied)",
                    "acknowledged": False,
                },
            )

    def set_channel(
        self,
        target_node_id: int,
        channel_index: int,
        channel_data: dict[str, Any],
    ) -> AdminCommandResult:
        """
        Set channel configuration on a remote node.

        Args:
            target_node_id: The target node ID
            channel_index: The channel index (0-7)
            channel_data: Dictionary of channel settings

        Returns:
            AdminCommandResult with success/failure info
        """
        gateway_id = self.gateway_node_id
        if not gateway_id:
            return AdminCommandResult(
                success=False,
                error="No gateway node configured",
            )

        conn_type = self.connection_type

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="set_channel",
            command_data=json.dumps(
                {
                    "channel_index": channel_index,
                    "channel_data": channel_data,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the request using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_set_channel(
                target_node_id=target_node_id,
                channel_index=channel_index,
                channel_data=channel_data,
            )
        else:
            # MQTT set_channel not yet implemented
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="set_channel not supported via MQTT",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="set_channel is only supported via TCP connection",
            )

        if packet_id is None:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send admin message via {conn_type.value}",
            )

        # Wait for response/acknowledgment with shorter timeout for write operations
        response = publisher.get_response(packet_id, timeout=10.0)

        if response:
            # Check if this was a NAK (negative acknowledgement)
            is_nak = response.get("is_nak", False)
            error_reason = response.get("error_reason", "")

            if is_nak:
                error_msg = f"Node rejected channel config: {error_reason}"
                AdminRepository.update_admin_log_status(
                    log_id=log_id,
                    status="failed",
                    error_message=error_msg,
                )
                return AdminCommandResult(
                    success=False,
                    packet_id=packet_id,
                    log_id=log_id,
                    error=error_msg,
                )

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps({"message": "Channel updated - ACK received"}),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "message": "Channel updated - ACK received",
                    "acknowledged": True,
                },
            )
        else:
            # No response - packet was sent but ACK not received
            # Treat as success since packet was sent; node may have applied config
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {"message": "Channel config sent - no ACK (likely applied)"}
                ),
            )
            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={
                    "message": "Channel config sent - no ACK received (likely applied)",
                    "acknowledged": False,
                },
            )

    def get_config_schema(self, config_type: str) -> list[dict[str, Any]]:
        """
        Get the field schema for a config type.

        Args:
            config_type: The config type (device, position, etc.)

        Returns:
            List of field definitions
        """
        return get_config_schema(config_type)

    def get_all_config_schemas(self) -> dict[str, list[dict[str, Any]]]:
        """
        Get all config field schemas.

        Returns:
            Dict of config type to field definitions
        """
        return get_all_config_schemas()

    def get_administrable_nodes(self) -> list[dict[str, Any]]:
        """
        Get list of all administrable nodes.

        Returns:
            List of administrable node records
        """
        return AdminRepository.get_administrable_nodes()

    def is_node_administrable(self, node_id: int) -> bool:
        """
        Check if a specific node is administrable.

        Args:
            node_id: The node ID to check

        Returns:
            True if the node is administrable
        """
        return AdminRepository.is_node_administrable(node_id)

    def get_admin_log(
        self,
        target_node_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Get admin command audit log.

        Args:
            target_node_id: Optional filter by target node
            limit: Maximum entries to return

        Returns:
            List of log entries
        """
        return AdminRepository.get_admin_log(target_node_id=target_node_id, limit=limit)

    @staticmethod
    def _config_to_dict(config: config_pb2.Config) -> dict[str, Any]:
        """
        Convert a Config protobuf to a dictionary.

        Args:
            config: The Config protobuf

        Returns:
            Dictionary representation
        """
        result = {}

        if config.HasField("device"):
            result["device"] = {
                "role": config.device.role,
                "serial_enabled": config.device.serial_enabled,
                "button_gpio": config.device.button_gpio,
                "buzzer_gpio": config.device.buzzer_gpio,
                "rebroadcast_mode": config.device.rebroadcast_mode,
                "node_info_broadcast_secs": config.device.node_info_broadcast_secs,
                "double_tap_as_button_press": config.device.double_tap_as_button_press,
                "is_managed": config.device.is_managed,
                "disable_triple_click": config.device.disable_triple_click,
                "tzdef": config.device.tzdef,
                "led_heartbeat_disabled": config.device.led_heartbeat_disabled,
            }

        if config.HasField("position"):
            result["position"] = {
                "position_broadcast_secs": config.position.position_broadcast_secs,
                "position_broadcast_smart_enabled": config.position.position_broadcast_smart_enabled,
                "gps_enabled": config.position.gps_mode,
                "fixed_position": config.position.fixed_position,
                "gps_update_interval": config.position.gps_update_interval,
                "gps_attempt_time": config.position.gps_attempt_time,
                "broadcast_smart_minimum_distance": config.position.broadcast_smart_minimum_distance,
                "broadcast_smart_minimum_interval_secs": config.position.broadcast_smart_minimum_interval_secs,
            }

        if config.HasField("power"):
            result["power"] = {
                "is_power_saving": config.power.is_power_saving,
                "on_battery_shutdown_after_secs": config.power.on_battery_shutdown_after_secs,
                "adc_multiplier_override": config.power.adc_multiplier_override,
                "wait_bluetooth_secs": config.power.wait_bluetooth_secs,
                "sds_secs": config.power.sds_secs,
                "ls_secs": config.power.ls_secs,
                "min_wake_secs": config.power.min_wake_secs,
            }

        if config.HasField("network"):
            result["network"] = {
                "wifi_enabled": config.network.wifi_enabled,
                "wifi_ssid": config.network.wifi_ssid,
                "eth_enabled": config.network.eth_enabled,
            }

        if config.HasField("display"):
            result["display"] = {
                "screen_on_secs": config.display.screen_on_secs,
                "gps_format": config.display.gps_format,
                "auto_screen_carousel_secs": config.display.auto_screen_carousel_secs,
                "compass_north_top": config.display.compass_north_top,
                "flip_screen": config.display.flip_screen,
                "units": config.display.units,
                "heading_bold": config.display.heading_bold,
                "wake_on_tap_or_motion": config.display.wake_on_tap_or_motion,
                "use_12h_clock": config.display.use_12h_clock,
            }

        if config.HasField("lora"):
            result["lora"] = {
                "use_preset": config.lora.use_preset,
                "modem_preset": config.lora.modem_preset,
                "bandwidth": config.lora.bandwidth,
                "spread_factor": config.lora.spread_factor,
                "coding_rate": config.lora.coding_rate,
                "frequency_offset": config.lora.frequency_offset,
                "region": config.lora.region,
                "hop_limit": config.lora.hop_limit,
                "tx_enabled": config.lora.tx_enabled,
                "tx_power": config.lora.tx_power,
                "channel_num": config.lora.channel_num,
                "override_duty_cycle": config.lora.override_duty_cycle,
                "sx126x_rx_boosted_gain": config.lora.sx126x_rx_boosted_gain,
                "override_frequency": config.lora.override_frequency,
                "pa_fan_disabled": config.lora.pa_fan_disabled,
                "ignore_mqtt": config.lora.ignore_mqtt,
                "config_ok_to_mqtt": config.lora.config_ok_to_mqtt,
            }

        if config.HasField("bluetooth"):
            result["bluetooth"] = {
                "enabled": config.bluetooth.enabled,
                "mode": config.bluetooth.mode,
                "fixed_pin": config.bluetooth.fixed_pin,
            }

        if config.HasField("security"):
            result["security"] = {
                "public_key": config.security.public_key.hex()
                if config.security.public_key
                else None,
                "private_key": config.security.private_key.hex()
                if config.security.private_key
                else None,
                # admin_key is a repeated field (list of admin public keys)
                "admin_key": [k.hex() for k in config.security.admin_key]
                if config.security.admin_key
                else [],
                "is_managed": config.security.is_managed,
                "serial_enabled": config.security.serial_enabled,
                "debug_log_api_enabled": config.security.debug_log_api_enabled,
                "admin_channel_enabled": config.security.admin_channel_enabled,
            }

        return result

    def create_backup(
        self,
        target_node_id: int,
        backup_name: str,
        description: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        timeout: float = 30.0,
    ) -> AdminCommandResult:
        """
        Create a full configuration backup of a remote node.

        This method retrieves all core configs, module configs, and channels
        from the target node and saves them as a complete backup.

        Args:
            target_node_id: The target node ID to backup
            backup_name: Name for the backup
            description: Optional description
            max_retries: Maximum number of retry attempts for each config
            retry_delay: Seconds to wait between retries
            timeout: Seconds to wait for each response

        Returns:
            AdminCommandResult with backup details
        """
        backup_data: dict[str, Any] = {
            "backup_version": 1,
            "target_node_id": target_node_id,
            "created_at": time.time(),
            "core_configs": {},
            "module_configs": {},
            "channels": {},
        }

        errors: list[str] = []
        successful_configs: list[str] = []

        # Get all core configs
        core_config_types = [
            ConfigType.DEVICE,
            ConfigType.POSITION,
            ConfigType.POWER,
            ConfigType.NETWORK,
            ConfigType.DISPLAY,
            ConfigType.LORA,
            ConfigType.BLUETOOTH,
            ConfigType.SECURITY,
        ]

        for config_type in core_config_types:
            result = self.get_config(
                target_node_id=target_node_id,
                config_type=config_type,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
            )
            if result.success and result.response:
                backup_data["core_configs"][config_type.name.lower()] = result.response
                successful_configs.append(f"core:{config_type.name}")
            else:
                errors.append(
                    f"core:{config_type.name}: {result.error or 'Unknown error'}"
                )

        # Get all module configs
        module_config_types = [
            ModuleConfigType.MQTT,
            ModuleConfigType.SERIAL,
            ModuleConfigType.EXTNOTIF,
            ModuleConfigType.STOREFORWARD,
            ModuleConfigType.RANGETEST,
            ModuleConfigType.TELEMETRY,
            ModuleConfigType.CANNEDMSG,
            ModuleConfigType.AUDIO,
            ModuleConfigType.REMOTEHARDWARE,
            ModuleConfigType.NEIGHBORINFO,
            ModuleConfigType.AMBIENTLIGHTING,
            ModuleConfigType.DETECTIONSENSOR,
            ModuleConfigType.PAXCOUNTER,
        ]

        for module_type in module_config_types:
            result = self.get_module_config(
                target_node_id=target_node_id,
                module_config_type=module_type,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
            )
            if result.success and result.response:
                backup_data["module_configs"][module_type.name.lower()] = (
                    result.response
                )
                successful_configs.append(f"module:{module_type.name}")
            else:
                errors.append(
                    f"module:{module_type.name}: {result.error or 'Unknown error'}"
                )

        # Get all 8 channels
        for channel_idx in range(8):
            result = self.get_channel(
                target_node_id=target_node_id,
                channel_index=channel_idx,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
            )
            if result.success and result.response:
                backup_data["channels"][str(channel_idx)] = result.response
                successful_configs.append(f"channel:{channel_idx}")
            else:
                errors.append(
                    f"channel:{channel_idx}: {result.error or 'Unknown error'}"
                )

        # Get node info for metadata
        node_long_name = None
        node_short_name = None
        node_hex_id = f"!{target_node_id:08x}"
        hardware_model = None
        firmware_version = None

        # Try to get node info from device config if available
        if "device" in backup_data["core_configs"]:
            device_config = backup_data["core_configs"]["device"]
            node_long_name = device_config.get("device", {}).get("owner", None)
            node_short_name = device_config.get("device", {}).get("owner_short", None)

        # Log the backup
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="create_backup",
            command_data=json.dumps(
                {
                    "backup_name": backup_name,
                    "successful_configs": len(successful_configs),
                    "failed_configs": len(errors),
                }
            ),
        )

        # Save to database if we got at least some configs
        if successful_configs:
            backup_id = AdminRepository.create_backup(
                node_id=target_node_id,
                backup_name=backup_name,
                backup_data=json.dumps(backup_data),
                description=description,
                node_long_name=node_long_name,
                node_short_name=node_short_name,
                node_hex_id=node_hex_id,
                hardware_model=hardware_model,
                firmware_version=firmware_version,
            )

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success" if not errors else "partial",
                response_data=json.dumps(
                    {
                        "backup_id": backup_id,
                        "successful": successful_configs,
                        "failed": errors,
                    }
                ),
            )

            return AdminCommandResult(
                success=True,
                log_id=log_id,
                response={
                    "backup_id": backup_id,
                    "backup_name": backup_name,
                    "successful_configs": successful_configs,
                    "failed_configs": errors,
                    "total_configs": len(successful_configs) + len(errors),
                },
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message="No configs retrieved",
            )

            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error="Failed to retrieve any configuration from node",
            )

    @staticmethod
    def _module_config_to_dict(
        module_config: module_config_pb2.ModuleConfig,
    ) -> dict[str, Any]:
        """
        Convert a ModuleConfig protobuf to a dictionary.

        Args:
            module_config: The ModuleConfig protobuf

        Returns:
            Dictionary representation
        """
        result = {}

        if module_config.HasField("mqtt"):
            result["mqtt"] = {
                "enabled": module_config.mqtt.enabled,
                "address": module_config.mqtt.address,
                "username": module_config.mqtt.username,
                "encryption_enabled": module_config.mqtt.encryption_enabled,
                "json_enabled": module_config.mqtt.json_enabled,
                "tls_enabled": module_config.mqtt.tls_enabled,
                "root": module_config.mqtt.root,
                "proxy_to_client_enabled": module_config.mqtt.proxy_to_client_enabled,
                "map_reporting_enabled": module_config.mqtt.map_reporting_enabled,
            }

        if module_config.HasField("serial"):
            result["serial"] = {
                "enabled": module_config.serial.enabled,
                "echo": module_config.serial.echo,
                "rxd": module_config.serial.rxd,
                "txd": module_config.serial.txd,
                "baud": module_config.serial.baud,
                "timeout": module_config.serial.timeout,
                "mode": module_config.serial.mode,
                "override_console_serial_port": module_config.serial.override_console_serial_port,
            }

        if module_config.HasField("external_notification"):
            result["extnotif"] = {
                "enabled": module_config.external_notification.enabled,
                "output_ms": module_config.external_notification.output_ms,
                "output": module_config.external_notification.output,
                "output_vibra": module_config.external_notification.output_vibra,
                "output_buzzer": module_config.external_notification.output_buzzer,
                "active": module_config.external_notification.active,
                "alert_message": module_config.external_notification.alert_message,
                "alert_message_vibra": module_config.external_notification.alert_message_vibra,
                "alert_message_buzzer": module_config.external_notification.alert_message_buzzer,
                "alert_bell": module_config.external_notification.alert_bell,
                "alert_bell_vibra": module_config.external_notification.alert_bell_vibra,
                "alert_bell_buzzer": module_config.external_notification.alert_bell_buzzer,
                "use_pwm": module_config.external_notification.use_pwm,
                "nag_timeout": module_config.external_notification.nag_timeout,
                "use_i2s_as_buzzer": module_config.external_notification.use_i2s_as_buzzer,
            }

        if module_config.HasField("store_forward"):
            result["storeforward"] = {
                "enabled": module_config.store_forward.enabled,
                "heartbeat": module_config.store_forward.heartbeat,
                "records": module_config.store_forward.records,
                "history_return_max": module_config.store_forward.history_return_max,
                "history_return_window": module_config.store_forward.history_return_window,
                "is_server": module_config.store_forward.is_server,
            }

        if module_config.HasField("range_test"):
            result["rangetest"] = {
                "enabled": module_config.range_test.enabled,
                "sender": module_config.range_test.sender,
                "save": module_config.range_test.save,
                "clear_on_reboot": module_config.range_test.clear_on_reboot,
            }

        if module_config.HasField("telemetry"):
            result["telemetry"] = {
                "device_update_interval": module_config.telemetry.device_update_interval,
                "environment_update_interval": module_config.telemetry.environment_update_interval,
                "environment_measurement_enabled": module_config.telemetry.environment_measurement_enabled,
                "environment_screen_enabled": module_config.telemetry.environment_screen_enabled,
                "environment_display_fahrenheit": module_config.telemetry.environment_display_fahrenheit,
                "air_quality_enabled": module_config.telemetry.air_quality_enabled,
                "air_quality_interval": module_config.telemetry.air_quality_interval,
                "power_measurement_enabled": module_config.telemetry.power_measurement_enabled,
                "power_update_interval": module_config.telemetry.power_update_interval,
                "power_screen_enabled": module_config.telemetry.power_screen_enabled,
                "device_telemetry_enabled": module_config.telemetry.device_telemetry_enabled,
                "health_measurement_enabled": module_config.telemetry.health_measurement_enabled,
                "health_update_interval": module_config.telemetry.health_update_interval,
                "health_screen_enabled": module_config.telemetry.health_screen_enabled,
            }

        if module_config.HasField("canned_message"):
            result["cannedmsg"] = {
                "rotary1_enabled": module_config.canned_message.rotary1_enabled,
                "inputbroker_pin_a": module_config.canned_message.inputbroker_pin_a,
                "inputbroker_pin_b": module_config.canned_message.inputbroker_pin_b,
                "inputbroker_pin_press": module_config.canned_message.inputbroker_pin_press,
                "inputbroker_event_cw": module_config.canned_message.inputbroker_event_cw,
                "inputbroker_event_ccw": module_config.canned_message.inputbroker_event_ccw,
                "inputbroker_event_press": module_config.canned_message.inputbroker_event_press,
                "updown1_enabled": module_config.canned_message.updown1_enabled,
                "enabled": module_config.canned_message.enabled,
                "allow_input_source": module_config.canned_message.allow_input_source,
                "send_bell": module_config.canned_message.send_bell,
            }

        if module_config.HasField("audio"):
            result["audio"] = {
                "codec2_enabled": module_config.audio.codec2_enabled,
                "ptt_pin": module_config.audio.ptt_pin,
                "bitrate": module_config.audio.bitrate,
                "i2s_ws": module_config.audio.i2s_ws,
                "i2s_sd": module_config.audio.i2s_sd,
                "i2s_din": module_config.audio.i2s_din,
                "i2s_sck": module_config.audio.i2s_sck,
            }

        if module_config.HasField("remote_hardware"):
            result["remotehardware"] = {
                "enabled": module_config.remote_hardware.enabled,
                "allow_undefined_pin_access": module_config.remote_hardware.allow_undefined_pin_access,
            }

        if module_config.HasField("neighbor_info"):
            result["neighborinfo"] = {
                "enabled": module_config.neighbor_info.enabled,
                "update_interval": module_config.neighbor_info.update_interval,
                "transmit_over_lora": module_config.neighbor_info.transmit_over_lora,
            }

        if module_config.HasField("ambient_lighting"):
            result["ambientlighting"] = {
                "led_state": module_config.ambient_lighting.led_state,
                "current": module_config.ambient_lighting.current,
                "red": module_config.ambient_lighting.red,
                "green": module_config.ambient_lighting.green,
                "blue": module_config.ambient_lighting.blue,
            }

        if module_config.HasField("detection_sensor"):
            result["detectionsensor"] = {
                "enabled": module_config.detection_sensor.enabled,
                "minimum_broadcast_secs": module_config.detection_sensor.minimum_broadcast_secs,
                "state_broadcast_secs": module_config.detection_sensor.state_broadcast_secs,
                "send_bell": module_config.detection_sensor.send_bell,
                "name": module_config.detection_sensor.name,
                "monitor_pin": module_config.detection_sensor.monitor_pin,
                "detection_trigger_type": module_config.detection_sensor.detection_trigger_type,
                "use_pullup": module_config.detection_sensor.use_pullup,
            }

        if module_config.HasField("paxcounter"):
            result["paxcounter"] = {
                "enabled": module_config.paxcounter.enabled,
                "paxcounter_update_interval": module_config.paxcounter.paxcounter_update_interval,
                "wifi_threshold": module_config.paxcounter.wifi_threshold,
                "ble_threshold": module_config.paxcounter.ble_threshold,
            }

        return result


# Global service instance
_admin_service: AdminService | None = None


def get_admin_service() -> AdminService:
    """Get the singleton AdminService instance."""
    global _admin_service
    if _admin_service is None:
        _admin_service = AdminService()
    return _admin_service
