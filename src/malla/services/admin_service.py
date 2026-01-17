"""
Admin service for remote node administration.

This service provides high-level operations for administering Meshtastic nodes
remotely via MQTT, TCP, or serial connection.
"""

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from meshtastic import config_pb2

from ..config import get_config
from ..database.admin_repository import AdminRepository
from .config_metadata import get_all_config_schemas, get_config_schema
from .mqtt_publisher import get_mqtt_publisher
from .tcp_publisher import get_tcp_publisher

logger = logging.getLogger(__name__)


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


@dataclass
class AdminCommandResult:
    """Result of an admin command."""

    success: bool
    packet_id: int | None = None
    log_id: int | None = None
    response: dict[str, Any] | None = None
    error: str | None = None


class AdminService:
    """
    Service for remote node administration.

    Provides high-level methods for common admin operations like
    getting configuration, rebooting nodes, etc.
    """

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
            # Serial not yet implemented, fall back to MQTT
            logger.warning("Serial connection not yet implemented, using MQTT")
            return get_mqtt_publisher()
        else:
            return get_mqtt_publisher()

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

        status: dict[str, Any] = {
            "enabled": self.is_enabled(),
            "connection_type": conn_type.value,
            "connected": publisher.is_connected,
            "gateway_node_id": gateway_id,
            "gateway_node_hex": f"!{gateway_id:08x}" if gateway_id else None,
        }

        # Add connection-specific details
        if conn_type == AdminConnectionType.TCP:
            tcp_publisher = get_tcp_publisher()
            status["tcp_host"] = tcp_publisher.tcp_host
            status["tcp_port"] = tcp_publisher.tcp_port
        elif conn_type == AdminConnectionType.MQTT:
            mqtt_publisher = get_mqtt_publisher()
            status["mqtt_connected"] = mqtt_publisher.is_connected

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
            admin_msg = response.get("admin_message")

            if admin_msg and hasattr(admin_msg, "HasField"):
                try:
                    if admin_msg.HasField("get_device_metadata_response"):
                        meta = admin_msg.get_device_metadata_response
                        firmware_version = meta.firmware_version
                        device_metadata = str(meta)
                        logger.info(
                            f"Got device metadata from node {target_node_id}: "
                            f"firmware={firmware_version}"
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
                },
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message="No response received within timeout",
            )
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
    ) -> AdminCommandResult:
        """
        Request configuration from a remote node.

        Args:
            target_node_id: The target node ID
            config_type: The type of configuration to request

        Returns:
            AdminCommandResult with config data
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
            command_type="get_config",
            command_data=json.dumps(
                {
                    "config_type": config_type.name,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the request using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_get_config(
                target_node_id=target_node_id,
                config_type=config_type.value,
            )
        else:
            packet_id = publisher.send_get_config(
                target_node_id=target_node_id,
                from_node_id=gateway_id,
                config_type=config_type.value,
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

        # Wait for response
        response = publisher.get_response(packet_id, timeout=30.0)

        if response:
            # Parse the config response
            admin_msg = response.get("admin_message")
            config_data = {}

            if admin_msg and admin_msg.HasField("get_config_response"):
                config = admin_msg.get_config_response
                config_data = self._config_to_dict(config)

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(config_data),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response=config_data,
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message="No response received",
            )
            return AdminCommandResult(
                success=False,
                packet_id=packet_id,
                log_id=log_id,
                error="No response received (timeout)",
            )

    def get_channel(
        self,
        target_node_id: int,
        channel_index: int,
    ) -> AdminCommandResult:
        """
        Request channel configuration from a remote node.

        Args:
            target_node_id: The target node ID
            channel_index: The channel index (0-7)

        Returns:
            AdminCommandResult with channel data
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
            command_type="get_channel",
            command_data=json.dumps(
                {
                    "channel_index": channel_index,
                    "connection_type": conn_type.value,
                }
            ),
        )

        # Send the request using appropriate publisher
        publisher = self._get_publisher()

        if conn_type == AdminConnectionType.TCP:
            packet_id = publisher.send_get_channel(
                target_node_id=target_node_id,
                channel_index=channel_index,
            )
        else:
            packet_id = publisher.send_get_channel(
                target_node_id=target_node_id,
                from_node_id=gateway_id,
                channel_index=channel_index,
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

        # Wait for response
        response = publisher.get_response(packet_id, timeout=30.0)

        if response:
            admin_msg = response.get("admin_message")
            channel_data = {}

            if admin_msg and admin_msg.HasField("get_channel_response"):
                channel = admin_msg.get_channel_response
                channel_data = {
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

            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(channel_data),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response=channel_data,
            )
        else:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="timeout",
                error_message="No response received",
            )
            return AdminCommandResult(
                success=False,
                packet_id=packet_id,
                log_id=log_id,
                error="No response received (timeout)",
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

        # Log the command
        log_id = AdminRepository.log_admin_command(
            target_node_id=target_node_id,
            command_type="reboot",
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
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="failed",
                error_message=f"Failed to send message via {conn_type.value}",
            )
            return AdminCommandResult(
                success=False,
                log_id=log_id,
                error=f"Failed to send reboot command via {conn_type.value}",
            )

        # Reboot doesn't send a response, mark as success
        AdminRepository.update_admin_log_status(
            log_id=log_id,
            status="success",
            response_data=json.dumps(
                {
                    "message": f"Reboot command sent, node will reboot in {delay_seconds}s"
                }
            ),
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

        # Wait for response/acknowledgment
        response = publisher.get_response(packet_id, timeout=30.0)

        if response:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps({"message": "Config updated successfully"}),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={"message": "Config updated successfully"},
            )
        else:
            # Even without response, the config may have been applied
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {"message": "Config sent (no confirmation received)"}
                ),
            )
            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={"message": "Config sent (no confirmation received)"},
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

        # Wait for response/acknowledgment
        response = publisher.get_response(packet_id, timeout=30.0)

        if response:
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps({"message": "Channel updated successfully"}),
            )

            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={"message": "Channel updated successfully"},
            )
        else:
            # Even without response, the channel may have been applied
            AdminRepository.update_admin_log_status(
                log_id=log_id,
                status="success",
                response_data=json.dumps(
                    {"message": "Channel sent (no confirmation received)"}
                ),
            )
            return AdminCommandResult(
                success=True,
                packet_id=packet_id,
                log_id=log_id,
                response={"message": "Channel sent (no confirmation received)"},
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
            }

        if config.HasField("position"):
            result["position"] = {
                "position_broadcast_secs": config.position.position_broadcast_secs,
                "position_broadcast_smart_enabled": config.position.position_broadcast_smart_enabled,
                "gps_enabled": config.position.gps_mode,
                "fixed_position": config.position.fixed_position,
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
            }

        if config.HasField("bluetooth"):
            result["bluetooth"] = {
                "enabled": config.bluetooth.enabled,
                "mode": config.bluetooth.mode,
                "fixed_pin": config.bluetooth.fixed_pin,
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
