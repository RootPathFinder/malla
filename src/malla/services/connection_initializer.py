"""
Connection Initializer - Initialize connections from configuration.

This module handles the initialization of multiple connections from the
application configuration, supporting both legacy single-connection and
new multi-connection configurations.
"""

import logging
from typing import Any

from ..config import get_config
from .connection_manager import (
    ConnectionManager,
    ConnectionRole,
    ConnectionType,
    get_connection_manager,
)
from .mqtt_publisher import get_mqtt_publisher
from .serial_publisher import SerialPublisher
from .tcp_publisher import TCPPublisher

logger = logging.getLogger(__name__)


def initialize_connections_from_config() -> ConnectionManager:
    """
    Initialize connections from the application configuration.

    This function supports both:
    1. Legacy single-connection config (admin_connection_type, admin_tcp_host, etc.)
    2. New multi-connection config (connections list)

    Returns:
        The ConnectionManager instance with all connections registered
    """
    config = get_config()
    manager = get_connection_manager()

    # Check if we have new multi-connection configuration
    if config.connections and len(config.connections) > 0:
        logger.info(
            f"Initializing {len(config.connections)} connections from config"
        )
        _initialize_multi_connections(manager, config.connections)
    else:
        # Fall back to legacy single-connection configuration
        logger.info("Initializing from legacy single-connection config")
        _initialize_legacy_connection(manager, config)

    return manager


def _initialize_multi_connections(
    manager: ConnectionManager, connections_config: list[dict[str, Any]]
) -> None:
    """
    Initialize multiple connections from the connections list in config.

    Args:
        manager: ConnectionManager instance
        connections_config: List of connection definitions from config
    """
    for conn_def in connections_config:
        try:
            conn_id = conn_def.get("id")
            conn_type_str = conn_def.get("type", "").lower()
            role_str = conn_def.get("role", "client").lower()
            auto_connect = conn_def.get("auto_connect", True)
            description = conn_def.get("description", "")

            if not conn_id:
                logger.warning("Connection definition missing 'id', skipping")
                continue

            # Parse connection type
            try:
                conn_type = ConnectionType(conn_type_str)
            except ValueError:
                logger.error(
                    f"Invalid connection type '{conn_type_str}' for '{conn_id}'"
                )
                continue

            # Parse role
            try:
                role = ConnectionRole(role_str)
            except ValueError:
                logger.error(
                    f"Invalid role '{role_str}' for '{conn_id}', defaulting to CLIENT"
                )
                role = ConnectionRole.CLIENT

            # Create publisher based on type
            publisher = None
            if conn_type == ConnectionType.TCP:
                publisher = _create_tcp_publisher(conn_id, conn_def)
            elif conn_type == ConnectionType.SERIAL:
                publisher = _create_serial_publisher(conn_id, conn_def)
            elif conn_type == ConnectionType.MQTT:
                # MQTT is typically read-only, use the singleton
                publisher = get_mqtt_publisher()
            else:
                logger.error(f"Unsupported connection type: {conn_type}")
                continue

            if publisher:
                manager.add_connection(
                    connection_id=conn_id,
                    connection_type=conn_type,
                    role=role,
                    publisher=publisher,
                    description=description,
                    auto_connect=auto_connect,
                )
                logger.info(
                    f"Registered connection '{conn_id}' "
                    f"(type={conn_type.value}, role={role.value})"
                )

        except Exception as e:
            logger.error(f"Error initializing connection: {e}")


def _initialize_legacy_connection(manager: ConnectionManager, config: Any) -> None:
    """
    Initialize a single connection from legacy configuration.

    Args:
        manager: ConnectionManager instance
        config: AppConfig instance
    """
    conn_type_str = config.admin_connection_type.lower()

    # Determine connection type
    try:
        conn_type = ConnectionType(conn_type_str)
    except ValueError:
        logger.warning(
            f"Invalid legacy connection type '{conn_type_str}', defaulting to MQTT"
        )
        conn_type = ConnectionType.MQTT

    # Legacy connections are used for admin by default
    role = ConnectionRole.ADMIN
    conn_id = f"legacy_{conn_type.value}"

    # Create publisher
    publisher = None
    if conn_type == ConnectionType.TCP:
        publisher = TCPPublisher(connection_id=conn_id)
        # Set connection parameters from legacy config
        if hasattr(config, "admin_tcp_host") and hasattr(config, "admin_tcp_port"):
            publisher.set_connection_params(
                host=config.admin_tcp_host, port=config.admin_tcp_port
            )
    elif conn_type == ConnectionType.SERIAL:
        publisher = SerialPublisher(connection_id=conn_id)
        # Serial port would need to be configured separately
    elif conn_type == ConnectionType.MQTT:
        publisher = get_mqtt_publisher()

    if publisher:
        manager.add_connection(
            connection_id=conn_id,
            connection_type=conn_type,
            role=role,
            publisher=publisher,
            description=f"Legacy {conn_type.value} connection",
            auto_connect=True,
        )
        logger.info(
            f"Registered legacy connection '{conn_id}' "
            f"(type={conn_type.value}, role={role.value})"
        )


def _create_tcp_publisher(conn_id: str, conn_def: dict[str, Any]) -> TCPPublisher:
    """
    Create a TCP publisher from connection definition.

    Args:
        conn_id: Connection identifier
        conn_def: Connection definition dict

    Returns:
        Configured TCPPublisher instance
    """
    publisher = TCPPublisher(connection_id=conn_id)

    # Set connection parameters
    host = conn_def.get("host")
    port = conn_def.get("port")

    if host or port:
        publisher.set_connection_params(host=host, port=port)
        logger.debug(f"TCP connection '{conn_id}' configured: {host}:{port}")

    return publisher


def _create_serial_publisher(
    conn_id: str, conn_def: dict[str, Any]
) -> SerialPublisher:
    """
    Create a Serial publisher from connection definition.

    Args:
        conn_id: Connection identifier
        conn_def: Connection definition dict

    Returns:
        Configured SerialPublisher instance
    """
    publisher = SerialPublisher(connection_id=conn_id)

    # Set serial port
    port = conn_def.get("port")
    if port:
        publisher.set_serial_port(port)
        logger.debug(f"Serial connection '{conn_id}' configured: {port}")

    return publisher
