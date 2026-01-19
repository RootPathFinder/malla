"""
Connection Manager for managing multiple Meshtastic node connections.

This module provides centralized management of multiple simultaneous connections
to Meshtastic nodes, with support for role-based connection designation (admin vs client).
"""

import logging
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class ConnectionRole(Enum):
    """Role designation for a connection."""

    ADMIN = (
        "admin"  # For administrative operations (backups, compliance, restores, config)
    )
    CLIENT = "client"  # For basic mesh activities (chat, traceroute, monitoring, bot)


class ConnectionType(Enum):
    """Type of connection interface."""

    TCP = "tcp"
    SERIAL = "serial"
    MQTT = "mqtt"


@dataclass
class ConnectionInfo:
    """Information about a managed connection."""

    connection_id: str
    connection_type: ConnectionType
    role: ConnectionRole
    publisher: (
        Any  # The actual publisher instance (TCPPublisher, SerialPublisher, etc.)
    )
    description: str = ""
    auto_connect: bool = True

    @property
    def is_connected(self) -> bool:
        """Check if this connection is currently active."""
        if self.publisher is None:
            return False
        return getattr(self.publisher, "is_connected", False)

    def get_connection_params(self) -> dict[str, Any]:
        """Get connection parameters for this connection."""
        params: dict[str, Any] = {
            "connection_id": self.connection_id,
            "connection_type": self.connection_type.value,
            "role": self.role.value,
            "description": self.description,
            "is_connected": self.is_connected,
            "auto_connect": self.auto_connect,
        }

        # Add type-specific parameters
        if self.connection_type == ConnectionType.TCP:
            if hasattr(self.publisher, "tcp_host"):
                params["host"] = self.publisher.tcp_host
            if hasattr(self.publisher, "tcp_port"):
                params["port"] = self.publisher.tcp_port
        elif self.connection_type == ConnectionType.SERIAL:
            if hasattr(self.publisher, "serial_port"):
                params["port"] = self.publisher.serial_port

        return params


class ConnectionManager:
    """
    Manages multiple simultaneous connections to Meshtastic nodes.

    This class allows the application to maintain multiple active connections
    (e.g., one TCP connection for admin, one Serial connection for client operations)
    and route operations to the appropriate connection based on role.
    """

    _instance: "ConnectionManager | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "ConnectionManager":
        """Singleton pattern to ensure only one ConnectionManager exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the connection manager."""
        if self._initialized:
            return

        self._initialized = True
        self._connections: dict[str, ConnectionInfo] = {}
        self._connections_lock = threading.RLock()

        logger.info("ConnectionManager initialized")

    def add_connection(
        self,
        connection_id: str,
        connection_type: ConnectionType,
        role: ConnectionRole,
        publisher: Any,
        description: str = "",
        auto_connect: bool = True,
    ) -> None:
        """
        Add a new connection to the manager.

        Args:
            connection_id: Unique identifier for this connection
            connection_type: Type of connection (TCP, Serial, MQTT)
            role: Role of this connection (ADMIN or CLIENT)
            publisher: The publisher instance for this connection
            description: Human-readable description of the connection
            auto_connect: Whether to auto-connect this connection on startup
        """
        with self._connections_lock:
            if connection_id in self._connections:
                logger.warning(
                    f"Connection '{connection_id}' already exists, replacing"
                )

            conn_info = ConnectionInfo(
                connection_id=connection_id,
                connection_type=connection_type,
                role=role,
                publisher=publisher,
                description=description,
                auto_connect=auto_connect,
            )

            self._connections[connection_id] = conn_info
            logger.info(
                f"Added connection '{connection_id}' "
                f"(type={connection_type.value}, role={role.value})"
            )

    def remove_connection(self, connection_id: str) -> bool:
        """
        Remove a connection from the manager.

        Args:
            connection_id: ID of the connection to remove

        Returns:
            True if connection was removed, False if not found
        """
        with self._connections_lock:
            if connection_id not in self._connections:
                logger.warning(f"Connection '{connection_id}' not found")
                return False

            conn_info = self._connections[connection_id]

            # Disconnect if currently connected
            if conn_info.is_connected and hasattr(conn_info.publisher, "disconnect"):
                try:
                    conn_info.publisher.disconnect()
                    logger.info(f"Disconnected connection '{connection_id}'")
                except Exception as e:
                    logger.warning(f"Error disconnecting '{connection_id}': {e}")

            del self._connections[connection_id]
            logger.info(f"Removed connection '{connection_id}'")
            return True

    def get_connection(self, connection_id: str) -> ConnectionInfo | None:
        """
        Get a specific connection by ID.

        Args:
            connection_id: ID of the connection to retrieve

        Returns:
            ConnectionInfo if found, None otherwise
        """
        with self._connections_lock:
            return self._connections.get(connection_id)

    def get_connection_by_role(self, role: ConnectionRole) -> ConnectionInfo | None:
        """
        Get the first connection with the specified role.

        Args:
            role: Role to search for (ADMIN or CLIENT)

        Returns:
            ConnectionInfo if found, None otherwise
        """
        with self._connections_lock:
            for conn in self._connections.values():
                if conn.role == role:
                    return conn
            return None

    def get_all_connections_by_role(self, role: ConnectionRole) -> list[ConnectionInfo]:
        """
        Get all connections with the specified role.

        Args:
            role: Role to search for (ADMIN or CLIENT)

        Returns:
            List of ConnectionInfo objects
        """
        with self._connections_lock:
            return [conn for conn in self._connections.values() if conn.role == role]

    def get_all_connections(self) -> list[ConnectionInfo]:
        """
        Get all registered connections.

        Returns:
            List of all ConnectionInfo objects
        """
        with self._connections_lock:
            return list(self._connections.values())

    def get_admin_publisher(self) -> Any | None:
        """
        Get a publisher for admin operations.

        Returns the first connected admin connection, or the first admin connection
        if none are connected.

        Returns:
            Publisher instance or None if no admin connection exists
        """
        with self._connections_lock:
            admin_connections = self.get_all_connections_by_role(ConnectionRole.ADMIN)

            if not admin_connections:
                logger.warning("No admin connection configured")
                return None

            # Prefer connected admin connections
            for conn in admin_connections:
                if conn.is_connected:
                    logger.debug(
                        f"Using connected admin connection '{conn.connection_id}'"
                    )
                    return conn.publisher

            # Fall back to first admin connection (will attempt to connect)
            logger.debug(
                f"Using admin connection '{admin_connections[0].connection_id}' "
                "(not yet connected)"
            )
            return admin_connections[0].publisher

    def get_client_publisher(self) -> Any | None:
        """
        Get a publisher for client operations.

        Returns the first connected client connection, or the first client connection
        if none are connected.

        Returns:
            Publisher instance or None if no client connection exists
        """
        with self._connections_lock:
            client_connections = self.get_all_connections_by_role(ConnectionRole.CLIENT)

            if not client_connections:
                logger.warning("No client connection configured")
                return None

            # Prefer connected client connections
            for conn in client_connections:
                if conn.is_connected:
                    logger.debug(
                        f"Using connected client connection '{conn.connection_id}'"
                    )
                    return conn.publisher

            # Fall back to first client connection (will attempt to connect)
            logger.debug(
                f"Using client connection '{client_connections[0].connection_id}' "
                "(not yet connected)"
            )
            return client_connections[0].publisher

    def set_connection_role(self, connection_id: str, role: ConnectionRole) -> bool:
        """
        Change the role of an existing connection.

        Args:
            connection_id: ID of the connection to update
            role: New role for the connection

        Returns:
            True if successful, False if connection not found
        """
        with self._connections_lock:
            conn = self._connections.get(connection_id)
            if conn is None:
                logger.warning(f"Connection '{connection_id}' not found")
                return False

            old_role = conn.role
            conn.role = role
            logger.info(
                f"Changed role of connection '{connection_id}' "
                f"from {old_role.value} to {role.value}"
            )
            return True

    def connect_all(self, role: ConnectionRole | None = None) -> dict[str, bool]:
        """
        Connect all registered connections, optionally filtered by role.

        Args:
            role: If specified, only connect connections with this role

        Returns:
            Dictionary mapping connection_id to success status
        """
        results: dict[str, bool] = {}

        with self._connections_lock:
            connections = (
                self.get_all_connections_by_role(role)
                if role
                else self.get_all_connections()
            )

            for conn in connections:
                if not conn.auto_connect:
                    logger.debug(
                        f"Skipping connection '{conn.connection_id}' "
                        "(auto_connect=False)"
                    )
                    results[conn.connection_id] = False
                    continue

                try:
                    if hasattr(conn.publisher, "connect"):
                        success = conn.publisher.connect()
                        results[conn.connection_id] = success
                        if success:
                            logger.info(
                                f"Connected '{conn.connection_id}' ({conn.role.value})"
                            )
                        else:
                            logger.warning(f"Failed to connect '{conn.connection_id}'")
                    else:
                        logger.warning(
                            f"Connection '{conn.connection_id}' has no connect method"
                        )
                        results[conn.connection_id] = False
                except Exception as e:
                    logger.error(f"Error connecting '{conn.connection_id}': {e}")
                    results[conn.connection_id] = False

        return results

    def disconnect_all(self, role: ConnectionRole | None = None) -> dict[str, bool]:
        """
        Disconnect all connections, optionally filtered by role.

        Args:
            role: If specified, only disconnect connections with this role

        Returns:
            Dictionary mapping connection_id to success status
        """
        results: dict[str, bool] = {}

        with self._connections_lock:
            connections = (
                self.get_all_connections_by_role(role)
                if role
                else self.get_all_connections()
            )

            for conn in connections:
                try:
                    if hasattr(conn.publisher, "disconnect"):
                        conn.publisher.disconnect()
                        results[conn.connection_id] = True
                        logger.info(f"Disconnected '{conn.connection_id}'")
                    else:
                        logger.warning(
                            f"Connection '{conn.connection_id}' has no disconnect method"
                        )
                        results[conn.connection_id] = False
                except Exception as e:
                    logger.error(f"Error disconnecting '{conn.connection_id}': {e}")
                    results[conn.connection_id] = False

        return results

    def get_status(self) -> dict[str, Any]:
        """
        Get status of all connections.

        Returns:
            Dictionary with connection status information
        """
        with self._connections_lock:
            connections_status = []

            for conn in self._connections.values():
                conn_status = conn.get_connection_params()
                connections_status.append(conn_status)

            admin_connections = self.get_all_connections_by_role(ConnectionRole.ADMIN)
            client_connections = self.get_all_connections_by_role(ConnectionRole.CLIENT)

            return {
                "total_connections": len(self._connections),
                "admin_connections": len(admin_connections),
                "client_connections": len(client_connections),
                "admin_connected": any(c.is_connected for c in admin_connections),
                "client_connected": any(c.is_connected for c in client_connections),
                "connections": connections_status,
            }


# Global instance accessor
_connection_manager: ConnectionManager | None = None
_connection_manager_lock = threading.Lock()


def get_connection_manager() -> ConnectionManager:
    """Get the singleton ConnectionManager instance."""
    global _connection_manager
    if _connection_manager is None:
        with _connection_manager_lock:
            if _connection_manager is None:
                _connection_manager = ConnectionManager()
    return _connection_manager
