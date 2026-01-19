"""
Unit tests for the ConnectionManager.
"""

import pytest

from malla.services.connection_manager import (
    ConnectionInfo,
    ConnectionManager,
    ConnectionRole,
    ConnectionType,
    get_connection_manager,
)


class MockPublisher:
    """Mock publisher for testing."""

    def __init__(self, publisher_id: str = "mock"):
        self.publisher_id = publisher_id
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        self._connected = True
        return True

    def disconnect(self) -> None:
        self._connected = False


@pytest.fixture
def clean_manager():
    """Provide a clean ConnectionManager for each test."""
    manager = get_connection_manager()
    # Clear all connections before each test
    for conn_id in list(manager._connections.keys()):
        manager.remove_connection(conn_id)
    yield manager
    # Clean up after test
    for conn_id in list(manager._connections.keys()):
        manager.remove_connection(conn_id)


class TestConnectionManager:
    """Test the ConnectionManager class."""

    def test_singleton(self, clean_manager):
        """Test that ConnectionManager is a singleton."""
        manager1 = get_connection_manager()
        manager2 = get_connection_manager()
        assert manager1 is manager2

    def test_add_connection(self, clean_manager):
        """Test adding a connection."""
        manager = clean_manager
        publisher = MockPublisher("test1")

        manager.add_connection(
            connection_id="test_conn",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=publisher,
            description="Test connection",
        )

        conn = manager.get_connection("test_conn")
        assert conn is not None
        assert conn.connection_id == "test_conn"
        assert conn.connection_type == ConnectionType.TCP
        assert conn.role == ConnectionRole.ADMIN
        assert conn.publisher is publisher

    def test_remove_connection(self, clean_manager):
        """Test removing a connection."""
        manager = clean_manager
        publisher = MockPublisher("test2")

        manager.add_connection(
            connection_id="test_remove",
            connection_type=ConnectionType.SERIAL,
            role=ConnectionRole.CLIENT,
            publisher=publisher,
        )

        assert manager.get_connection("test_remove") is not None
        success = manager.remove_connection("test_remove")
        assert success is True
        assert manager.get_connection("test_remove") is None

    def test_remove_nonexistent_connection(self, clean_manager):
        """Test removing a connection that doesn't exist."""
        manager = clean_manager
        success = manager.remove_connection("nonexistent")
        assert success is False

    def test_get_connection_by_role(self, clean_manager):
        """Test getting a connection by role."""
        manager = clean_manager

        admin_publisher = MockPublisher("admin")
        client_publisher = MockPublisher("client")

        manager.add_connection(
            connection_id="admin_conn",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=admin_publisher,
        )

        manager.add_connection(
            connection_id="client_conn",
            connection_type=ConnectionType.SERIAL,
            role=ConnectionRole.CLIENT,
            publisher=client_publisher,
        )

        admin_conn = manager.get_connection_by_role(ConnectionRole.ADMIN)
        assert admin_conn is not None
        assert admin_conn.role == ConnectionRole.ADMIN
        assert admin_conn.publisher is admin_publisher

        client_conn = manager.get_connection_by_role(ConnectionRole.CLIENT)
        assert client_conn is not None
        assert client_conn.role == ConnectionRole.CLIENT
        assert client_conn.publisher is client_publisher

    def test_get_all_connections_by_role(self, clean_manager):
        """Test getting all connections with a specific role."""
        manager = clean_manager

        # Add multiple admin connections
        for i in range(3):
            manager.add_connection(
                connection_id=f"admin_{i}",
                connection_type=ConnectionType.TCP,
                role=ConnectionRole.ADMIN,
                publisher=MockPublisher(f"admin_{i}"),
            )

        # Add client connection
        manager.add_connection(
            connection_id="client_1",
            connection_type=ConnectionType.SERIAL,
            role=ConnectionRole.CLIENT,
            publisher=MockPublisher("client_1"),
        )

        admin_conns = manager.get_all_connections_by_role(ConnectionRole.ADMIN)
        assert len(admin_conns) == 3

        client_conns = manager.get_all_connections_by_role(ConnectionRole.CLIENT)
        assert len(client_conns) == 1

    def test_get_admin_publisher(self, clean_manager):
        """Test getting an admin publisher."""
        manager = clean_manager

        admin_publisher = MockPublisher("admin")
        manager.add_connection(
            connection_id="admin_tcp",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=admin_publisher,
        )

        retrieved_publisher = manager.get_admin_publisher()
        assert retrieved_publisher is admin_publisher

    def test_get_client_publisher(self, clean_manager):
        """Test getting a client publisher."""
        manager = clean_manager

        client_publisher = MockPublisher("client")
        manager.add_connection(
            connection_id="client_serial",
            connection_type=ConnectionType.SERIAL,
            role=ConnectionRole.CLIENT,
            publisher=client_publisher,
        )

        retrieved_publisher = manager.get_client_publisher()
        assert retrieved_publisher is client_publisher

    def test_set_connection_role(self, clean_manager):
        """Test changing a connection's role."""
        manager = clean_manager

        publisher = MockPublisher("test")
        manager.add_connection(
            connection_id="test_conn",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=publisher,
        )

        # Change role to CLIENT
        success = manager.set_connection_role("test_conn", ConnectionRole.CLIENT)
        assert success is True

        conn = manager.get_connection("test_conn")
        assert conn is not None
        assert conn.role == ConnectionRole.CLIENT

    def test_connect_all(self, clean_manager):
        """Test connecting all connections."""
        manager = clean_manager

        # Add connections
        for i in range(3):
            publisher = MockPublisher(f"test_{i}")
            manager.add_connection(
                connection_id=f"conn_{i}",
                connection_type=ConnectionType.TCP,
                role=ConnectionRole.ADMIN if i % 2 == 0 else ConnectionRole.CLIENT,
                publisher=publisher,
                auto_connect=True,
            )

        # Connect all
        results = manager.connect_all()
        assert len(results) == 3
        assert all(results.values())

        # Verify all are connected
        for conn in manager.get_all_connections():
            assert conn.is_connected

    def test_connect_all_filtered_by_role(self, clean_manager):
        """Test connecting all connections filtered by role."""
        manager = clean_manager

        # Add admin and client connections
        admin_pub = MockPublisher("admin")
        client_pub = MockPublisher("client")

        manager.add_connection(
            connection_id="admin_conn",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=admin_pub,
        )

        manager.add_connection(
            connection_id="client_conn",
            connection_type=ConnectionType.SERIAL,
            role=ConnectionRole.CLIENT,
            publisher=client_pub,
        )

        # Connect only admin connections
        results = manager.connect_all(role=ConnectionRole.ADMIN)
        assert len(results) == 1
        assert results["admin_conn"] is True

        # Verify admin is connected, client is not
        admin_conn = manager.get_connection("admin_conn")
        client_conn = manager.get_connection("client_conn")
        assert admin_conn is not None and admin_conn.is_connected
        assert client_conn is not None and not client_conn.is_connected

    def test_disconnect_all(self, clean_manager):
        """Test disconnecting all connections."""
        manager = clean_manager

        # Add and connect connections
        for i in range(3):
            publisher = MockPublisher(f"test_{i}")
            publisher.connect()  # Pre-connect
            manager.add_connection(
                connection_id=f"conn_{i}",
                connection_type=ConnectionType.TCP,
                role=ConnectionRole.ADMIN,
                publisher=publisher,
            )

        # Disconnect all
        results = manager.disconnect_all()
        assert len(results) == 3
        assert all(results.values())

        # Verify all are disconnected
        for conn in manager.get_all_connections():
            assert not conn.is_connected

    def test_get_status(self, clean_manager):
        """Test getting overall connection status."""
        manager = clean_manager

        # Add mixed connections
        admin_pub = MockPublisher("admin")
        admin_pub.connect()
        manager.add_connection(
            connection_id="admin_tcp",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=admin_pub,
        )

        client_pub = MockPublisher("client")
        manager.add_connection(
            connection_id="client_serial",
            connection_type=ConnectionType.SERIAL,
            role=ConnectionRole.CLIENT,
            publisher=client_pub,
        )

        status = manager.get_status()

        assert status["total_connections"] == 2
        assert status["admin_connections"] == 1
        assert status["client_connections"] == 1
        assert status["admin_connected"] is True
        assert status["client_connected"] is False
        assert len(status["connections"]) == 2


class TestConnectionInfo:
    """Test the ConnectionInfo class."""

    def test_is_connected_property(self):
        """Test the is_connected property."""
        publisher = MockPublisher()
        conn_info = ConnectionInfo(
            connection_id="test",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=publisher,
        )

        assert conn_info.is_connected is False

        publisher.connect()
        assert conn_info.is_connected is True

    def test_get_connection_params(self):
        """Test getting connection parameters."""
        publisher = MockPublisher()
        publisher.tcp_host = "192.168.1.1"  # type: ignore[attr-defined]
        publisher.tcp_port = 4403  # type: ignore[attr-defined]

        conn_info = ConnectionInfo(
            connection_id="test_tcp",
            connection_type=ConnectionType.TCP,
            role=ConnectionRole.ADMIN,
            publisher=publisher,
            description="Test connection",
        )

        params = conn_info.get_connection_params()

        assert params["connection_id"] == "test_tcp"
        assert params["connection_type"] == "tcp"
        assert params["role"] == "admin"
        assert params["description"] == "Test connection"
        assert params["is_connected"] is False
        assert params["host"] == "192.168.1.1"
        assert params["port"] == 4403


class TestConnectionRole:
    """Test the ConnectionRole enum."""

    def test_role_values(self):
        """Test that role values are correct."""
        assert ConnectionRole.ADMIN.value == "admin"
        assert ConnectionRole.CLIENT.value == "client"

    def test_role_from_string(self):
        """Test creating role from string."""
        admin = ConnectionRole("admin")
        assert admin == ConnectionRole.ADMIN

        client = ConnectionRole("client")
        assert client == ConnectionRole.CLIENT


class TestConnectionType:
    """Test the ConnectionType enum."""

    def test_type_values(self):
        """Test that type values are correct."""
        assert ConnectionType.TCP.value == "tcp"
        assert ConnectionType.SERIAL.value == "serial"
        assert ConnectionType.MQTT.value == "mqtt"

    def test_type_from_string(self):
        """Test creating type from string."""
        tcp = ConnectionType("tcp")
        assert tcp == ConnectionType.TCP

        serial = ConnectionType("serial")
        assert serial == ConnectionType.SERIAL

        mqtt = ConnectionType("mqtt")
        assert mqtt == ConnectionType.MQTT
