"""Integration tests for chat routes."""

import os
import tempfile

import pytest

from malla.config import AppConfig
from malla.models.user import UserRole
from malla.services.auth_service import AuthService
from malla.web_ui import create_app
from tests.fixtures.database_fixtures import DatabaseFixtures


@pytest.fixture(scope="function")
def admin_enabled_app():
    """Create app with admin enabled."""
    # Create a temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    cfg = AppConfig(
        database_file=temp_db.name,
        admin_enabled=True,
    )

    try:
        app = create_app(cfg)
        app.config["TESTING"] = True

        # Set up test data
        db_fixtures = DatabaseFixtures()
        db_fixtures.create_test_database(temp_db.name)

        yield app
    finally:
        try:
            os.unlink(temp_db.name)
        except FileNotFoundError:
            pass


@pytest.fixture(scope="function")
def admin_enabled_client(admin_enabled_app):
    """Create test client with admin enabled."""
    return admin_enabled_app.test_client()


@pytest.fixture(scope="function")
def authenticated_operator_client(admin_enabled_app):
    """Create an authenticated test client with operator role."""
    with admin_enabled_app.app_context():
        # Create test operator user
        test_user = AuthService.create_user(
            username="test_chat_operator",
            password="test_password_123",
            role=UserRole.OPERATOR,
        )
        if test_user is None:
            # User may already exist, try to get it
            from malla.database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM users WHERE username = ?", ("test_chat_operator",)
            )
            row = cursor.fetchone()
            conn.close()
            if not row:
                raise RuntimeError("Failed to create test operator user")

    client = admin_enabled_app.test_client()

    # Log in the test user
    response = client.post(
        "/login",
        data={
            "username": "test_chat_operator",
            "password": "test_password_123",
        },
        follow_redirects=False,
    )

    # Login should redirect
    assert response.status_code == 302, (
        f"Login failed with status {response.status_code}"
    )

    return client


class TestChatPage:
    """Tests for the chat page."""

    @pytest.mark.integration
    def test_chat_page_requires_login(self, admin_enabled_client):
        """Test that chat page requires authentication."""
        response = admin_enabled_client.get("/chat")
        # Should redirect to login
        assert response.status_code in (302, 401)

    @pytest.mark.integration
    def test_chat_page_accessible_when_authenticated(
        self, authenticated_operator_client
    ):
        """Test that authenticated users can access chat page."""
        response = authenticated_operator_client.get("/chat")
        assert response.status_code == 200
        assert b"Mesh Chat" in response.data


class TestChatMessagesAPI:
    """Tests for the chat messages API."""

    @pytest.mark.integration
    def test_get_messages_requires_login(self, admin_enabled_client):
        """Test that getting messages requires authentication."""
        response = admin_enabled_client.get("/api/chat/messages")
        # Should return 401 or redirect
        assert response.status_code in (302, 401)

    @pytest.mark.integration
    def test_get_messages_returns_json(self, authenticated_operator_client):
        """Test that authenticated users can get messages."""
        response = authenticated_operator_client.get("/api/chat/messages")
        assert response.status_code == 200
        data = response.get_json()
        assert "messages" in data
        assert "count" in data
        assert isinstance(data["messages"], list)

    @pytest.mark.integration
    def test_get_messages_with_limit(self, authenticated_operator_client):
        """Test getting messages with limit parameter."""
        response = authenticated_operator_client.get("/api/chat/messages?limit=50")
        assert response.status_code == 200
        data = response.get_json()
        assert data["limit"] == 50


class TestChatChannelsAPI:
    """Tests for the chat channels API."""

    @pytest.mark.integration
    def test_get_channels_requires_login(self, admin_enabled_client):
        """Test that getting channels requires authentication."""
        response = admin_enabled_client.get("/api/chat/channels")
        assert response.status_code in (302, 401)

    @pytest.mark.integration
    def test_get_channels_returns_json(self, authenticated_operator_client):
        """Test that authenticated users can get channels."""
        response = authenticated_operator_client.get("/api/chat/channels")
        assert response.status_code == 200
        data = response.get_json()
        assert "channels" in data
        assert isinstance(data["channels"], list)


class TestChatSendAPI:
    """Tests for the chat send message API."""

    @pytest.mark.integration
    def test_send_message_requires_login(self, admin_enabled_client):
        """Test that sending messages requires authentication."""
        response = admin_enabled_client.post(
            "/api/chat/send",
            json={"text": "Hello"},
            content_type="application/json",
        )
        assert response.status_code in (302, 401)

    @pytest.mark.integration
    def test_send_message_requires_text(self, authenticated_operator_client):
        """Test that sending requires text field."""
        response = authenticated_operator_client.post(
            "/api/chat/send",
            json={},
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data
        assert "text" in data["error"].lower()

    @pytest.mark.integration
    def test_send_message_validates_length(self, authenticated_operator_client):
        """Test that message length is validated."""
        # 229 characters should be too long
        long_text = "a" * 229
        response = authenticated_operator_client.post(
            "/api/chat/send",
            json={"text": long_text},
            content_type="application/json",
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data
        assert "long" in data["error"].lower() or "228" in data["error"]


class TestChatConnectionStatusAPI:
    """Tests for the chat connection status API."""

    @pytest.mark.integration
    def test_connection_status_requires_login(self, admin_enabled_client):
        """Test that connection status requires authentication."""
        response = admin_enabled_client.get("/api/chat/connection-status")
        assert response.status_code in (302, 401)

    @pytest.mark.integration
    def test_connection_status_returns_json(self, authenticated_operator_client):
        """Test that connection status returns proper JSON."""
        response = authenticated_operator_client.get("/api/chat/connection-status")
        assert response.status_code == 200
        data = response.get_json()
        assert "tcp_connected" in data
        assert "bot_running" in data
        assert "can_send" in data
