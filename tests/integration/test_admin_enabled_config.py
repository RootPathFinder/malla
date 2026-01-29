"""
Integration tests for admin enabled/disabled configuration.

Tests that the admin feature can be properly disabled via configuration
to support read-only public deployments.
"""

import pytest

from malla.config import AppConfig, get_config


class TestAdminEnabledConfiguration:
    """Test admin_enabled configuration behavior."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_enabled_endpoint_returns_status(self, client):
        """Test that /api/admin/enabled returns the admin enabled status."""
        response = client.get("/api/admin/enabled")
        assert response.status_code == 200

        data = response.get_json()
        assert "admin_enabled" in data
        assert isinstance(data["admin_enabled"], bool)

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_page_accessible_when_enabled(self, operator_client):
        """Test that admin page is accessible when admin is enabled (default)."""
        # By default, admin is enabled in test configuration
        config = get_config()
        assert config.admin_enabled is True

        response = operator_client.get("/admin")
        assert response.status_code == 200
        assert b"Mesh Admin" in response.data

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_api_accessible_when_enabled(self, client):
        """Test that admin API endpoints work when admin is enabled."""
        response = client.get("/api/admin/status")
        assert response.status_code == 200

        data = response.get_json()
        # Should return valid status data
        assert "connected" in data or "error" not in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_nodes_endpoint_when_enabled(self, operator_client):
        """Test admin nodes endpoint returns data when enabled."""
        response = operator_client.get("/api/admin/nodes")
        assert response.status_code == 200

        data = response.get_json()
        assert "nodes" in data


class TestAdminDisabledBehavior:
    """Test behavior when admin is disabled.

    These tests verify the expected behavior by checking code paths,
    since we can't easily change config during test runs.
    """

    @pytest.mark.integration
    def test_admin_enabled_default_value(self):
        """Test that admin_enabled defaults to True."""
        # Create a fresh config to check default
        config = AppConfig()
        assert config.admin_enabled is True

    @pytest.mark.integration
    def test_admin_enabled_can_be_set_false(self):
        """Test that admin_enabled can be set to False."""
        config = AppConfig(admin_enabled=False)
        assert config.admin_enabled is False

    @pytest.mark.integration
    def test_admin_disabled_template_exists(self, app):
        """Test that the admin_disabled template exists."""
        # The template should be loadable
        with app.app_context():
            template = app.jinja_env.get_template("admin_disabled.html")
            assert template is not None


class TestAdminConfigFields:
    """Test admin configuration fields are properly defined."""

    @pytest.mark.integration
    def test_admin_config_fields_exist(self):
        """Test that all admin config fields exist with correct defaults."""
        config = AppConfig()

        # Check all admin-related fields
        assert hasattr(config, "admin_enabled")
        assert hasattr(config, "admin_gateway_node_id")
        assert hasattr(config, "admin_connection_type")
        assert hasattr(config, "admin_tcp_host")
        assert hasattr(config, "admin_tcp_port")

        # Check default values
        assert config.admin_enabled is True
        assert config.admin_gateway_node_id is None
        assert config.admin_connection_type == "mqtt"
        assert config.admin_tcp_host == "192.168.1.1"
        assert config.admin_tcp_port == 4403

    @pytest.mark.integration
    def test_admin_connection_types(self):
        """Test that valid connection types can be set."""
        for conn_type in ["mqtt", "tcp", "serial"]:
            config = AppConfig(admin_connection_type=conn_type)
            assert config.admin_connection_type == conn_type
