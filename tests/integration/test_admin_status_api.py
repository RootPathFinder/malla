"""
Integration tests for admin status API endpoint.

Tests the /api/admin/node/<node_id>/status endpoint which checks
if an admin session is ready for a specific node.
"""

import pytest


class TestAdminNodeStatusEndpoint:
    """Test the /api/admin/node/<node_id>/status endpoint."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_node_status_basic(self, client):
        """Test basic admin node status endpoint."""
        # Use a test node ID
        response = client.get("/api/admin/node/!12345678/status")
        assert response.status_code == 200

        data = response.get_json()

        # Check required fields
        assert "node_id" in data
        assert "hex_id" in data
        assert "ready" in data
        assert "connection_type" in data
        assert "checks" in data
        assert "issues" in data
        assert "suggestions" in data
        assert "status_message" in data
        assert "status_level" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_node_status_checks_structure(self, client):
        """Test that checks have proper structure."""
        response = client.get("/api/admin/node/!abcd1234/status")
        assert response.status_code == 200

        data = response.get_json()
        checks = data.get("checks", [])

        # Should have at least connection and gateway checks
        assert len(checks) >= 2

        # Each check should have name, passed, and message
        for check in checks:
            assert "name" in check
            assert "passed" in check
            assert "message" in check
            assert isinstance(check["passed"], bool)

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_node_status_unknown_node(self, client):
        """Test status for a node that isn't in the administrable list."""
        response = client.get("/api/admin/node/!00000001/status")
        assert response.status_code == 200

        data = response.get_json()

        # Node should not be marked as administrable
        node_check = next(
            (c for c in data["checks"] if c["name"] == "node_administrable"),
            None,
        )
        assert node_check is not None
        assert node_check["passed"] is False

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_node_status_decimal_node_id(self, client):
        """Test status endpoint with decimal node ID."""
        response = client.get("/api/admin/node/305419896/status")
        assert response.status_code == 200

        data = response.get_json()
        assert "hex_id" in data
        # 305419896 = 0x12345678
        assert data["hex_id"] == "!12345678"

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_node_status_issues_and_suggestions(self, client):
        """Test that issues and suggestions are arrays."""
        response = client.get("/api/admin/node/!12345678/status")
        assert response.status_code == 200

        data = response.get_json()

        assert isinstance(data["issues"], list)
        assert isinstance(data["suggestions"], list)

    @pytest.mark.integration
    @pytest.mark.api
    def test_admin_node_status_level_values(self, client):
        """Test that status_level is one of expected values."""
        response = client.get("/api/admin/node/!12345678/status")
        assert response.status_code == 200

        data = response.get_json()
        assert data["status_level"] in ("success", "warning", "danger")
