"""
Integration tests for configuration template API endpoints.

Tests the template CRUD operations and deployment functionality.
"""

import pytest


class TestConfigTemplateAPI:
    """Test configuration template CRUD operations."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_templates_empty(self, client):
        """Test getting templates when none exist."""
        response = client.get("/api/admin/templates")
        assert response.status_code == 200

        data = response.get_json()
        assert "templates" in data
        assert "count" in data
        assert isinstance(data["templates"], list)

    @pytest.mark.integration
    @pytest.mark.api
    def test_create_template(self, client):
        """Test creating a new configuration template."""
        response = client.post(
            "/api/admin/templates",
            json={
                "name": "Test LoRa Config",
                "template_type": "lora",
                "description": "Test template for LoRa settings",
                "config_data": {"region": "US", "modem_preset": "LONG_FAST"},
            },
        )
        assert response.status_code == 200

        data = response.get_json()
        assert data["success"] is True
        assert "template_id" in data
        assert data["template_id"] > 0

    @pytest.mark.integration
    @pytest.mark.api
    def test_create_template_missing_name(self, client):
        """Test creating a template without a name fails."""
        response = client.post(
            "/api/admin/templates",
            json={
                "template_type": "lora",
                "config_data": {"region": "US"},
            },
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data
        assert "name" in data["error"].lower()

    @pytest.mark.integration
    @pytest.mark.api
    def test_create_template_invalid_type(self, client):
        """Test creating a template with invalid type fails."""
        response = client.post(
            "/api/admin/templates",
            json={
                "name": "Invalid Type Template",
                "template_type": "invalid_type",
                "config_data": {"key": "value"},
            },
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data
        assert "template_type" in data["error"]

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_template_by_id(self, client):
        """Test getting a specific template by ID."""
        # First create a template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Get By ID Test",
                "template_type": "device",
                "config_data": {"debug_log_enabled": True},
            },
        )
        assert create_response.status_code == 200
        template_id = create_response.get_json()["template_id"]

        # Now get it
        response = client.get(f"/api/admin/templates/{template_id}")
        assert response.status_code == 200

        data = response.get_json()
        assert data["name"] == "Get By ID Test"
        assert data["template_type"] == "device"
        assert isinstance(data["config_data"], dict)

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_template_not_found(self, client):
        """Test getting a non-existent template returns 404."""
        response = client.get("/api/admin/templates/99999")
        assert response.status_code == 404

        data = response.get_json()
        assert "error" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_update_template(self, client):
        """Test updating an existing template."""
        # Create a template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Update Test Original",
                "template_type": "lora",
                "config_data": {"region": "US"},
            },
        )
        template_id = create_response.get_json()["template_id"]

        # Update it
        response = client.put(
            f"/api/admin/templates/{template_id}",
            json={
                "name": "Update Test Modified",
                "description": "Updated description",
                "config_data": {"region": "EU_868"},
            },
        )
        assert response.status_code == 200

        data = response.get_json()
        assert data["success"] is True

        # Verify the update
        get_response = client.get(f"/api/admin/templates/{template_id}")
        updated = get_response.get_json()
        assert updated["name"] == "Update Test Modified"
        assert updated["description"] == "Updated description"

    @pytest.mark.integration
    @pytest.mark.api
    def test_delete_template(self, client):
        """Test deleting a template."""
        # Create a template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Delete Test",
                "template_type": "channel",
                "config_data": {"name": "test", "index": 0},
            },
        )
        template_id = create_response.get_json()["template_id"]

        # Delete it
        response = client.delete(f"/api/admin/templates/{template_id}")
        assert response.status_code == 200

        data = response.get_json()
        assert data["success"] is True

        # Verify deletion
        get_response = client.get(f"/api/admin/templates/{template_id}")
        assert get_response.status_code == 404

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_templates_with_type_filter(self, client):
        """Test filtering templates by type."""
        # Create templates of different types
        client.post(
            "/api/admin/templates",
            json={
                "name": "Filter Test Lora",
                "template_type": "lora",
                "config_data": {"region": "US"},
            },
        )
        client.post(
            "/api/admin/templates",
            json={
                "name": "Filter Test Device",
                "template_type": "device",
                "config_data": {"debug": False},
            },
        )

        # Filter by lora type
        response = client.get("/api/admin/templates?type=lora")
        assert response.status_code == 200

        data = response.get_json()
        # All returned templates should be lora type
        for template in data["templates"]:
            assert template["template_type"] == "lora"


class TestConfigTemplateDeploymentAPI:
    """Test template deployment functionality."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_deploy_template_no_nodes(self, client):
        """Test deploying a template with no nodes selected."""
        # Create a template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Deploy Test",
                "template_type": "lora",
                "config_data": {"region": "US"},
            },
        )
        template_id = create_response.get_json()["template_id"]

        # Try to deploy with no nodes
        response = client.post(
            f"/api/admin/templates/{template_id}/deploy",
            json={"node_ids": []},
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_deploy_template_not_found(self, client):
        """Test deploying a non-existent template."""
        response = client.post(
            "/api/admin/templates/99999/deploy",
            json={"node_ids": [12345678]},
        )
        assert response.status_code == 404

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_deployment_history_empty(self, client):
        """Test getting deployment history when empty."""
        response = client.get("/api/admin/deployments")
        assert response.status_code == 200

        data = response.get_json()
        assert "deployments" in data
        assert "count" in data
        assert isinstance(data["deployments"], list)
