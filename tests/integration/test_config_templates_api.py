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


class TestExtractTemplateFromNode:
    """Test extracting templates from nodes."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_template_missing_node_id(self, client):
        """Test extracting template without node_id fails."""
        response = client.post(
            "/api/admin/templates/extract-from-node",
            json={"config_type": "lora"},
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data
        assert "node_id" in data["error"].lower()

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_template_missing_config_type(self, client):
        """Test extracting template without config_type fails."""
        response = client.post(
            "/api/admin/templates/extract-from-node",
            json={"node_id": "12345678"},
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data
        assert "config_type" in data["error"].lower()

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_template_invalid_config_type(self, client):
        """Test extracting template with invalid config_type fails."""
        response = client.post(
            "/api/admin/templates/extract-from-node",
            json={"node_id": "12345678", "config_type": "invalid"},
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data
        assert "invalid" in data["error"].lower()

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_template_invalid_node_id(self, client):
        """Test extracting template with invalid node_id format fails."""
        response = client.post(
            "/api/admin/templates/extract-from-node",
            json={"node_id": "not-a-valid-id", "config_type": "lora"},
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "error" in data
        assert "invalid" in data["error"].lower()


class TestConfigTemplateSafetyValidation:
    """Test configuration safety validation."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_validate_safe_template(self, client):
        """Test validating a safe template returns no issues."""
        # Create a safe lora template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Safe LoRa Config",
                "template_type": "lora",
                "config_data": {"hop_limit": 3, "tx_power": 20},
            },
        )
        template_id = create_response.get_json()["template_id"]

        # Validate it
        response = client.post(f"/api/admin/templates/{template_id}/validate")
        assert response.status_code == 200

        data = response.get_json()
        assert data["is_safe"] is True
        assert len(data.get("blocking_issues", [])) == 0

    @pytest.mark.integration
    @pytest.mark.api
    def test_validate_dangerous_tx_disabled(self, client):
        """Test validating template with TX disabled shows blocking issue."""
        # Create a dangerous template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Dangerous LoRa Config",
                "template_type": "lora",
                "config_data": {"tx_enabled": False},
            },
        )
        template_id = create_response.get_json()["template_id"]

        # Validate it
        response = client.post(f"/api/admin/templates/{template_id}/validate")
        assert response.status_code == 200

        data = response.get_json()
        assert data["is_safe"] is False
        assert len(data["blocking_issues"]) > 0
        assert "CRITICAL" in data["blocking_issues"][0]

    @pytest.mark.integration
    @pytest.mark.api
    def test_deploy_dangerous_template_blocked(self, client):
        """Test deploying a dangerous template is blocked without force flag."""
        # Create a dangerous template
        create_response = client.post(
            "/api/admin/templates",
            json={
                "name": "Block Deploy Test",
                "template_type": "lora",
                "config_data": {"tx_enabled": False},
            },
        )
        template_id = create_response.get_json()["template_id"]

        # Try to deploy without force
        response = client.post(
            f"/api/admin/templates/{template_id}/deploy",
            json={"node_ids": [12345678]},
        )
        assert response.status_code == 400

        data = response.get_json()
        assert "blocking_issues" in data
        assert data["requires_force"] is True

    @pytest.mark.integration
    @pytest.mark.api
    def test_validate_template_not_found(self, client):
        """Test validating non-existent template returns 404."""
        response = client.post("/api/admin/templates/99999/validate")
        assert response.status_code == 404


class TestExtractTemplateSSE:
    """Test SSE streaming endpoint for config extraction."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_stream_missing_node_id(self, client):
        """Test SSE stream returns error when node_id is missing."""
        response = client.get(
            "/api/admin/templates/extract-from-node/stream",
            query_string={"config_type": "lora"},
        )
        assert response.status_code == 200
        assert response.content_type.startswith("text/event-stream")

        # Read the SSE data
        import json

        data_lines = [
            line
            for line in response.get_data(as_text=True).split("\n")
            if line.startswith("data:")
        ]
        assert len(data_lines) > 0

        event_data = json.loads(data_lines[0].replace("data: ", ""))
        assert event_data["complete"] is True
        assert event_data["success"] is False
        assert "node_id" in event_data["error"]

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_stream_missing_config_type(self, client):
        """Test SSE stream returns error when config_type is missing."""
        response = client.get(
            "/api/admin/templates/extract-from-node/stream",
            query_string={"node_id": "!12345678"},
        )
        assert response.status_code == 200
        assert response.content_type.startswith("text/event-stream")

        import json

        data_lines = [
            line
            for line in response.get_data(as_text=True).split("\n")
            if line.startswith("data:")
        ]
        assert len(data_lines) > 0

        event_data = json.loads(data_lines[0].replace("data: ", ""))
        assert event_data["complete"] is True
        assert event_data["success"] is False
        assert "config_type" in event_data["error"]

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_stream_invalid_node_id(self, client):
        """Test SSE stream returns error for invalid node ID format."""
        response = client.get(
            "/api/admin/templates/extract-from-node/stream",
            query_string={"node_id": "not-a-valid-id", "config_type": "lora"},
        )
        assert response.status_code == 200
        assert response.content_type.startswith("text/event-stream")

        import json

        data_lines = [
            line
            for line in response.get_data(as_text=True).split("\n")
            if line.startswith("data:")
        ]
        assert len(data_lines) > 0

        event_data = json.loads(data_lines[0].replace("data: ", ""))
        assert event_data["complete"] is True
        assert event_data["success"] is False
        assert "invalid" in event_data["error"].lower()

    @pytest.mark.integration
    @pytest.mark.api
    def test_extract_stream_invalid_config_type(self, client):
        """Test SSE stream returns error for invalid config type."""
        response = client.get(
            "/api/admin/templates/extract-from-node/stream",
            query_string={"node_id": "!12345678", "config_type": "invalid_type"},
        )
        assert response.status_code == 200
        assert response.content_type.startswith("text/event-stream")

        import json

        data_lines = [
            line
            for line in response.get_data(as_text=True).split("\n")
            if line.startswith("data:")
        ]
        assert len(data_lines) > 0

        event_data = json.loads(data_lines[0].replace("data: ", ""))
        assert event_data["complete"] is True
        assert event_data["success"] is False
        assert "invalid" in event_data["error"].lower()
