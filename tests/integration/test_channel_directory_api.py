"""
Integration tests for the Channel Directory API endpoints.
"""

import pytest


class TestChannelDirectoryAPI:
    """Test /api/bot/channels endpoints."""

    @pytest.mark.integration
    @pytest.mark.api
    def test_list_channels_empty(self, client):
        """Empty directory returns empty list."""
        response = client.get("/api/bot/channels")
        assert response.status_code == 200
        data = response.get_json()
        assert "channels" in data
        assert isinstance(data["channels"], list)
        assert data["count"] == len(data["channels"])

    @pytest.mark.integration
    @pytest.mark.api
    def test_add_channel(self, client):
        """Add a channel via the API."""
        response = client.post(
            "/api/bot/channels",
            json={
                "channel_name": "Dispatches",
                "psk": "AQ==",
                "description": "EMS/Fire dispatches",
            },
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert data["channel"]["channel_name"] == "Dispatches"

    @pytest.mark.integration
    @pytest.mark.api
    def test_add_channel_missing_name(self, client):
        """Missing channel_name returns 400."""
        response = client.post(
            "/api/bot/channels",
            json={"psk": "AQ=="},
        )
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    @pytest.mark.integration
    @pytest.mark.api
    def test_add_duplicate_channel(self, client):
        """Duplicate channel name returns 409."""
        client.post(
            "/api/bot/channels",
            json={"channel_name": "DupTest", "psk": "AQ=="},
        )
        response = client.post(
            "/api/bot/channels",
            json={"channel_name": "DupTest", "psk": "BQ=="},
        )
        assert response.status_code == 409

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_channel_info(self, client):
        """Get a single channel by name."""
        client.post(
            "/api/bot/channels",
            json={
                "channel_name": "InfoTest",
                "psk": "AQ==",
                "description": "Test channel",
            },
        )
        response = client.get("/api/bot/channels/InfoTest")
        assert response.status_code == 200
        data = response.get_json()
        assert data["channel_name"] == "InfoTest"
        assert data["psk"] == "AQ=="

    @pytest.mark.integration
    @pytest.mark.api
    def test_get_channel_not_found(self, client):
        """Non-existent channel returns 404."""
        response = client.get("/api/bot/channels/NoSuch")
        assert response.status_code == 404

    @pytest.mark.integration
    @pytest.mark.api
    def test_update_channel(self, client):
        """Update a channel's description."""
        client.post(
            "/api/bot/channels",
            json={"channel_name": "UpdTest", "psk": "AQ=="},
        )
        response = client.put(
            "/api/bot/channels/UpdTest",
            json={"description": "Updated description"},
        )
        assert response.status_code == 200
        assert response.get_json()["success"] is True

        # Verify
        info = client.get("/api/bot/channels/UpdTest").get_json()
        assert info["description"] == "Updated description"

    @pytest.mark.integration
    @pytest.mark.api
    def test_update_channel_not_found(self, client):
        """Update non-existent channel returns 404."""
        response = client.put(
            "/api/bot/channels/NoSuch",
            json={"psk": "X==="},
        )
        assert response.status_code == 404

    @pytest.mark.integration
    @pytest.mark.api
    def test_delete_channel(self, client):
        """Delete a channel from the directory."""
        client.post(
            "/api/bot/channels",
            json={"channel_name": "DelTest", "psk": "AQ=="},
        )
        response = client.delete("/api/bot/channels/DelTest")
        assert response.status_code == 200
        assert response.get_json()["success"] is True

        # Verify it's gone
        response = client.get("/api/bot/channels/DelTest")
        assert response.status_code == 404

    @pytest.mark.integration
    @pytest.mark.api
    def test_delete_channel_not_found(self, client):
        """Delete non-existent channel returns 404."""
        response = client.delete("/api/bot/channels/NoSuch")
        assert response.status_code == 404

    @pytest.mark.integration
    @pytest.mark.api
    def test_list_active_only(self, client):
        """Active-only filter works correctly."""
        client.post(
            "/api/bot/channels",
            json={"channel_name": "Active1", "psk": "AQ=="},
        )
        client.post(
            "/api/bot/channels",
            json={"channel_name": "Disabled1", "psk": "BQ=="},
        )
        client.put(
            "/api/bot/channels/Disabled1",
            json={"active": False},
        )

        # Default (active_only=true)
        resp = client.get("/api/bot/channels")
        data = resp.get_json()
        names = [ch["channel_name"] for ch in data["channels"]]
        assert "Active1" in names
        assert "Disabled1" not in names

        # active_only=false
        resp = client.get("/api/bot/channels?active_only=false")
        data = resp.get_json()
        names = [ch["channel_name"] for ch in data["channels"]]
        assert "Active1" in names
        assert "Disabled1" in names
