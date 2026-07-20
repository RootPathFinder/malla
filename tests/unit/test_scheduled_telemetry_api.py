"""API tests for scheduled solicited telemetry admin endpoints."""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestScheduledTelemetryApi:
    def test_upsert_and_list_via_api(self, operator_client):
        node_hex = "!0a11c001"
        resp = operator_client.post(
            f"/api/admin/node/{node_hex}/telemetry/schedule",
            json={
                "interval_minutes": 30,
                "telemetry_types": ["device_metrics", "environment_metrics"],
                "enabled": True,
            },
        )
        if resp.status_code == 403:
            pytest.skip("Admin disabled in test app")
        assert resp.status_code == 200, resp.get_data(as_text=True)
        data = resp.get_json()
        assert data["success"] is True
        assert data["schedule"]["interval_seconds"] == 1800
        assert data["schedule"]["telemetry_types"] == [
            "device_metrics",
            "environment_metrics",
        ]

        listed = operator_client.get("/api/admin/telemetry/schedules")
        assert listed.status_code == 200
        body = listed.get_json()
        assert body["success"] is True
        assert body["min_interval_minutes"] == 30
        assert any(s["hex_id"].lower() == node_hex for s in body["schedules"])

        # Cleanup so later tests see a clean slate
        operator_client.delete(f"/api/admin/node/{node_hex}/telemetry/schedule")

    def test_rejects_interval_under_30_minutes(self, operator_client):
        resp = operator_client.post(
            "/api/admin/node/!0a11c001/telemetry/schedule",
            json={"interval_minutes": 15, "telemetry_types": ["device_metrics"]},
        )
        if resp.status_code == 403:
            pytest.skip("Admin disabled in test app")
        assert resp.status_code == 400
        data = resp.get_json()
        assert data["success"] is False
        assert "30" in data["error"]

    def test_delete_schedule(self, operator_client):
        create = operator_client.post(
            "/api/admin/node/!0a11c001/telemetry/schedule",
            json={"interval_minutes": 60, "telemetry_types": ["device_metrics"]},
        )
        if create.status_code == 403:
            pytest.skip("Admin disabled in test app")
        assert create.status_code == 200

        deleted = operator_client.delete(
            "/api/admin/node/!0a11c001/telemetry/schedule"
        )
        assert deleted.status_code == 200
        assert deleted.get_json()["deleted"] is True
