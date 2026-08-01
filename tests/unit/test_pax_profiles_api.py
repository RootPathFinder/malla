"""API tests for PAX ID profile endpoints."""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestPaxProfilesApi:
    def test_upsert_list_get_and_delete(self, client):
        mac = "aa:bb:cc:dd:ee:ff"
        put = client.put(
            f"/api/paxcounter/profiles/{mac}",
            json={"nickname": "Kitchen AP", "notes": "2.4GHz"},
        )
        assert put.status_code == 200, put.get_data(as_text=True)
        body = put.get_json()
        assert body["success"] is True
        assert body["profile"]["nickname"] == "Kitchen AP"

        listed = client.get("/api/paxcounter/profiles?q=kitchen")
        assert listed.status_code == 200
        profiles = listed.get_json()["profiles"]
        assert len(profiles) == 1
        assert profiles[0]["mac"] == mac

        detail = client.get(f"/api/paxcounter/profiles/{mac}?hours=24")
        assert detail.status_code == 200
        detail_body = detail.get_json()
        assert detail_body["profile"]["nickname"] == "Kitchen AP"
        assert detail_body["stats"]["mac"] == mac

        deleted = client.delete(f"/api/paxcounter/profiles/{mac}")
        assert deleted.status_code == 200
        assert deleted.get_json()["deleted"] is True

        gone = client.get(f"/api/paxcounter/profiles/{mac}")
        assert gone.status_code == 200
        assert gone.get_json()["profile"] is None

    def test_rejects_invalid_mac(self, client):
        resp = client.put(
            "/api/paxcounter/profiles/not-a-mac",
            json={"nickname": "Nope"},
        )
        assert resp.status_code == 400
