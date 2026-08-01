"""API tests for PAX ID profile endpoints."""

from __future__ import annotations

import time

import pytest


def _insert_pax_packet(app, *, timestamp: float, payload: bytes, from_node_id: int = 0xAABBCCDD):
    from malla.database.connection import get_db_connection

    with app.app_context():
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO packet_history (
                timestamp, topic, from_node_id, to_node_id, portnum, portnum_name,
                gateway_id, rssi, snr, hop_limit, payload_length, raw_payload,
                processed_successfully
            ) VALUES (?, ?, ?, ?, 34, 'PAXCOUNTER_APP', ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                timestamp,
                "msh/test",
                from_node_id,
                0xFFFFFFFF,
                "!gateway",
                -90,
                5.0,
                3,
                len(payload),
                payload,
            ),
        )
        conn.commit()
        conn.close()


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

    def test_fingerprint_profile_roundtrip(self, client):
        profile_id = "fp:abcd"
        put = client.put(
            f"/api/paxcounter/profiles/{profile_id}",
            json={"nickname": "Rotating BLE", "notes": "soft sticky id"},
        )
        assert put.status_code == 200, put.get_data(as_text=True)
        body = put.get_json()
        assert body["success"] is True
        assert body["profile"]["mac"] == profile_id
        assert body["profile"]["nickname"] == "Rotating BLE"

        detail = client.get(f"/api/paxcounter/profiles/{profile_id}?hours=24")
        assert detail.status_code == 200
        detail_body = detail.get_json()
        assert detail_body["profile"]["nickname"] == "Rotating BLE"
        assert detail_body["stats"]["id"] == profile_id or detail_body["stats"]["mac"] == profile_id

        deleted = client.delete(f"/api/paxcounter/profiles/{profile_id}")
        assert deleted.status_code == 200
        assert deleted.get_json()["deleted"] is True

    def test_fingerprint_history_and_status_page(self, client, app):
        from malla.vendor.meshtastic import paxcount_pb2

        now = time.time()
        msg = paxcount_pb2.Paxcount(wifi=0, ble=1, uptime=10, sighting_count=1)
        s = msg.sightings.add()
        s.mac = bytes([0x11, 0x22, 0x33, 0x44, 0x55, 0x66])
        s.kind = paxcount_pb2.PaxSighting.BLE_APPLE
        s.rssi = -62
        s.fingerprint = bytes([0xAB, 0xCD])
        payload = msg.SerializeToString()
        _insert_pax_packet(app, timestamp=now - 120, payload=payload)

        client.put(
            "/api/paxcounter/profiles/fp:abcd",
            json={"nickname": "Walk-up phone"},
        )

        hist = client.get(
            "/api/paxcounter/profiles/fp:abcd/history?hours=24&bucket_minutes=15"
        )
        assert hist.status_code == 200, hist.get_data(as_text=True)
        body = hist.get_json()
        assert body["id"] == "fp:abcd"
        assert body["profile"]["nickname"] == "Walk-up phone"
        assert body["summary"]["hit_count"] >= 1
        assert body["summary"]["best_rssi"] == -62
        assert any(s.get("rssi") == -62 for s in body["samples"])
        assert body["presence"]
        assert body["summary"]["present_now"] is True
        assert body["summary"]["nearest_m"] is not None
        assert body["summary"]["nearest_label"]
        assert any(s.get("distance_m") is not None for s in body["samples"])

        page = client.get("/paxcounter/id/fp:abcd")
        assert page.status_code == 200
        html = page.get_data(as_text=True)
        assert "PAX ID Status" in html
        assert "fp:abcd" in html
        assert "rssiChart" in html
        assert "distanceChart" in html
        assert "presenceChart" in html
        assert "Approx distance" in html
