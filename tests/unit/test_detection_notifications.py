"""Tests for detection-sensor notification catalog and related routes."""

from __future__ import annotations

import time

import pytest


def _insert_detection(app, *, node_id: int, name: str, ts: float | None = None):
    from malla.database.connection import get_db_connection

    payload = name.encode("utf-8")
    with app.app_context():
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO packet_history (
                timestamp, topic, from_node_id, to_node_id, portnum, portnum_name,
                gateway_id, rssi, snr, hop_limit, payload_length, raw_payload,
                processed_successfully
            ) VALUES (?, ?, ?, ?, 10, 'DETECTION_SENSOR_APP', ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                ts if ts is not None else time.time(),
                "msh/test",
                node_id,
                0xFFFFFFFF,
                "!gateway",
                -80,
                4.0,
                3,
                len(payload),
                payload,
            ),
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO node_info (node_id, hex_id, long_name, short_name, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                f"!{node_id:08x}",
                f"Node {node_id:08x}",
                "N1",
                time.time(),
                time.time(),
            ),
        )
        conn.commit()
        conn.close()


@pytest.mark.unit
class TestDetectionNotificationCatalog:
    def test_catalog_groups_node_and_sensor(self, client, app):
        node_a = 0xAABBCC01
        node_b = 0xAABBCC02
        _insert_detection(app, node_id=node_a, name="driveway")
        _insert_detection(app, node_id=node_a, name="driveway")
        _insert_detection(app, node_id=node_a, name="gate")
        _insert_detection(app, node_id=node_b, name="driveway")

        res = client.get("/api/detection-sensors/catalog?hours=24")
        assert res.status_code == 200, res.get_data(as_text=True)
        body = res.get_json()
        assert body["total"] >= 3
        sensors = body["sensors"]
        driveway_a = next(
            s
            for s in sensors
            if s["node_id"] == node_a and s["sensor_name"] == "driveway"
        )
        assert driveway_a["event_count"] == 2
        assert driveway_a["node_hex"] == "!aabbcc01"
        names = {(s["node_id"], s["sensor_name"]) for s in sensors}
        assert (node_a, "gate") in names
        assert (node_b, "driveway") in names

    def test_service_worker_and_manifest_routes(self, client):
        sw = client.get("/sw.js")
        assert sw.status_code == 200
        assert "SHOW_NOTIFICATION" in sw.get_data(as_text=True)
        assert sw.headers.get("Service-Worker-Allowed") == "/"

        manifest = client.get("/manifest.webmanifest")
        assert manifest.status_code == 200
        assert "Malla" in manifest.get_data(as_text=True)

    def test_profile_page_has_detection_alert_controls(self, operator_client):
        res = operator_client.get("/profile")
        assert res.status_code == 200
        html = res.get_data(as_text=True)
        assert "Detection Alerts" in html
        assert "pref-detection-notifications-enabled" in html
        assert "notifySensorCatalog" in html
