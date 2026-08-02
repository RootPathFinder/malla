"""Tests for detection-sensor notification catalog and related routes."""

from __future__ import annotations

import time

import pytest


def _insert_detection(
    app,
    *,
    node_id: int,
    name: str,
    ts: float | None = None,
    long_name: str | None = None,
    short_name: str | None = None,
):
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
                long_name or f"Node {node_id:08x}",
                short_name or f"N{node_id & 0xFF:02X}",
                time.time(),
                time.time(),
            ),
        )
        conn.commit()
        conn.close()


def _clear_api_cache():
    from malla.utils.cache_utils import clear_cache

    clear_cache()


@pytest.mark.unit
class TestDetectionNotificationCatalog:
    def test_catalog_groups_node_and_sensor(self, client, app):
        node_a = 0xAABBCC01
        node_b = 0xAABBCC02
        _insert_detection(
            app, node_id=node_a, name="driveway", long_name="Gate Sensor", short_name="GS1"
        )
        _insert_detection(
            app, node_id=node_a, name="driveway", long_name="Gate Sensor", short_name="GS1"
        )
        _insert_detection(
            app, node_id=node_a, name="gate", long_name="Gate Sensor", short_name="GS1"
        )
        _insert_detection(
            app, node_id=node_b, name="driveway", long_name="Gate Sensor", short_name="GS2"
        )

        _clear_api_cache()
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
        assert driveway_a["long_name"] == "Gate Sensor"
        assert driveway_a["short_name"] == "GS1"
        assert driveway_a["node_name"] == "Gate Sensor (GS1)"
        driveway_b = next(
            s
            for s in sensors
            if s["node_id"] == node_b and s["sensor_name"] == "driveway"
        )
        assert driveway_b["short_name"] == "GS2"
        assert driveway_b["node_name"] == "Gate Sensor (GS2)"
        names = {(s["node_id"], s["sensor_name"]) for s in sensors}
        assert (node_a, "gate") in names
        assert (node_b, "driveway") in names

    def test_catalog_collapses_dwell_suffix_into_sensor_name(self, client, app):
        node_id = 0xAABBCC10
        now = time.time()
        _insert_detection(app, node_id=node_id, name="driveway detected", ts=now - 30)
        _insert_detection(
            app,
            node_id=node_id,
            name="driveway detected dwell_ms=2100",
            ts=now - 10,
        )
        _insert_detection(
            app,
            node_id=node_id,
            name="driveway detected dwell_ms=1995",
            ts=now - 1,
        )

        _clear_api_cache()
        # Unique query avoids colliding with other catalog tests' response cache
        res = client.get("/api/detection-sensors/catalog?hours=23")
        assert res.status_code == 200
        sensors = res.get_json()["sensors"]
        matches = [s for s in sensors if s["node_id"] == node_id]
        assert matches, f"expected node {node_id} in catalog, got {sensors!r}"
        driveway = next(
            (s for s in matches if s["sensor_name"] == "driveway"),
            None,
        )
        assert driveway is not None, f"expected sensor_name=driveway, got {matches!r}"
        assert driveway["event_count"] == 3
        assert driveway["last_dwell_ms"] == 1995

    def test_detection_events_expose_dwell_ms(self, client, app):
        node_id = 0xAABBCC11
        _insert_detection(
            app, node_id=node_id, name="driveway detected dwell_ms=2048"
        )
        _clear_api_cache()
        res = client.get("/api/detection-sensors?hours=24&limit=20")
        assert res.status_code == 200
        events = res.get_json()["events"]
        match = next(e for e in events if e["from_node_id"] == node_id)
        assert match["detection_name"] == "driveway"
        assert match["detection_text"] == "driveway detected dwell_ms=2048"
        assert match["event_kind"] == "trip"
        assert match["dwell_ms"] == 2048

    def test_sensor_nodes_include_short_name(self, client, app):
        node_a = 0xAABBCC21
        node_b = 0xAABBCC22
        _insert_detection(
            app,
            node_id=node_a,
            name="driveway detected",
            long_name="Gate Sensor",
            short_name="GS1",
        )
        _insert_detection(
            app,
            node_id=node_b,
            name="driveway detected",
            long_name="Gate Sensor",
            short_name="GS2",
        )
        _clear_api_cache()
        res = client.get("/api/detection-sensors?hours=24&limit=50")
        assert res.status_code == 200
        nodes = res.get_json()["sensor_nodes"]
        by_id = {n["node_id"]: n for n in nodes}
        assert by_id[node_a]["name"] == "Gate Sensor"
        assert by_id[node_a]["short_name"] == "GS1"
        assert by_id[node_b]["name"] == "Gate Sensor"
        assert by_id[node_b]["short_name"] == "GS2"

    def test_detection_sensors_page_has_short_name_helper(self, client):
        html = client.get("/detection-sensors")
        if html.status_code != 200:
            return
        body = html.get_data(as_text=True)
        assert "formatNodeWithShort" in body
        assert "nodeShortLabel" in body

    def test_profile_catalog_renders_short_name_helper(self, operator_client):
        res = operator_client.get("/profile")
        assert res.status_code == 200
        html = res.get_data(as_text=True)
        assert "nodeShortName" in html
        assert "nodePrimaryName" in html

    def test_detection_ui_mentions_dwell(self, client):
        # Page may require auth depending on config; static script always available
        js = client.get("/static/js/detection-notifications.js")
        assert js.status_code == 200
        assert "dwell" in js.get_data(as_text=True).lower()
        html = client.get("/detection-sensors")
        if html.status_code == 200:
            assert "formatDwell" in html.get_data(as_text=True)

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
        assert "notifyCapabilityHint" in html
        assert "Add to Home Screen" in html
        assert "Chrome cannot show system notifications" in html

    def test_detection_notifications_js_has_ios_in_app_fallback(self, client):
        res = client.get("/static/js/detection-notifications.js")
        assert res.status_code == 200
        js = res.get_data(as_text=True)
        assert "getCapability" in js
        assert "showInAppToast" in js
        assert "CriOS" in js
        assert "in_app" in js
        assert "Add to Home Screen" in js
