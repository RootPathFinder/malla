"""Tests for channel utilization investigation page/APIs."""

from __future__ import annotations

import time

import pytest

from malla.database.utilization import UtilizationRepository


def _setup_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS node_info (
            node_id INTEGER PRIMARY KEY,
            hex_id TEXT,
            long_name TEXT,
            short_name TEXT,
            role TEXT,
            first_seen REAL,
            last_updated REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS telemetry_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            node_id INTEGER NOT NULL,
            battery_level INTEGER,
            voltage REAL,
            channel_utilization REAL,
            air_util_tx REAL,
            uptime_seconds INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS packet_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            topic TEXT,
            from_node_id INTEGER,
            to_node_id INTEGER,
            portnum INTEGER,
            portnum_name TEXT,
            gateway_id TEXT,
            rssi REAL,
            snr REAL,
            hop_limit INTEGER,
            payload_length INTEGER,
            raw_payload BLOB,
            processed_successfully INTEGER
        )
        """
    )
    conn.commit()


@pytest.fixture
def util_db(app):
    from malla.database.connection import get_db_connection

    with app.app_context():
        conn = get_db_connection()
        _setup_tables(conn)
        # Isolate from fixture seed data
        conn.execute("DELETE FROM telemetry_data")
        conn.execute("DELETE FROM packet_history")
        now = time.time()
        busy_id = 0xAA011111
        quiet_id = 0xBB022222
        conn.execute(
            """
            INSERT OR REPLACE INTO node_info
                (node_id, hex_id, long_name, short_name, role, first_seen, last_updated)
            VALUES (?,?,?,?,?,?,?)
            """,
            (busy_id, f"!{busy_id:08x}", "Busy Node", "BUSY", "ROUTER", now, now),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO node_info
                (node_id, hex_id, long_name, short_name, role, first_seen, last_updated)
            VALUES (?,?,?,?,?,?,?)
            """,
            (quiet_id, f"!{quiet_id:08x}", "Quiet Node", "QUIET", "CLIENT", now, now),
        )
        # Busy node high util samples
        for i, util in enumerate([35.0, 42.0, 38.5]):
            conn.execute(
                """
                INSERT INTO telemetry_data
                    (timestamp, node_id, channel_utilization, air_util_tx)
                VALUES (?, ?, ?, ?)
                """,
                (now - (i + 1) * 600, busy_id, util, 12.0 + i),
            )
        # Quiet node
        conn.execute(
            """
            INSERT INTO telemetry_data
                (timestamp, node_id, channel_utilization, air_util_tx)
            VALUES (?, ?, ?, ?)
            """,
            (now - 300, quiet_id, 8.0, 1.5),
        )
        # Packets from busy node
        for i in range(20):
            conn.execute(
                """
                INSERT INTO packet_history
                    (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name,
                     gateway_id, rssi, snr, hop_limit, payload_length, raw_payload,
                     processed_successfully)
                VALUES (?, 'msh/test', ?, ?, 1, 'TEXT_MESSAGE_APP', '!gw', -80, 4.0, 3, 10, ?, 1)
                """,
                (now - i * 60, busy_id, 0xFFFFFFFF, b"hi"),
            )
        for i in range(3):
            conn.execute(
                """
                INSERT INTO packet_history
                    (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name,
                     gateway_id, rssi, snr, hop_limit, payload_length, raw_payload,
                     processed_successfully)
                VALUES (?, 'msh/test', ?, ?, 67, 'TELEMETRY_APP', '!gw', -80, 4.0, 3, 10, ?, 1)
                """,
                (now - i * 120, quiet_id, 0xFFFFFFFF, b"t"),
            )
        conn.commit()
        conn.close()

    yield app, busy_id, quiet_id


@pytest.mark.unit
def test_utilization_summary_and_nodes(util_db):
    _app, busy_id, quiet_id = util_db
    summary = UtilizationRepository.get_summary(hours=24, high_util_pct=30.0)
    assert summary["reporting_nodes"] == 2
    assert summary["nodes_over_threshold"] == 1
    assert summary["avg_channel_utilization"] is not None
    assert summary["avg_channel_utilization"] > 20
    assert summary["max_channel_utilization"] == pytest.approx(42.0)
    assert summary["packet_count"] == 23
    assert summary["packets_per_hour"] > 0

    nodes = UtilizationRepository.get_nodes(hours=24, high_util_pct=30.0)
    assert len(nodes) == 2
    busy = next(n for n in nodes if n["node_id"] == busy_id)
    assert busy["over_threshold"] is True
    assert busy["max_channel_utilization"] == pytest.approx(42.0)
    assert busy["packet_count"] == 20
    quiet = next(n for n in nodes if n["node_id"] == quiet_id)
    assert quiet["over_threshold"] is False


@pytest.mark.unit
def test_utilization_timeline_and_talkers(util_db):
    _app, busy_id, _quiet_id = util_db
    timeline = UtilizationRepository.get_timeline(hours=24, bucket_mins=15)
    assert timeline["bucket_mins"] == 15
    assert len(timeline["buckets"]) >= 1
    assert any(b.get("packet_count", 0) > 0 for b in timeline["buckets"])

    talkers = UtilizationRepository.get_talkers(hours=24, limit=10)
    assert talkers["total_packets"] == 23
    assert talkers["top_nodes"][0]["node_id"] == busy_id
    assert talkers["top_nodes"][0]["packet_count"] == 20
    ports = {p["portnum_name"]: p["packet_count"] for p in talkers["by_portnum"]}
    assert ports["TEXT_MESSAGE_APP"] == 20
    assert ports["TELEMETRY_APP"] == 3


@pytest.mark.unit
def test_utilization_api_routes(client, util_db):
    _app, busy_id, _quiet_id = util_db
    page = client.get("/utilization")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "Channel Utilization" in body
    assert "/api/utilization/summary" in body

    summary = client.get("/api/utilization/summary?hours=24")
    assert summary.status_code == 200
    assert summary.get_json()["reporting_nodes"] == 2

    nodes = client.get("/api/utilization/nodes?hours=24")
    assert nodes.status_code == 200
    assert nodes.get_json()["total"] == 2

    timeline = client.get("/api/utilization/timeline?hours=24")
    assert timeline.status_code == 200
    assert "buckets" in timeline.get_json()

    talkers = client.get("/api/utilization/talkers?hours=24")
    assert talkers.status_code == 200
    top = talkers.get_json()["top_nodes"][0]
    assert top["node_id"] == busy_id
    assert top["packet_count"] == 20


@pytest.mark.unit
def test_utilization_nav_link(client):
    # Any authenticated page with base nav — utilization page itself is fine
    html = client.get("/utilization")
    if html.status_code != 200:
        return
    body = html.get_data(as_text=True)
    assert 'href="/utilization"' in body or "utilization.utilization_page" in body
    assert "Channel Utilization" in body
