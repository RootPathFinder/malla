"""Unit tests for dashboard stats and last-24h summary accuracy."""

import sqlite3
import tempfile
import time

import pytest

from malla.database.repositories import DashboardRepository


@pytest.fixture
def dashboard_db(monkeypatch):
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp.close()
    conn = sqlite3.connect(temp.name)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE node_info (
            node_id INTEGER PRIMARY KEY,
            short_name TEXT,
            long_name TEXT,
            first_seen REAL,
            last_updated REAL,
            archived INTEGER DEFAULT 0
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE packet_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            from_node_id INTEGER,
            gateway_id TEXT,
            portnum_name TEXT,
            rssi REAL,
            snr REAL,
            hop_start INTEGER,
            hop_limit INTEGER,
            processed_successfully INTEGER,
            mesh_packet_id INTEGER
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE telemetry_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            node_id INTEGER,
            battery_level INTEGER,
            voltage REAL
        )
        """
    )
    conn.commit()

    def _get_conn():
        c = sqlite3.connect(temp.name)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr("malla.database.repositories.get_db_connection", _get_conn)
    monkeypatch.setattr(
        "malla.power_analysis.recent_telemetry_cutoff",
        lambda now=None: (now or time.time()) - 48 * 3600,
    )

    yield conn, temp.name
    conn.close()
    import os

    try:
        os.unlink(temp.name)
    except FileNotFoundError:
        pass


def _seed_basic(conn):
    now = time.time()
    cursor = conn.cursor()
    # Known nodes: 2 active, 1 archived, 1 new
    cursor.execute(
        "INSERT INTO node_info VALUES (1, 'AAA', 'Alpha', ?, ?, 0)",
        (now - 10 * 86400, now),
    )
    cursor.execute(
        "INSERT INTO node_info VALUES (2, 'BBB', 'Beta', ?, ?, 0)",
        (now - 5 * 86400, now),
    )
    cursor.execute(
        "INSERT INTO node_info VALUES (3, 'OLD', 'Archived', ?, ?, 1)",
        (now - 30 * 86400, now - 2 * 86400),
    )
    cursor.execute(
        "INSERT INTO node_info VALUES (4, 'NEW', 'Newbie', ?, ?, 0)",
        (now - 3600, now),
    )

    # Packets in last 24h from nodes 1,2,4 and archived 3
    for nid, n in ((1, 5), (2, 3), (3, 2), (4, 1)):
        for i in range(n):
            cursor.execute(
                """
                INSERT INTO packet_history
                (timestamp, from_node_id, gateway_id, portnum_name, rssi, snr,
                 hop_start, hop_limit, processed_successfully, mesh_packet_id)
                VALUES (?, ?, '!gw1', ?, -75, 8.0, 3, ?, 1, ?)
                """,
                (
                    now - i * 600,
                    nid,
                    "TEXT_MESSAGE_APP" if i == 0 else "TELEMETRY_APP",
                    3 if i % 2 == 0 else 2,  # hop_count 0 or 1
                    1000 + nid * 10 + i,
                ),
            )

    # Prior-day packets for trend
    for i in range(4):
        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, from_node_id, gateway_id, portnum_name, rssi, snr,
             hop_start, hop_limit, processed_successfully, mesh_packet_id)
            VALUES (?, 1, '!gw1', 'TELEMETRY_APP', -80, 5.0, 3, 3, 1, ?)
            """,
            (now - 30 * 3600 - i * 600, 2000 + i),
        )

    cursor.execute(
        "INSERT INTO telemetry_data VALUES (NULL, ?, 1, 25, 3.6)",
        (now - 100,),
    )
    conn.commit()
    return now


@pytest.mark.unit
def test_get_stats_excludes_archived_from_active(dashboard_db):
    conn, _ = dashboard_db
    _seed_basic(conn)

    stats = DashboardRepository.get_stats()
    # Nodes 1,2,4 active; archived 3 excluded. Total non-archived = 3
    assert stats["total_nodes"] == 3
    assert stats["active_nodes_24h"] == 3
    assert stats["packets_24h"] > 0
    assert "success_rate" in stats


@pytest.mark.unit
def test_last_24h_summary_new_nodes_and_trends(dashboard_db):
    conn, _ = dashboard_db
    _seed_basic(conn)

    summary = DashboardRepository.get_last_24h_summary()
    assert summary["new_nodes_24h"] == 1
    assert "NEW" in summary["new_node_names"] or "Newbie" in summary["new_node_names"]
    assert summary["active_nodes_24h"] == 3
    assert summary["text_messages_24h"] >= 1
    assert summary["packets_24h"] > summary["packets_prior_24h"]
    assert summary["packets_trend_pct"] != 0
    assert len(summary["hourly"]) == 24
    assert summary["timezone"] == "UTC"
    assert summary["direct_packets"] + summary["relayed_packets"] > 0
    assert summary["low_battery_nodes"] == 1
    assert len(summary["top_talkers"]) >= 1
