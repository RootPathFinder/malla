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
            portnum INTEGER,
            portnum_name TEXT,
            rssi REAL,
            snr REAL,
            hop_start INTEGER,
            hop_limit INTEGER,
            processed_successfully INTEGER,
            mesh_packet_id INTEGER,
            raw_payload BLOB
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
    # Prefer long names when available
    assert "Newbie" in summary["new_node_names"]
    assert summary["active_nodes_24h"] == 3
    assert summary["text_messages_24h"] >= 1
    assert summary["packets_24h"] > summary["packets_prior_24h"]
    assert summary["packets_trend_pct"] != 0
    assert len(summary["hourly"]) == 24
    assert summary["timezone"] == "UTC"
    assert summary["direct_packets"] + summary["relayed_packets"] > 0
    assert summary["low_battery_nodes"] == 1
    assert len(summary["top_talkers"]) >= 1
    assert summary["top_talkers"][0]["name"] in {"Alpha", "Beta", "Newbie"}
    assert "farthest_node" in summary
    assert summary["farthest_node"] is None


@pytest.mark.unit
def test_last_24h_summary_farthest_node_uses_long_name(dashboard_db):
    from meshtastic.protobuf import mesh_pb2

    conn, _ = dashboard_db
    now = _seed_basic(conn)
    cursor = conn.cursor()

    # Gateway node (!gw1 is not a valid hex node). Use a real gateway hex id.
    gw_id = 0xAABBCC01
    far_id = 0xAABBCC99
    cursor.execute(
        "INSERT INTO node_info VALUES (?, 'GW1', 'Gateway One', ?, ?, 0)",
        (gw_id, now - 10 * 86400, now),
    )
    cursor.execute(
        "INSERT INTO node_info VALUES (?, 'FAR', 'Farthest Peak', ?, ?, 0)",
        (far_id, now - 10 * 86400, now),
    )

    def _pos(lat: float, lon: float) -> bytes:
        p = mesh_pb2.Position()
        p.latitude_i = int(lat * 1e7)
        p.longitude_i = int(lon * 1e7)
        return p.SerializeToString()

    # Gateway near SF; far node ~111km north
    cursor.execute(
        """
        INSERT INTO packet_history
        (timestamp, from_node_id, gateway_id, portnum, portnum_name, rssi, snr,
         hop_start, hop_limit, processed_successfully, mesh_packet_id, raw_payload)
        VALUES (?, ?, ?, 3, 'POSITION_APP', -70, 8.0, 3, 3, 1, 9001, ?)
        """,
        (now - 100, gw_id, f"!{gw_id:08x}", _pos(37.77, -122.42)),
    )
    cursor.execute(
        """
        INSERT INTO packet_history
        (timestamp, from_node_id, gateway_id, portnum, portnum_name, rssi, snr,
         hop_start, hop_limit, processed_successfully, mesh_packet_id, raw_payload)
        VALUES (?, ?, ?, 3, 'POSITION_APP', -90, 2.0, 3, 2, 1, 9002, ?)
        """,
        (now - 90, far_id, f"!{gw_id:08x}", _pos(38.77, -122.42)),
    )
    # Activity from far node heard by gateway
    cursor.execute(
        """
        INSERT INTO packet_history
        (timestamp, from_node_id, gateway_id, portnum, portnum_name, rssi, snr,
         hop_start, hop_limit, processed_successfully, mesh_packet_id)
        VALUES (?, ?, ?, 1, 'TEXT_MESSAGE_APP', -95, 1.0, 3, 1, 1, 9003)
        """,
        (now - 50, far_id, f"!{gw_id:08x}"),
    )
    # Also give the gateway some traffic so it is the busiest gateway
    for i in range(3):
        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, from_node_id, gateway_id, portnum, portnum_name, rssi, snr,
             hop_start, hop_limit, processed_successfully, mesh_packet_id)
            VALUES (?, ?, ?, 1, 'TELEMETRY_APP', -70, 8.0, 3, 3, 1, ?)
            """,
            (now - i * 10, gw_id, f"!{gw_id:08x}", 9100 + i),
        )
    conn.commit()

    summary = DashboardRepository.get_last_24h_summary()
    farthest = summary["farthest_node"]
    assert farthest is not None
    assert farthest["name"] == "Farthest Peak"
    assert farthest["distance_km"] > 100
    assert farthest["from_name"] == "Gateway One"
