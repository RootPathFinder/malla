"""Unit tests for dashboard Network Activity temporal buckets."""

import sqlite3
import tempfile
import time

import pytest

from malla.services.analytics_service import AnalyticsService


@pytest.fixture
def analytics_db(monkeypatch):
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp.close()
    conn = sqlite3.connect(temp.name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE packet_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            from_node_id INTEGER,
            gateway_id TEXT,
            hop_start INTEGER,
            hop_limit INTEGER,
            processed_successfully INTEGER
        )
        """
    )
    conn.commit()

    def _get_conn():
        c = sqlite3.connect(temp.name)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr("malla.database.connection.get_db_connection", _get_conn)
    AnalyticsService._CACHE.clear()

    yield conn, temp.name
    conn.close()
    import os

    try:
        os.unlink(temp.name)
    except FileNotFoundError:
        pass


@pytest.mark.unit
def test_hourly_buckets_are_absolute_and_include_active_nodes(analytics_db):
    conn, _ = analytics_db
    now = time.time()
    current_hour = int(now // 3600) * 3600
    cur = conn.cursor()
    # Two nodes in current hour, one node in previous hour
    cur.execute(
        """
        INSERT INTO packet_history
        (timestamp, from_node_id, gateway_id, hop_start, hop_limit, processed_successfully)
        VALUES (?, 1, '!gw', 3, 3, 1), (?, 2, '!gw', 3, 3, 1), (?, 1, '!gw', 3, 3, 1)
        """,
        (current_hour + 10, current_hour + 20, current_hour - 3600 + 30),
    )
    conn.commit()

    result = AnalyticsService._get_temporal_patterns({}, now - 86400, days=1)
    hourly = result["hourly_breakdown"]
    assert hourly is not None
    assert len(hourly) == 24
    assert hourly[0]["bucket_ts"] < hourly[-1]["bucket_ts"]
    assert hourly[-1]["bucket_ts"] == current_hour

    by_ts = {h["bucket_ts"]: h for h in hourly}
    assert by_ts[current_hour]["total_packets"] == 2
    assert by_ts[current_hour]["active_nodes"] == 2
    assert by_ts[current_hour - 3600]["total_packets"] == 1
    assert by_ts[current_hour - 3600]["active_nodes"] == 1


@pytest.mark.unit
def test_daily_buckets_zero_filled_with_active_nodes(analytics_db):
    conn, _ = analytics_db
    now = time.time()
    current_day = int(now // 86400) * 86400
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO packet_history
        (timestamp, from_node_id, gateway_id, hop_start, hop_limit, processed_successfully)
        VALUES (?, 11, '!gw', 3, 3, 1), (?, 12, '!gw', 3, 3, 1)
        """,
        (current_day + 100, current_day - 86400 + 100),
    )
    conn.commit()

    result = AnalyticsService._get_temporal_patterns({}, now - 7 * 86400, days=7)
    daily = result["daily_breakdown"]
    assert daily is not None
    assert len(daily) == 7
    assert all("bucket_ts" in d and "active_nodes" in d for d in daily)
    assert daily[-1]["bucket_ts"] == current_day
    assert daily[-1]["active_nodes"] == 1
    assert daily[-2]["active_nodes"] == 1
    assert daily[0]["total_packets"] == 0
