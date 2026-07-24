"""Unit tests for expanded custom-dashboard telemetry history metrics."""

import time

import pytest
from meshtastic import telemetry_pb2

from malla.database.repositories import NodeRepository


@pytest.fixture
def history_db(monkeypatch, tmp_path):
    import sqlite3

    db_path = tmp_path / "history.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE packet_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            from_node_id INTEGER,
            portnum_name TEXT,
            raw_payload BLOB
        )
        """
    )
    conn.commit()

    def _get_conn():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr("malla.database.repositories.get_db_connection", _get_conn)
    yield conn
    conn.close()


@pytest.mark.unit
def test_telemetry_history_includes_extended_metrics(history_db):
    now = time.time()
    cursor = history_db.cursor()

    # Telemetry uses oneof variants — seed separate packets per metric family.
    env = telemetry_pb2.Telemetry()
    env.environment_metrics.temperature = 21.5
    env.environment_metrics.gas_resistance = 1200
    env.environment_metrics.voltage = 3.7
    env.environment_metrics.current = 12.5

    power = telemetry_pb2.Telemetry()
    power.power_metrics.ch1_voltage = 12.1
    power.power_metrics.ch1_current = 150

    air = telemetry_pb2.Telemetry()
    air.air_quality_metrics.pm25_standard = 8

    for offset, payload in (
        (60, env.SerializeToString()),
        (50, power.SerializeToString()),
        (40, air.SerializeToString()),
    ):
        cursor.execute(
            """
            INSERT INTO packet_history (timestamp, from_node_id, portnum_name, raw_payload)
            VALUES (?, 42, 'TELEMETRY_APP', ?)
            """,
            (now - offset, payload),
        )
    history_db.commit()

    history = NodeRepository.get_telemetry_history(42, hours=24)
    assert "temperature" in history
    assert "gas_resistance" in history
    assert "environment_voltage" in history
    assert "environment_current" in history
    assert "ch1_voltage" in history
    assert "ch1_current" in history
    assert "pm25_standard" in history
    assert history["pm25_standard"][0]["y"] == 8
