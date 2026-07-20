"""Tests for operator-scheduled solicited telemetry."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from malla.database.connection import get_db_connection
from malla.database.scheduled_telemetry_repository import (
    MIN_INTERVAL_SECONDS,
    ScheduledTelemetryRepository,
    init_scheduled_telemetry_tables,
    normalize_interval_seconds,
    normalize_telemetry_types,
)
from malla.services.scheduled_telemetry_service import (
    run_due_schedules_once,
    run_schedule_now,
)


@pytest.fixture
def scheduled_db(tmp_path, monkeypatch):
    """Isolate scheduled telemetry tables on a temp SQLite DB."""
    db_path = tmp_path / "scheduled_telemetry.db"
    cfg = MagicMock()
    cfg.database_file = str(db_path)
    monkeypatch.setenv("MALLA_DATABASE_FILE", str(db_path))
    monkeypatch.setattr("malla.database.connection.get_config", lambda: cfg)
    monkeypatch.setattr("src.malla.database.connection.get_config", lambda: cfg)

    init_scheduled_telemetry_tables()
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS node_info (
            node_id INTEGER PRIMARY KEY,
            hex_id TEXT,
            long_name TEXT,
            short_name TEXT,
            role TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO node_info (node_id, hex_id, long_name, short_name, role)
        VALUES (?, ?, ?, ?, ?)
        """,
        (0x0A11C001, "!0a11c001", "Solar Ridge", "SOLR", "ROUTER"),
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.mark.unit
class TestScheduledTelemetryNormalization:
    def test_min_interval_is_30_minutes(self):
        assert MIN_INTERVAL_SECONDS == 1800
        assert normalize_interval_seconds(1800) == 1800
        assert normalize_interval_seconds(3600) == 3600
        with pytest.raises(ValueError, match="at least"):
            normalize_interval_seconds(1799)

    def test_telemetry_types_default_and_filter(self):
        assert normalize_telemetry_types(None) == ["device_metrics"]
        assert normalize_telemetry_types(
            ["device_metrics", "bogus", "environment_metrics", "device_metrics"]
        ) == ["device_metrics", "environment_metrics"]
        with pytest.raises(ValueError, match="at least one"):
            normalize_telemetry_types(["nope"])


@pytest.mark.unit
class TestScheduledTelemetryRepository:
    def test_upsert_list_claim_and_result(self, scheduled_db):
        schedule = ScheduledTelemetryRepository.upsert_schedule(
            0x0A11C001,
            interval_seconds=1800,
            telemetry_types=["device_metrics", "power_metrics"],
            enabled=True,
        )
        assert schedule["node_id"] == 0x0A11C001
        assert schedule["interval_seconds"] == 1800
        assert schedule["display_name"] == "Solar Ridge"
        assert schedule["telemetry_types"] == ["device_metrics", "power_metrics"]

        listed = ScheduledTelemetryRepository.list_schedules()
        assert len(listed) == 1

        now = time.time()
        claimed = ScheduledTelemetryRepository.claim_due_schedules(limit=5, now=now)
        assert len(claimed) == 1
        assert claimed[0]["node_id"] == 0x0A11C001

        # Not due again until interval elapses
        claimed_again = ScheduledTelemetryRepository.claim_due_schedules(
            limit=5, now=now + 60
        )
        assert claimed_again == []

        # Due after interval
        claimed_later = ScheduledTelemetryRepository.claim_due_schedules(
            limit=5, now=now + 1800
        )
        assert len(claimed_later) == 1

        assert (
            ScheduledTelemetryRepository.pick_next_type(schedule) == "device_metrics"
        )
        ScheduledTelemetryRepository.record_result(
            0x0A11C001,
            success=True,
            telemetry_type="device_metrics",
        )
        updated = ScheduledTelemetryRepository.get_schedule(0x0A11C001)
        assert updated is not None
        assert updated["last_success_at"] is not None
        assert updated["last_error"] is None
        assert updated["next_type_index"] == 1
        assert (
            ScheduledTelemetryRepository.pick_next_type(updated) == "power_metrics"
        )

    def test_rejects_too_short_interval(self, scheduled_db):
        with pytest.raises(ValueError, match="at least"):
            ScheduledTelemetryRepository.upsert_schedule(
                0x1, interval_seconds=60, enabled=True
            )

    def test_delete_schedule(self, scheduled_db):
        ScheduledTelemetryRepository.upsert_schedule(
            0x0A11C001, interval_seconds=3600, enabled=True
        )
        assert ScheduledTelemetryRepository.delete_schedule(0x0A11C001) is True
        assert ScheduledTelemetryRepository.get_schedule(0x0A11C001) is None


@pytest.mark.unit
class TestScheduledTelemetryRunner:
    def test_run_due_skips_without_publisher(self, scheduled_db):
        ScheduledTelemetryRepository.upsert_schedule(
            0x0A11C001, interval_seconds=1800, enabled=True
        )
        with patch(
            "malla.services.scheduled_telemetry_service.get_connected_mesh_publisher",
            return_value=(None, "tcp"),
        ):
            summary = run_due_schedules_once()
        assert summary["claimed"] == 0
        assert summary["skipped_reason"] == "publisher_disconnected"

    def test_run_due_solicits_claimed_nodes(self, scheduled_db):
        ScheduledTelemetryRepository.upsert_schedule(
            0x0A11C001,
            interval_seconds=1800,
            telemetry_types=["device_metrics"],
            enabled=True,
        )
        fake_publisher = object()
        with (
            patch(
                "malla.services.scheduled_telemetry_service.get_connected_mesh_publisher",
                return_value=(fake_publisher, "tcp"),
            ),
            patch(
                "malla.services.scheduled_telemetry_service.solicit_node_telemetry",
                return_value={
                    "success": True,
                    "telemetry": {"device_metrics": {"battery_level": 88}},
                    "source": "live",
                    "estimated_hops": 1,
                },
            ) as solicit,
        ):
            summary = run_due_schedules_once(limit=3)

        assert summary["claimed"] == 1
        assert summary["succeeded"] == 1
        assert summary["failed"] == 0
        solicit.assert_called_once()
        args = solicit.call_args[0]
        assert args[0] == 0x0A11C001
        assert args[1] == "device_metrics"

        schedule = ScheduledTelemetryRepository.get_schedule(0x0A11C001)
        assert schedule is not None
        assert schedule["last_success_at"] is not None

    def test_run_schedule_now_requires_existing_schedule(self, scheduled_db):
        outcome = run_schedule_now(0xDEADBEEF)
        assert outcome["success"] is False
        assert "No schedule" in outcome["error"]
