"""
Repository for operator-managed scheduled solicited telemetry.

Routers on firmware ≥2.7.26 often broadcast telemetry at most every 12h.
Operators can schedule hop-aware solicitations (minimum 30 minutes) so health
monitoring stays useful without changing node roles.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

from ..utils.telemetry_request import TELEMETRY_TYPE_KEYS
from .connection import get_db_connection

logger = logging.getLogger(__name__)

MIN_INTERVAL_SECONDS = 1800  # 30 minutes
DEFAULT_INTERVAL_SECONDS = 3600  # 1 hour
DEFAULT_TELEMETRY_TYPES = ("device_metrics",)
MAX_INTERVAL_SECONDS = 7 * 24 * 3600  # 7 days


def _ensure_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduled_telemetry_requests (
            node_id INTEGER PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            interval_seconds INTEGER NOT NULL,
            telemetry_types TEXT NOT NULL,
            next_type_index INTEGER NOT NULL DEFAULT 0,
            last_requested_at REAL,
            last_success_at REAL,
            last_error TEXT,
            last_telemetry_type TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            CHECK(interval_seconds >= 1800)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scheduled_telemetry_due
        ON scheduled_telemetry_requests(enabled, last_requested_at)
        """
    )


def init_scheduled_telemetry_tables() -> None:
    """Create scheduled telemetry tables if needed."""
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.warning(f"Could not initialize scheduled telemetry tables: {e}")
        return
    try:
        cursor = conn.cursor()
        _ensure_table(cursor)
        conn.commit()
    finally:
        conn.close()
    logger.info("Scheduled telemetry tables initialized")


def normalize_interval_seconds(value: Any) -> int:
    """Clamp/validate interval; raises ValueError if invalid."""
    try:
        interval = int(value)
    except (TypeError, ValueError) as e:
        raise ValueError("interval_seconds must be an integer") from e
    if interval < MIN_INTERVAL_SECONDS:
        raise ValueError(
            f"interval_seconds must be at least {MIN_INTERVAL_SECONDS} "
            f"(30 minutes); got {interval}"
        )
    if interval > MAX_INTERVAL_SECONDS:
        raise ValueError(
            f"interval_seconds must be at most {MAX_INTERVAL_SECONDS}; got {interval}"
        )
    return interval


def normalize_telemetry_types(value: Any) -> list[str]:
    """Normalize a list of telemetry type keys; defaults to device_metrics."""
    if value is None:
        return list(DEFAULT_TELEMETRY_TYPES)
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [t.strip() for t in value.split(",") if t.strip()]
    if not isinstance(value, (list, tuple)):
        raise ValueError("telemetry_types must be a list of type keys")
    cleaned: list[str] = []
    for item in value:
        key = str(item).strip()
        if key in TELEMETRY_TYPE_KEYS and key not in cleaned:
            cleaned.append(key)
    if not cleaned:
        raise ValueError(
            "telemetry_types must include at least one valid type: "
            + ", ".join(TELEMETRY_TYPE_KEYS)
        )
    return cleaned


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    types_raw = row["telemetry_types"]
    try:
        types = json.loads(types_raw) if types_raw else list(DEFAULT_TELEMETRY_TYPES)
    except (TypeError, json.JSONDecodeError):
        types = list(DEFAULT_TELEMETRY_TYPES)
    return {
        "node_id": int(row["node_id"]),
        "hex_id": f"!{int(row['node_id']):08x}",
        "enabled": bool(row["enabled"]),
        "interval_seconds": int(row["interval_seconds"]),
        "interval_minutes": int(row["interval_seconds"]) // 60,
        "telemetry_types": types,
        "next_type_index": int(row["next_type_index"] or 0),
        "last_requested_at": row["last_requested_at"],
        "last_success_at": row["last_success_at"],
        "last_error": row["last_error"],
        "last_telemetry_type": row["last_telemetry_type"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


class ScheduledTelemetryRepository:
    """CRUD and due-claim helpers for scheduled solicited telemetry."""

    @staticmethod
    def _fetch_schedule_rows(
        cursor: sqlite3.Cursor, *, node_id: int | None = None
    ) -> list[sqlite3.Row]:
        """Fetch schedules, enriching with node_info when that table exists."""
        where = "WHERE s.node_id = ?" if node_id is not None else ""
        params: tuple[Any, ...] = (node_id,) if node_id is not None else ()
        try:
            cursor.execute(
                f"""
                SELECT s.*,
                       n.long_name,
                       n.short_name,
                       n.hex_id AS node_hex_id,
                       n.role
                FROM scheduled_telemetry_requests s
                LEFT JOIN node_info n ON n.node_id = s.node_id
                {where}
                ORDER BY s.enabled DESC, COALESCE(s.last_requested_at, 0) ASC
                """,
                params,
            )
            return list(cursor.fetchall())
        except sqlite3.OperationalError:
            cursor.execute(
                f"""
                SELECT s.*,
                       NULL AS long_name,
                       NULL AS short_name,
                       NULL AS node_hex_id,
                       NULL AS role
                FROM scheduled_telemetry_requests s
                {where}
                ORDER BY s.enabled DESC, COALESCE(s.last_requested_at, 0) ASC
                """,
                params,
            )
            return list(cursor.fetchall())

    @staticmethod
    def _enrich_schedule_row(row: sqlite3.Row) -> dict[str, Any]:
        item = _row_to_dict(row)
        item["long_name"] = row["long_name"]
        item["short_name"] = row["short_name"]
        item["role"] = row["role"]
        if row["node_hex_id"]:
            item["hex_id"] = row["node_hex_id"]
        item["display_name"] = (
            row["long_name"] or row["short_name"] or item["hex_id"]
        )
        return item

    @staticmethod
    def list_schedules() -> list[dict[str, Any]]:
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        rows = ScheduledTelemetryRepository._fetch_schedule_rows(cursor)
        conn.close()
        return [
            ScheduledTelemetryRepository._enrich_schedule_row(row) for row in rows
        ]

    @staticmethod
    def get_schedule(node_id: int) -> dict[str, Any] | None:
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        rows = ScheduledTelemetryRepository._fetch_schedule_rows(
            cursor, node_id=node_id
        )
        conn.close()
        if not rows:
            return None
        return ScheduledTelemetryRepository._enrich_schedule_row(rows[0])

    @staticmethod
    def upsert_schedule(
        node_id: int,
        *,
        interval_seconds: Any = DEFAULT_INTERVAL_SECONDS,
        telemetry_types: Any = None,
        enabled: bool = True,
    ) -> dict[str, Any]:
        interval = normalize_interval_seconds(interval_seconds)
        types = normalize_telemetry_types(telemetry_types)
        now = time.time()
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        cursor.execute(
            """
            INSERT INTO scheduled_telemetry_requests (
                node_id, enabled, interval_seconds, telemetry_types,
                next_type_index, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                enabled = excluded.enabled,
                interval_seconds = excluded.interval_seconds,
                telemetry_types = excluded.telemetry_types,
                updated_at = excluded.updated_at
            """,
            (
                node_id,
                1 if enabled else 0,
                interval,
                json.dumps(types),
                now,
                now,
            ),
        )
        conn.commit()
        conn.close()
        schedule = ScheduledTelemetryRepository.get_schedule(node_id)
        assert schedule is not None
        return schedule

    @staticmethod
    def delete_schedule(node_id: int) -> bool:
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        cursor.execute(
            "DELETE FROM scheduled_telemetry_requests WHERE node_id = ?",
            (node_id,),
        )
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    @staticmethod
    def set_enabled(node_id: int, enabled: bool) -> dict[str, Any] | None:
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        cursor.execute(
            """
            UPDATE scheduled_telemetry_requests
            SET enabled = ?, updated_at = ?
            WHERE node_id = ?
            """,
            (1 if enabled else 0, time.time(), node_id),
        )
        conn.commit()
        conn.close()
        return ScheduledTelemetryRepository.get_schedule(node_id)

    @staticmethod
    def claim_due_schedules(
        *,
        limit: int = 3,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """
        Atomically claim due schedules for this worker tick.

        Uses BEGIN IMMEDIATE so multiple app workers do not solicit the same
        node concurrently.
        """
        if limit < 1:
            return []
        now = time.time() if now is None else float(now)
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        claimed: list[dict[str, Any]] = []
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                """
                SELECT *
                FROM scheduled_telemetry_requests
                WHERE enabled = 1
                  AND (
                    last_requested_at IS NULL
                    OR last_requested_at + interval_seconds <= ?
                  )
                ORDER BY COALESCE(last_requested_at, 0) ASC
                LIMIT ?
                """,
                (now, limit),
            )
            rows = cursor.fetchall()
            for row in rows:
                node_id = int(row["node_id"])
                cursor.execute(
                    """
                    UPDATE scheduled_telemetry_requests
                    SET last_requested_at = ?, updated_at = ?
                    WHERE node_id = ?
                      AND enabled = 1
                      AND (
                        last_requested_at IS NULL
                        OR last_requested_at + interval_seconds <= ?
                      )
                    """,
                    (now, now, node_id, now),
                )
                if cursor.rowcount:
                    item = _row_to_dict(row)
                    item["last_requested_at"] = now
                    claimed.append(item)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return claimed

    @staticmethod
    def record_result(
        node_id: int,
        *,
        success: bool,
        telemetry_type: str,
        error: str | None = None,
        advance_type_index: bool = True,
    ) -> None:
        now = time.time()
        conn = get_db_connection()
        cursor = conn.cursor()
        _ensure_table(cursor)
        if advance_type_index:
            cursor.execute(
                """
                UPDATE scheduled_telemetry_requests
                SET last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                    last_error = ?,
                    last_telemetry_type = ?,
                    next_type_index = next_type_index + 1,
                    updated_at = ?
                WHERE node_id = ?
                """,
                (
                    1 if success else 0,
                    now,
                    None if success else (error or "request failed"),
                    telemetry_type,
                    now,
                    node_id,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE scheduled_telemetry_requests
                SET last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                    last_error = ?,
                    last_telemetry_type = ?,
                    updated_at = ?
                WHERE node_id = ?
                """,
                (
                    1 if success else 0,
                    now,
                    None if success else (error or "request failed"),
                    telemetry_type,
                    now,
                    node_id,
                ),
            )
        conn.commit()
        conn.close()

    @staticmethod
    def pick_next_type(schedule: dict[str, Any]) -> str:
        types = schedule.get("telemetry_types") or list(DEFAULT_TELEMETRY_TYPES)
        if not types:
            return "device_metrics"
        idx = int(schedule.get("next_type_index") or 0) % len(types)
        return str(types[idx])
