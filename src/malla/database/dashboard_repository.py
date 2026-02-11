"""
Dashboard Configuration Repository - Database operations for persisting
user custom dashboard layouts.

Each authenticated user gets their own set of dashboards stored server-side,
so dashboards are available across devices and browsers.
"""

import json
import logging
import sqlite3
import time
from typing import Any

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema management
# ---------------------------------------------------------------------------


def _ensure_table(cursor: sqlite3.Cursor) -> None:
    """Create the custom_dashboards table if it does not exist yet.

    Safe to call repeatedly – the CREATE TABLE uses IF NOT EXISTS
    so this is idempotent. We always run the DDL rather than caching
    in a module-level flag, which makes the code more robust across
    test runs that swap out the underlying database file.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS custom_dashboards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            dashboards_json TEXT NOT NULL DEFAULT '[]',
            active_dashboard_id TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(user_id)
        )
    """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_custom_dashboards_user "
        "ON custom_dashboards(user_id)"
    )


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class DashboardConfigRepository:
    """Data-access layer for user custom dashboard configurations."""

    @staticmethod
    def get_config(user_id: int) -> dict[str, Any] | None:
        """Retrieve the stored dashboard configuration for a user.

        Returns a dict with 'dashboards' (list) and 'active_dashboard_id' (str),
        or None if no configuration is stored.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            cursor.execute(
                "SELECT dashboards_json, active_dashboard_id "
                "FROM custom_dashboards WHERE user_id = ?",
                (user_id,),
            )
            row = cursor.fetchone()
            conn.close()

            if not row:
                return None

            dashboards = json.loads(row["dashboards_json"])
            return {
                "dashboards": dashboards,
                "active_dashboard_id": row["active_dashboard_id"],
            }
        except Exception as e:
            logger.error(f"Error loading dashboard config for user {user_id}: {e}")
            return None

    @staticmethod
    def save_config(
        user_id: int,
        dashboards: list[dict[str, Any]],
        active_dashboard_id: str | None = None,
    ) -> bool:
        """Save (upsert) the dashboard configuration for a user.

        Args:
            user_id: The authenticated user's ID.
            dashboards: The full list of dashboard objects.
            active_dashboard_id: ID of the currently active dashboard.

        Returns:
            True on success, False on failure.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            now = time.time()
            dashboards_json = json.dumps(dashboards, separators=(",", ":"))

            cursor.execute(
                """
                INSERT INTO custom_dashboards
                    (user_id, dashboards_json, active_dashboard_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    dashboards_json = excluded.dashboards_json,
                    active_dashboard_id = excluded.active_dashboard_id,
                    updated_at = excluded.updated_at
                """,
                (user_id, dashboards_json, active_dashboard_id, now, now),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error saving dashboard config for user {user_id}: {e}")
            return False

    @staticmethod
    def delete_config(user_id: int) -> bool:
        """Delete the stored dashboard configuration for a user.

        Returns True on success, False on failure.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            cursor.execute(
                "DELETE FROM custom_dashboards WHERE user_id = ?", (user_id,)
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error deleting dashboard config for user {user_id}: {e}")
            return False
