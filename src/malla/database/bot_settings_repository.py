"""
Bot settings repository - persist mesh bot configuration across restarts.

Stores key/value settings in SQLite so Mesh Bot UI changes, command toggles,
and broadcast timestamps survive process restarts.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

from .connection import get_db_connection

logger = logging.getLogger(__name__)


def _ensure_table(cursor: sqlite3.Cursor) -> None:
    """Create the bot_settings table if needed."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )


class BotSettingsRepository:
    """Key/value persistence for BotService runtime settings."""

    @staticmethod
    def get_all() -> dict[str, Any]:
        """Return all settings with JSON-decoded values."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)
            cursor.execute("SELECT key, value FROM bot_settings")
            rows = cursor.fetchall()
            conn.close()

            result: dict[str, Any] = {}
            for row in rows:
                key = row["key"]
                raw = row["value"]
                try:
                    result[key] = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    result[key] = raw
            return result
        except Exception as e:
            logger.error("Failed to load bot settings: %s", e, exc_info=True)
            return {}

    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """Return one setting value, or default if missing."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)
            cursor.execute(
                "SELECT value FROM bot_settings WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            conn.close()
            if not row:
                return default
            try:
                return json.loads(row["value"])
            except (TypeError, json.JSONDecodeError):
                return row["value"]
        except Exception as e:
            logger.error("Failed to load bot setting %s: %s", key, e, exc_info=True)
            return default

    @staticmethod
    def set(key: str, value: Any) -> None:
        """Persist a single setting."""
        BotSettingsRepository.set_many({key: value})

    @staticmethod
    def set_many(settings: dict[str, Any]) -> None:
        """Persist multiple settings atomically."""
        if not settings:
            return
        try:
            now = time.time()
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)
            for key, value in settings.items():
                cursor.execute(
                    """
                    INSERT INTO bot_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (key, json.dumps(value), now),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error("Failed to save bot settings: %s", e, exc_info=True)
