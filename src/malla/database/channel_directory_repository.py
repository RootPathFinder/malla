"""
Channel Directory Repository - Database operations for the community channel directory.

The channel directory allows mesh users to register channels (name + PSK) that the
bot advertises periodically.  Other users can then add those channels to their
radios to participate in topic-specific communication.
"""

import logging
import sqlite3
import time
from typing import Any

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema management
# ---------------------------------------------------------------------------

_TABLE_CREATED = False


def _ensure_table(cursor: sqlite3.Cursor) -> None:
    """Create the channel_directory table if it does not exist yet.

    Safe to call repeatedly – the CREATE TABLE uses IF NOT EXISTS and a
    module-level flag avoids hitting the database more than once per process.
    """
    global _TABLE_CREATED  # noqa: PLW0603
    if _TABLE_CREATED:
        return

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_directory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT NOT NULL,
            psk TEXT NOT NULL DEFAULT 'AQ==',
            description TEXT,
            registered_by_node_id INTEGER,
            registered_by_name TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            UNIQUE(channel_name COLLATE NOCASE)
        )
    """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_channel_dir_active "
        "ON channel_directory(active)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_channel_dir_name "
        "ON channel_directory(channel_name COLLATE NOCASE)"
    )

    _TABLE_CREATED = True


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class ChannelDirectoryRepository:
    """Data-access layer for the community channel directory."""

    # -- Write operations ----------------------------------------------------

    @staticmethod
    def add_channel(
        channel_name: str,
        psk: str = "AQ==",
        description: str | None = None,
        registered_by_node_id: int | None = None,
        registered_by_name: str | None = None,
    ) -> dict[str, Any]:
        """Register a new channel in the directory.

        Returns:
            Dict with ``success`` (bool) and either ``channel`` or ``error``.
        """
        now = time.time()
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            cursor.execute(
                """
                INSERT INTO channel_directory
                    (channel_name, psk, description,
                     registered_by_node_id, registered_by_name,
                     created_at, updated_at, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    channel_name.strip(),
                    psk.strip(),
                    description.strip() if description else None,
                    registered_by_node_id,
                    registered_by_name,
                    now,
                    now,
                ),
            )
            conn.commit()
            row_id = cursor.lastrowid
            conn.close()

            return {
                "success": True,
                "channel": {
                    "id": row_id,
                    "channel_name": channel_name.strip(),
                    "psk": psk.strip(),
                    "description": description,
                    "registered_by_node_id": registered_by_node_id,
                    "registered_by_name": registered_by_name,
                    "created_at": now,
                    "updated_at": now,
                    "active": True,
                },
            }
        except sqlite3.IntegrityError:
            return {
                "success": False,
                "error": f"Channel '{channel_name}' already exists",
            }
        except Exception as e:
            logger.error(f"Error adding channel to directory: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def update_channel(
        channel_name: str,
        psk: str | None = None,
        description: str | None = None,
        active: bool | None = None,
    ) -> dict[str, Any]:
        """Update an existing channel entry.

        Only the fields that are not ``None`` will be modified.

        Returns:
            Dict with ``success`` (bool) and optional ``error``.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            sets: list[str] = ["updated_at = ?"]
            params: list[Any] = [time.time()]

            if psk is not None:
                sets.append("psk = ?")
                params.append(psk.strip())
            if description is not None:
                sets.append("description = ?")
                params.append(description.strip())
            if active is not None:
                sets.append("active = ?")
                params.append(1 if active else 0)

            params.append(channel_name.strip())

            cursor.execute(
                f"UPDATE channel_directory SET {', '.join(sets)} "
                "WHERE channel_name = ? COLLATE NOCASE",
                params,
            )
            conn.commit()
            changed = cursor.rowcount
            conn.close()

            if changed == 0:
                return {
                    "success": False,
                    "error": f"Channel '{channel_name}' not found",
                }
            return {"success": True}
        except Exception as e:
            logger.error(f"Error updating channel directory: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def remove_channel(
        channel_name: str,
        requester_node_id: int | None = None,
    ) -> dict[str, Any]:
        """Remove a channel from the directory.

        If *requester_node_id* is given, only the original registrant (or the
        web UI / admin with ``None``) is allowed to delete.

        Returns:
            Dict with ``success`` (bool) and optional ``error``.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            if requester_node_id is not None:
                # Only allow the registrant to remove
                cursor.execute(
                    "DELETE FROM channel_directory "
                    "WHERE channel_name = ? COLLATE NOCASE "
                    "AND registered_by_node_id = ?",
                    (channel_name.strip(), requester_node_id),
                )
            else:
                # Admin / web UI – unrestricted
                cursor.execute(
                    "DELETE FROM channel_directory "
                    "WHERE channel_name = ? COLLATE NOCASE",
                    (channel_name.strip(),),
                )

            conn.commit()
            deleted = cursor.rowcount
            conn.close()

            if deleted == 0:
                return {
                    "success": False,
                    "error": f"Channel '{channel_name}' not found or you didn't register it",
                }
            return {"success": True}
        except Exception as e:
            logger.error(f"Error removing channel from directory: {e}")
            return {"success": False, "error": str(e)}

    # -- Read operations -----------------------------------------------------

    @staticmethod
    def get_all_channels(active_only: bool = True) -> list[dict[str, Any]]:
        """Return all channels in the directory.

        Args:
            active_only: When ``True`` (default), exclude deactivated entries.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            if active_only:
                cursor.execute(
                    "SELECT * FROM channel_directory WHERE active = 1 "
                    "ORDER BY channel_name COLLATE NOCASE"
                )
            else:
                cursor.execute(
                    "SELECT * FROM channel_directory "
                    "ORDER BY channel_name COLLATE NOCASE"
                )

            rows = cursor.fetchall()
            conn.close()

            return [
                {
                    "id": row["id"],
                    "channel_name": row["channel_name"],
                    "psk": row["psk"],
                    "description": row["description"],
                    "registered_by_node_id": row["registered_by_node_id"],
                    "registered_by_name": row["registered_by_name"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "active": bool(row["active"]),
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error listing channel directory: {e}")
            return []

    @staticmethod
    def get_channel(channel_name: str) -> dict[str, Any] | None:
        """Fetch a single channel by name (case-insensitive)."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            cursor.execute(
                "SELECT * FROM channel_directory "
                "WHERE channel_name = ? COLLATE NOCASE",
                (channel_name.strip(),),
            )
            row = cursor.fetchone()
            conn.close()

            if not row:
                return None

            return {
                "id": row["id"],
                "channel_name": row["channel_name"],
                "psk": row["psk"],
                "description": row["description"],
                "registered_by_node_id": row["registered_by_node_id"],
                "registered_by_name": row["registered_by_name"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "active": bool(row["active"]),
            }
        except Exception as e:
            logger.error(f"Error fetching channel: {e}")
            return None

    @staticmethod
    def get_channel_count(active_only: bool = True) -> int:
        """Return the number of registered channels."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)

            if active_only:
                cursor.execute(
                    "SELECT COUNT(*) as cnt FROM channel_directory WHERE active = 1"
                )
            else:
                cursor.execute("SELECT COUNT(*) as cnt FROM channel_directory")

            count = cursor.fetchone()["cnt"]
            conn.close()
            return count
        except Exception as e:
            logger.error(f"Error counting channels: {e}")
            return 0
