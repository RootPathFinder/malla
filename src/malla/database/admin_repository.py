"""
Repository for admin-related database operations.

Handles audit logging of admin commands and tracking of administrable nodes.
"""

import logging
import time
from typing import Any

from .connection import get_db_connection

logger = logging.getLogger(__name__)


def init_admin_tables() -> None:
    """Initialize admin-related database tables."""
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.warning(f"Could not initialize admin tables: {e}")
        return

    cursor = conn.cursor()

    # Table for admin command audit log
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            target_node_id INTEGER NOT NULL,
            command_type TEXT NOT NULL,
            command_data TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            response_data TEXT,
            response_timestamp REAL,
            error_message TEXT
        )
    """)

    # Table to track nodes that are administrable (have responded to admin requests)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS administrable_nodes (
            node_id INTEGER PRIMARY KEY,
            first_confirmed REAL NOT NULL,
            last_confirmed REAL NOT NULL,
            firmware_version TEXT,
            device_metadata TEXT,
            admin_channel_index INTEGER DEFAULT 0,
            last_status_check REAL,
            last_status_result TEXT DEFAULT 'unknown'
        )
    """)

    # Migration: Add new columns if they don't exist (for existing databases)
    try:
        cursor.execute(
            "ALTER TABLE administrable_nodes ADD COLUMN last_status_check REAL"
        )
    except Exception:
        pass  # Column already exists

    try:
        cursor.execute(
            "ALTER TABLE administrable_nodes ADD COLUMN last_status_result TEXT DEFAULT 'unknown'"
        )
    except Exception:
        pass  # Column already exists

    # Index for efficient queries
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_log_node ON admin_log(target_node_id, timestamp DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_log_status ON admin_log(status, timestamp DESC)"
    )

    conn.commit()
    conn.close()
    logger.info("Admin tables initialized")


class AdminRepository:
    """Repository for admin-related database operations."""

    @staticmethod
    def log_admin_command(
        target_node_id: int,
        command_type: str,
        command_data: str | None = None,
    ) -> int:
        """
        Log an admin command being sent.

        Args:
            target_node_id: The node ID the command is being sent to
            command_type: Type of admin command (e.g., 'get_config', 'reboot')
            command_data: Optional JSON string of command parameters

        Returns:
            The ID of the created log entry
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO admin_log (timestamp, target_node_id, command_type, command_data, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (time.time(), target_node_id, command_type, command_data),
        )

        log_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(
            f"Logged admin command {command_type} to node {target_node_id}, log_id={log_id}"
        )
        # lastrowid can be None if no row was inserted, but we just inserted so it should be set
        assert log_id is not None, "Failed to get log_id after insert"
        return log_id

    @staticmethod
    def update_admin_log_status(
        log_id: int,
        status: str,
        response_data: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Update the status of an admin command log entry.

        Args:
            log_id: The log entry ID
            status: New status ('success', 'failed', 'timeout')
            response_data: Optional JSON string of response data
            error_message: Optional error message if failed
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE admin_log
            SET status = ?, response_data = ?, response_timestamp = ?, error_message = ?
            WHERE id = ?
            """,
            (status, response_data, time.time(), error_message, log_id),
        )

        conn.commit()
        conn.close()

        logger.info(f"Updated admin log {log_id} status to {status}")

    @staticmethod
    def get_admin_log(
        target_node_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Get admin command log entries.

        Args:
            target_node_id: Optional filter by target node
            limit: Maximum number of entries to return
            offset: Number of entries to skip

        Returns:
            List of log entries
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        if target_node_id:
            cursor.execute(
                """
                SELECT * FROM admin_log
                WHERE target_node_id = ?
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
                """,
                (target_node_id, limit, offset),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM admin_log
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    @staticmethod
    def get_pending_commands(max_age_seconds: float = 60.0) -> list[dict[str, Any]]:
        """
        Get pending admin commands that are waiting for responses.

        Args:
            max_age_seconds: Maximum age of pending commands to return

        Returns:
            List of pending command entries
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        min_timestamp = time.time() - max_age_seconds

        cursor.execute(
            """
            SELECT * FROM admin_log
            WHERE status = 'pending' AND timestamp > ?
            ORDER BY timestamp ASC
            """,
            (min_timestamp,),
        )

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    @staticmethod
    def mark_node_administrable(
        node_id: int,
        firmware_version: str | None = None,
        device_metadata: str | None = None,
        admin_channel_index: int = 0,
    ) -> None:
        """
        Mark a node as administrable (has responded to admin requests).

        Args:
            node_id: The node ID
            firmware_version: Optional firmware version string
            device_metadata: Optional JSON string of device metadata
            admin_channel_index: The channel index used for admin
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()

        cursor.execute(
            """
            INSERT INTO administrable_nodes (node_id, first_confirmed, last_confirmed,
                                             firmware_version, device_metadata, admin_channel_index,
                                             last_status_check, last_status_result)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'online')
            ON CONFLICT(node_id) DO UPDATE SET
                last_confirmed = excluded.last_confirmed,
                firmware_version = COALESCE(excluded.firmware_version, firmware_version),
                device_metadata = COALESCE(excluded.device_metadata, device_metadata),
                admin_channel_index = excluded.admin_channel_index,
                last_status_check = excluded.last_status_check,
                last_status_result = 'online'
            """,
            (
                node_id,
                now,
                now,
                firmware_version,
                device_metadata,
                admin_channel_index,
                now,
            ),
        )

        conn.commit()
        conn.close()

        logger.info(f"Marked node {node_id} as administrable")

    @staticmethod
    def update_node_status(node_id: int, status: str) -> None:
        """
        Update the last status check for an administrable node.

        Args:
            node_id: The node ID
            status: Status result ('online', 'offline', 'timeout', 'error')
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()

        cursor.execute(
            """
            UPDATE administrable_nodes
            SET last_status_check = ?, last_status_result = ?
            WHERE node_id = ?
            """,
            (now, status, node_id),
        )

        conn.commit()
        conn.close()

        logger.debug(f"Updated status for node {node_id}: {status}")

    @staticmethod
    def is_node_administrable(node_id: int) -> bool:
        """
        Check if a node is marked as administrable.

        Args:
            node_id: The node ID to check

        Returns:
            True if the node is administrable
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1 FROM administrable_nodes WHERE node_id = ?",
            (node_id,),
        )

        result = cursor.fetchone()
        conn.close()

        return result is not None

    @staticmethod
    def get_administrable_nodes() -> list[dict[str, Any]]:
        """
        Get all administrable nodes with their details.

        Returns:
            List of administrable node records with node_info joined
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                an.*,
                ni.hex_id,
                ni.long_name,
                ni.short_name,
                ni.hw_model,
                ni.last_updated as last_seen
            FROM administrable_nodes an
            LEFT JOIN node_info ni ON an.node_id = ni.node_id
            ORDER BY an.last_confirmed DESC
            """
        )

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    @staticmethod
    def get_administrable_node_details(node_id: int) -> dict[str, Any] | None:
        """
        Get details for a specific administrable node.

        Args:
            node_id: The node ID

        Returns:
            Node details or None if not administrable
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                an.*,
                ni.hex_id,
                ni.long_name,
                ni.short_name,
                ni.hw_model,
                ni.last_updated as last_seen
            FROM administrable_nodes an
            LEFT JOIN node_info ni ON an.node_id = ni.node_id
            WHERE an.node_id = ?
            """,
            (node_id,),
        )

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def remove_administrable_node(node_id: int) -> bool:
        """
        Remove a node from the administrable list.

        Args:
            node_id: The node ID to remove

        Returns:
            True if a node was removed
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM administrable_nodes WHERE node_id = ?",
            (node_id,),
        )

        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()

        if deleted:
            logger.info(f"Removed node {node_id} from administrable list")

        return deleted
