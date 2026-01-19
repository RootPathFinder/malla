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

    # Table for configuration templates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            template_type TEXT NOT NULL,
            config_data TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
    """)

    # Table for template deployment history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS template_deployments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            node_id INTEGER NOT NULL,
            deployed_at REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            result_message TEXT,
            FOREIGN KEY (template_id) REFERENCES config_templates(id)
        )
    """)

    # Index for deployment queries
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_deployments_template ON template_deployments(template_id, deployed_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_deployments_node ON template_deployments(node_id, deployed_at DESC)"
    )

    # Table for node configuration backups
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS node_backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER NOT NULL,
            backup_name TEXT NOT NULL,
            description TEXT,
            backup_data TEXT NOT NULL,
            created_at REAL NOT NULL,
            node_long_name TEXT,
            node_short_name TEXT,
            node_hex_id TEXT,
            hardware_model TEXT,
            firmware_version TEXT
        )
    """)

    # Index for backup queries
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_backups_node ON node_backups(node_id, created_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_backups_name ON node_backups(backup_name)"
    )

    # Table for compliance check results
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS compliance_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            node_id INTEGER NOT NULL,
            checked_at REAL NOT NULL,
            is_compliant INTEGER NOT NULL DEFAULT 0,
            diff_data TEXT,
            error_message TEXT,
            FOREIGN KEY (template_id) REFERENCES config_templates(id)
        )
    """)

    # Index for compliance queries
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_template ON compliance_checks(template_id, checked_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_node ON compliance_checks(node_id, checked_at DESC)"
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

    # =========================================================================
    # Configuration Template Methods
    # =========================================================================

    @staticmethod
    def create_template(
        name: str,
        template_type: str,
        config_data: str,
        description: str | None = None,
    ) -> int:
        """
        Create a new configuration template.

        Args:
            name: Unique template name
            template_type: Type of config (device, lora, channel, position, etc.)
            config_data: JSON string of configuration data
            description: Optional description

        Returns:
            The ID of the created template
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()

        cursor.execute(
            """
            INSERT INTO config_templates (name, description, template_type, config_data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, description, template_type, config_data, now, now),
        )

        template_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Created config template '{name}' (id={template_id})")
        return template_id  # type: ignore[return-value]

    @staticmethod
    def update_template(
        template_id: int,
        name: str | None = None,
        description: str | None = None,
        config_data: str | None = None,
    ) -> bool:
        """
        Update an existing configuration template.

        Args:
            template_id: The template ID to update
            name: New name (optional)
            description: New description (optional)
            config_data: New config data JSON (optional)

        Returns:
            True if template was updated
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()

        # Build dynamic update query
        updates = ["updated_at = ?"]
        params: list[Any] = [now]

        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if config_data is not None:
            updates.append("config_data = ?")
            params.append(config_data)

        params.append(template_id)

        cursor.execute(
            f"UPDATE config_templates SET {', '.join(updates)} WHERE id = ?",
            params,
        )

        updated = cursor.rowcount > 0
        conn.commit()
        conn.close()

        if updated:
            logger.info(f"Updated config template id={template_id}")

        return updated

    @staticmethod
    def delete_template(template_id: int) -> bool:
        """
        Delete a configuration template and its deployment history.

        Args:
            template_id: The template ID to delete

        Returns:
            True if template was deleted
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # First delete related deployment records to avoid foreign key constraint
        cursor.execute(
            "DELETE FROM template_deployments WHERE template_id = ?", (template_id,)
        )
        deployments_deleted = cursor.rowcount

        # Then delete the template
        cursor.execute("DELETE FROM config_templates WHERE id = ?", (template_id,))

        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()

        if deleted:
            logger.info(
                f"Deleted config template id={template_id} "
                f"(and {deployments_deleted} deployment records)"
            )

        return deleted

    @staticmethod
    def get_template(template_id: int) -> dict[str, Any] | None:
        """
        Get a configuration template by ID.

        Args:
            template_id: The template ID

        Returns:
            Template dict or None if not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM config_templates WHERE id = ?", (template_id,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def get_template_by_name(name: str) -> dict[str, Any] | None:
        """
        Get a configuration template by name.

        Args:
            name: The template name

        Returns:
            Template dict or None if not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM config_templates WHERE name = ?", (name,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def get_all_templates(template_type: str | None = None) -> list[dict[str, Any]]:
        """
        Get all configuration templates, optionally filtered by type.

        Args:
            template_type: Filter by type (optional)

        Returns:
            List of template dicts
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        if template_type:
            cursor.execute(
                "SELECT * FROM config_templates WHERE template_type = ? ORDER BY name",
                (template_type,),
            )
        else:
            cursor.execute("SELECT * FROM config_templates ORDER BY name")

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    # =========================================================================
    # Template Deployment Methods
    # =========================================================================

    @staticmethod
    def log_deployment(
        template_id: int,
        node_id: int,
        status: str = "pending",
        result_message: str | None = None,
    ) -> int:
        """
        Log a template deployment attempt.

        Args:
            template_id: The template being deployed
            node_id: Target node ID
            status: Deployment status
            result_message: Optional result message

        Returns:
            The deployment log ID
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()

        cursor.execute(
            """
            INSERT INTO template_deployments (template_id, node_id, deployed_at, status, result_message)
            VALUES (?, ?, ?, ?, ?)
            """,
            (template_id, node_id, now, status, result_message),
        )

        deployment_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return deployment_id  # type: ignore[return-value]

    @staticmethod
    def update_deployment_status(
        deployment_id: int,
        status: str,
        result_message: str | None = None,
    ) -> None:
        """
        Update a deployment status.

        Args:
            deployment_id: The deployment ID
            status: New status
            result_message: Optional result message
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE template_deployments
            SET status = ?, result_message = ?
            WHERE id = ?
            """,
            (status, result_message, deployment_id),
        )

        conn.commit()
        conn.close()

    @staticmethod
    def get_deployment_history(
        template_id: int | None = None,
        node_id: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Get deployment history, optionally filtered.

        Args:
            template_id: Filter by template (optional)
            node_id: Filter by node (optional)
            limit: Maximum records to return

        Returns:
            List of deployment records with template and node info
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                td.*,
                ct.name as template_name,
                ct.template_type,
                ni.long_name as node_name,
                ni.hex_id as node_hex
            FROM template_deployments td
            JOIN config_templates ct ON td.template_id = ct.id
            LEFT JOIN node_info ni ON td.node_id = ni.node_id
        """

        conditions = []
        params: list[Any] = []

        if template_id:
            conditions.append("td.template_id = ?")
            params.append(template_id)
        if node_id:
            conditions.append("td.node_id = ?")
            params.append(node_id)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY td.deployed_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    # =========================
    # Node Backup Methods
    # =========================

    @staticmethod
    def create_backup(
        node_id: int,
        backup_name: str,
        backup_data: str,
        description: str | None = None,
        node_long_name: str | None = None,
        node_short_name: str | None = None,
        node_hex_id: str | None = None,
        hardware_model: str | None = None,
        firmware_version: str | None = None,
    ) -> int:
        """
        Create a new node configuration backup.

        Args:
            node_id: The node ID the backup was taken from
            backup_name: Name/label for this backup
            backup_data: JSON string containing all config data
            description: Optional description
            node_long_name: Long name of the node at backup time
            node_short_name: Short name of the node at backup time
            node_hex_id: Hex ID of the node
            hardware_model: Hardware model at backup time
            firmware_version: Firmware version at backup time

        Returns:
            The ID of the created backup
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO node_backups
            (node_id, backup_name, description, backup_data, created_at,
             node_long_name, node_short_name, node_hex_id, hardware_model, firmware_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                backup_name,
                description,
                backup_data,
                time.time(),
                node_long_name,
                node_short_name,
                node_hex_id,
                hardware_model,
                firmware_version,
            ),
        )

        backup_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return backup_id  # type: ignore[return-value]

    @staticmethod
    def get_backup(backup_id: int) -> dict[str, Any] | None:
        """
        Get a backup by ID.

        Args:
            backup_id: The backup ID

        Returns:
            Backup record as dict, or None if not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM node_backups WHERE id = ?", (backup_id,))
        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def get_backups(
        node_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Get backups, optionally filtered by node.

        Args:
            node_id: Optional node ID to filter by
            limit: Maximum number of backups to return

        Returns:
            List of backup records
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        if node_id:
            cursor.execute(
                """
                SELECT * FROM node_backups
                WHERE node_id = ?
                ORDER BY created_at DESC LIMIT ?
                """,
                (node_id, limit),
            )
        else:
            cursor.execute(
                "SELECT * FROM node_backups ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    @staticmethod
    def delete_backup(backup_id: int) -> bool:
        """
        Delete a backup by ID.

        Args:
            backup_id: The backup ID to delete

        Returns:
            True if deleted, False if not found
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM node_backups WHERE id = ?", (backup_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()

        return deleted

    @staticmethod
    def update_backup(
        backup_id: int,
        backup_name: str | None = None,
        description: str | None = None,
    ) -> bool:
        """
        Update a backup's metadata.

        Args:
            backup_id: The backup ID to update
            backup_name: New name (optional)
            description: New description (optional)

        Returns:
            True if updated, False if not found
        """
        updates = []
        params: list[Any] = []

        if backup_name is not None:
            updates.append("backup_name = ?")
            params.append(backup_name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)

        if not updates:
            return False

        params.append(backup_id)

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"UPDATE node_backups SET {', '.join(updates)} WHERE id = ?",
            params,
        )

        updated = cursor.rowcount > 0
        conn.commit()
        conn.close()

        return updated

    # =========================
    # Compliance Check Methods
    # =========================

    @staticmethod
    def save_compliance_check(
        template_id: int,
        node_id: int,
        is_compliant: bool,
        diff_data: str | None = None,
        error_message: str | None = None,
    ) -> int:
        """
        Save a compliance check result.

        Args:
            template_id: The template used for the check
            node_id: The node that was checked
            is_compliant: Whether the node matches the template
            diff_data: JSON string of the differences found
            error_message: Error message if the check failed

        Returns:
            The ID of the created record
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO compliance_checks
            (template_id, node_id, checked_at, is_compliant, diff_data, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                template_id,
                node_id,
                time.time(),
                1 if is_compliant else 0,
                diff_data,
                error_message,
            ),
        )

        check_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return check_id if check_id else 0

    @staticmethod
    def get_latest_compliance_results(
        template_id: int,
    ) -> list[dict[str, Any]]:
        """
        Get the latest compliance check results for a template.

        Returns only the most recent check for each node.

        Args:
            template_id: The template ID

        Returns:
            List of compliance results with node info
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get latest check for each node using window function or subquery
        cursor.execute(
            """
            SELECT
                cc.id,
                cc.template_id,
                cc.node_id,
                cc.checked_at,
                cc.is_compliant,
                cc.diff_data,
                cc.error_message,
                ni.long_name as node_name,
                ni.short_name,
                ni.hex_id as node_hex,
                ni.hw_model
            FROM compliance_checks cc
            LEFT JOIN node_info ni ON cc.node_id = ni.node_id
            WHERE cc.template_id = ?
            AND cc.id = (
                SELECT MAX(cc2.id)
                FROM compliance_checks cc2
                WHERE cc2.template_id = cc.template_id
                AND cc2.node_id = cc.node_id
            )
            ORDER BY cc.is_compliant ASC, ni.long_name ASC
            """,
            (template_id,),
        )

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    @staticmethod
    def get_compliance_summary(template_id: int) -> dict[str, Any]:
        """
        Get a summary of compliance for a template.

        Args:
            template_id: The template ID

        Returns:
            Summary with counts of compliant/non-compliant nodes
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get latest check for each node and count compliance
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN latest.is_compliant = 1 THEN 1 ELSE 0 END) as compliant_count,
                SUM(CASE WHEN latest.is_compliant = 0 THEN 1 ELSE 0 END) as non_compliant_count,
                SUM(CASE WHEN latest.error_message IS NOT NULL THEN 1 ELSE 0 END) as error_count,
                COUNT(*) as total_checked,
                MAX(latest.checked_at) as last_checked
            FROM (
                SELECT cc.*
                FROM compliance_checks cc
                WHERE cc.template_id = ?
                AND cc.id = (
                    SELECT MAX(cc2.id)
                    FROM compliance_checks cc2
                    WHERE cc2.template_id = cc.template_id
                    AND cc2.node_id = cc.node_id
                )
            ) as latest
            """,
            (template_id,),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return {
            "compliant_count": 0,
            "non_compliant_count": 0,
            "error_count": 0,
            "total_checked": 0,
            "last_checked": None,
        }

    @staticmethod
    def clear_compliance_history(template_id: int) -> int:
        """
        Clear all compliance check history for a template.

        Args:
            template_id: The template ID

        Returns:
            Number of records deleted
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM compliance_checks WHERE template_id = ?",
            (template_id,),
        )

        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        return deleted
