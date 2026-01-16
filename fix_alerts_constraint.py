#!/usr/bin/env python3
"""
Fix the alerts table UNIQUE constraint issue.
This script removes the bad UNIQUE(alert_type, node_id, resolved) constraint
and relies only on the partial unique index for unresolved alerts.
"""

import logging
import sqlite3
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_alerts_table(db_path: str = "meshtastic_history.db"):
    """Fix the alerts table constraint."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check current schema
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='alerts'"
        )
        table_schema = cursor.fetchone()

        if not table_schema:
            logger.error("Alerts table does not exist!")
            return False

        schema_sql = table_schema[0]
        logger.info(f"Current schema:\n{schema_sql}")

        if "UNIQUE(alert_type, node_id, resolved)" not in schema_sql:
            logger.info("Table already has correct schema (no bad constraint)")
            return True

        logger.info("Found bad UNIQUE constraint, migrating table...")

        # Create new table without the bad constraint
        cursor.execute("""
            CREATE TABLE alerts_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                node_id INTEGER,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp REAL NOT NULL,
                resolved BOOLEAN DEFAULT 0,
                resolved_at REAL,
                metadata TEXT
            )
        """)

        # Copy all data
        cursor.execute("""
            INSERT INTO alerts_new
            SELECT id, alert_type, severity, node_id, title, message,
                   timestamp, resolved, resolved_at, metadata
            FROM alerts
        """)

        row_count = cursor.rowcount
        logger.info(f"Copied {row_count} rows")

        # Drop old table and rename new one
        cursor.execute("DROP TABLE alerts")
        cursor.execute("ALTER TABLE alerts_new RENAME TO alerts")

        # Recreate indexes
        cursor.execute("CREATE INDEX idx_alerts_resolved ON alerts(resolved)")
        cursor.execute("CREATE INDEX idx_alerts_severity ON alerts(severity)")
        cursor.execute("CREATE INDEX idx_alerts_node_id ON alerts(node_id)")
        cursor.execute("CREATE INDEX idx_alerts_timestamp ON alerts(timestamp)")
        cursor.execute(
            """CREATE UNIQUE INDEX idx_alerts_unique_active
               ON alerts(alert_type, node_id) WHERE resolved = 0"""
        )

        conn.commit()
        logger.info("Migration complete - bad constraint removed")

        # Verify new schema
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='alerts'"
        )
        new_schema = cursor.fetchone()[0]
        logger.info(f"New schema:\n{new_schema}")

        return True

    except Exception as e:
        logger.error(f"Error during migration: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "meshtastic_history.db"
    success = fix_alerts_table(db_path)
    sys.exit(0 if success else 1)
