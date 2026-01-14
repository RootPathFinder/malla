"""
Database connection management for Meshtastic Mesh Health Web UI.
"""

import logging
import os
import sqlite3

# Prefer configuration loader over environment variables
from malla.config import get_config

logger = logging.getLogger(__name__)


def get_db_connection() -> sqlite3.Connection:
    """
    Get a connection to the SQLite database with proper concurrency configuration.

    Returns:
        sqlite3.Connection: Database connection with row factory set and WAL mode enabled
    """
    # Resolve DB path:
    # 1. Explicit override via `MALLA_DATABASE_FILE` env-var (handy for scripts)
    # 2. Value from YAML configuration
    # 3. Fallback to hard-coded default

    db_path: str = (
        os.getenv("MALLA_DATABASE_FILE")
        or get_config().database_file
        or "meshtastic_history.db"
    )

    try:
        conn = sqlite3.connect(
            db_path, timeout=30.0
        )  # 30 second timeout for busy database
        conn.row_factory = sqlite3.Row  # Enable column access by name

        # Configure SQLite for better concurrency
        cursor = conn.cursor()

        # Enable WAL mode for better concurrent read/write performance
        cursor.execute("PRAGMA journal_mode=WAL")

        # Set synchronous to NORMAL for better performance while maintaining safety
        cursor.execute("PRAGMA synchronous=NORMAL")

        # Set busy timeout to handle concurrent access
        cursor.execute("PRAGMA busy_timeout=30000")  # 30 seconds

        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys=ON")

        # Optimize for read performance
        cursor.execute("PRAGMA cache_size=10000")  # 10MB cache
        cursor.execute("PRAGMA temp_store=MEMORY")

        # ------------------------------------------------------------------
        # Lightweight schema migrations – run once per connection.
        # ------------------------------------------------------------------
        try:
            _ensure_schema_migrations(cursor)
        except Exception as e:
            logger.warning(f"Schema migration check failed: {e}")

        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def init_database() -> None:
    """
    Initialize the database connection and verify it's accessible.
    This function is called during application startup.
    """
    # Resolve DB path:
    # 1. Explicit override via `MALLA_DATABASE_FILE` env-var (handy for scripts)
    # 2. Value from YAML configuration
    # 3. Fallback to hard-coded default

    db_path: str = (
        os.getenv("MALLA_DATABASE_FILE")
        or get_config().database_file
        or "meshtastic_history.db"
    )

    logger.info(f"Initializing database connection to: {db_path}")

    try:
        # Test the connection
        conn = get_db_connection()

        # Test a simple query to verify the database is accessible
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]

        # Check and log the journal mode
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]

        conn.close()

        logger.info(
            f"Database connection successful - found {table_count} tables, journal_mode: {journal_mode}"
        )

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        # Don't raise the exception - let the app start anyway
        # The database might not exist yet or be created by another process


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------


_SCHEMA_MIGRATIONS_DONE: set[str] = set()


def _ensure_schema_migrations(cursor: sqlite3.Cursor) -> None:
    """Run any idempotent schema updates that the application depends on.

    Currently this checks that ``node_info`` has a ``primary_channel`` column
    (added in April 2024) so queries that reference it do not fail when the
    database was created with an older version of the schema.

    The function is **safe** to run repeatedly – it will only attempt each
    migration once per Python process and each individual migration is
    guarded with a try/except that ignores the *duplicate column* error.
    """

    global _SCHEMA_MIGRATIONS_DONE  # pylint: disable=global-statement

    # Create alert_thresholds table first (regardless of other tables)
    try:
        if "alert_thresholds" not in _SCHEMA_MIGRATIONS_DONE:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alert_thresholds (
                    key TEXT PRIMARY KEY,
                    value REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)
            logging.info("Alert thresholds table created via auto-migration")
            _SCHEMA_MIGRATIONS_DONE.add("alert_thresholds")
    except sqlite3.OperationalError as exc:
        if "already exists" in str(exc).lower():
            _SCHEMA_MIGRATIONS_DONE.add("alert_thresholds")
        else:
            raise

    # Quickly short-circuit if we've already handled other migrations in this process
    if "primary_channel" in _SCHEMA_MIGRATIONS_DONE:
        return

    try:
        # Check whether the column already exists
        cursor.execute("PRAGMA table_info(node_info)")
        columns = [row[1] for row in cursor.fetchall()]

        if "primary_channel" not in columns:
            cursor.execute("ALTER TABLE node_info ADD COLUMN primary_channel TEXT")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_node_primary_channel ON node_info(primary_channel)"
            )
            logging.info(
                "Added primary_channel column to node_info table via auto-migration"
            )

        _SCHEMA_MIGRATIONS_DONE.add("primary_channel")
    except sqlite3.OperationalError as exc:
        # Ignore errors about duplicate columns in race situations – another
        # process may have altered the table first.
        if "duplicate column name" in str(exc).lower():
            _SCHEMA_MIGRATIONS_DONE.add("primary_channel")

        else:
            raise

    # Create alerts table if it doesn't exist
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
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
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(resolved)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_node_id ON alerts(node_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp)"
        )
        # Partial unique index: only one unresolved alert per (alert_type, node_id)
        cursor.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_unique_active
               ON alerts(alert_type, node_id) WHERE resolved = 0"""
        )
        logging.info("Alerts table created via auto-migration")

        _SCHEMA_MIGRATIONS_DONE.add("alerts")
    except sqlite3.OperationalError as exc:
        if "already exists" in str(exc).lower():
            _SCHEMA_MIGRATIONS_DONE.add("alerts")
        else:
            raise
    # Migration: Fix alerts table UNIQUE constraint
    # Old constraint included 'resolved' column which caused errors when resolving alerts
    if "alerts_constraint_fix" not in _SCHEMA_MIGRATIONS_DONE:
        try:
            # Check if the old bad constraint exists by checking table schema
            cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='alerts'"
            )
            table_schema = cursor.fetchone()

            if (
                table_schema
                and "UNIQUE(alert_type, node_id, resolved)" in table_schema[0]
            ):
                logging.info("Migrating alerts table to fix UNIQUE constraint...")

                # Recreate the table without the bad constraint
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

                logging.info("Alerts table migration complete - constraint fixed")
            else:
                logging.debug("Alerts table already has correct schema")

            _SCHEMA_MIGRATIONS_DONE.add("alerts_constraint_fix")
        except Exception as exc:
            logging.error(f"Failed to migrate alerts table: {exc}")
            # Don't fail startup, just log the error
            _SCHEMA_MIGRATIONS_DONE.add("alerts_constraint_fix")
