"""
Connection pooling for SQLite database to improve performance under concurrent access.

This module provides a simple connection pool that reuses database connections
instead of creating new ones for each request, reducing overhead and improving
performance.
"""

import logging
import sqlite3
import threading
import time
from collections import deque

from malla.config import get_config

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Thread-safe connection pool for SQLite database connections."""

    def __init__(
        self,
        max_connections: int = 10,
        min_connections: int = 2,
        connection_timeout: float = 30.0,
    ):
        """
        Initialize the connection pool.

        Args:
            max_connections: Maximum number of connections in the pool
            min_connections: Minimum number of connections to maintain
            connection_timeout: Timeout in seconds for acquiring a connection
        """
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout = connection_timeout
        self._pool: deque = deque()
        self._pool_lock = threading.Lock()
        self._active_connections = 0
        self._total_connections = 0
        self._db_path: str | None = None

        # Statistics
        self._stats_lock = threading.Lock()
        self._connections_created = 0
        self._connections_reused = 0
        self._wait_timeouts = 0

        # Initialize minimum connections
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the pool with minimum number of connections."""
        import os

        # Resolve DB path
        self._db_path = (
            os.getenv("MALLA_DATABASE_FILE")
            or get_config().database_file
            or "meshtastic_history.db"
        )

        logger.info(
            f"Initializing connection pool with {self.min_connections} connections to {self._db_path}"
        )

        with self._pool_lock:
            for _ in range(self.min_connections):
                conn = self._create_new_connection()
                if conn:
                    self._pool.append(conn)
                    self._total_connections += 1

    def _create_new_connection(self) -> sqlite3.Connection | None:
        """
        Create a new database connection with proper configuration.

        Returns:
            sqlite3.Connection or None if creation fails
        """
        if self._db_path is None:
            logger.error("Database path is not configured.")
            return None
        try:
            conn = sqlite3.connect(self._db_path, timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row

            cursor = conn.cursor()
            # Configure for optimal performance and concurrency
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA cache_size=10000")
            cursor.execute("PRAGMA temp_store=MEMORY")

            with self._stats_lock:
                self._connections_created += 1

            logger.debug(
                f"Created new database connection (total: {self._total_connections + 1})"
            )
            return conn

        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            return None

    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool or create a new one if needed.

        Returns:
            sqlite3.Connection: A database connection

        Raises:
            TimeoutError: If no connection available within timeout period
            RuntimeError: If connection creation fails
        """
        start_time = time.time()

        while True:
            # Try to get a connection from the pool
            with self._pool_lock:
                if self._pool:
                    conn = self._pool.popleft()
                    self._active_connections += 1
                    with self._stats_lock:
                        self._connections_reused += 1
                    logger.debug(
                        f"Reused connection from pool (active: {self._active_connections}/{self._total_connections})"
                    )
                    return conn

                # If pool is empty and we haven't reached max, create new connection
                if self._total_connections < self.max_connections:
                    conn = self._create_new_connection()
                    if conn:
                        self._total_connections += 1
                        self._active_connections += 1
                        return conn
                    else:
                        raise RuntimeError("Failed to create database connection")

            # Check timeout
            if time.time() - start_time > self.connection_timeout:
                with self._stats_lock:
                    self._wait_timeouts += 1
                logger.warning(
                    f"Connection pool timeout after {self.connection_timeout}s "
                    f"(active: {self._active_connections}/{self._total_connections})"
                )
                raise TimeoutError(
                    f"Could not acquire database connection within {self.connection_timeout}s"
                )

            # Wait a bit before retrying
            time.sleep(0.01)

    def return_connection(self, conn: sqlite3.Connection) -> None:
        """
        Return a connection to the pool.

        Args:
            conn: The connection to return to the pool
        """
        if conn is None:
            return

        try:
            # Rollback any uncommitted transactions
            conn.rollback()

            with self._pool_lock:
                self._active_connections -= 1

                # Only keep connection if pool isn't full
                if len(self._pool) < self.max_connections:
                    self._pool.append(conn)
                    logger.debug(
                        f"Returned connection to pool (available: {len(self._pool)}, "
                        f"active: {self._active_connections})"
                    )
                else:
                    # Close excess connection
                    conn.close()
                    self._total_connections -= 1
                    logger.debug("Closed excess connection")

        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")
            # Close the connection on error
            try:
                conn.close()
                with self._pool_lock:
                    self._total_connections -= 1
            except Exception:
                pass

    def close_all(self) -> None:
        """Close all connections in the pool."""
        logger.info("Closing all connections in pool")
        with self._pool_lock:
            while self._pool:
                conn = self._pool.popleft()
                try:
                    conn.close()
                    self._total_connections -= 1
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")

    def get_stats(self) -> dict:
        """
        Get connection pool statistics.

        Returns:
            dict: Statistics about pool usage
        """
        with self._pool_lock:
            available = len(self._pool)
            active = self._active_connections
            total = self._total_connections

        with self._stats_lock:
            created = self._connections_created
            reused = self._connections_reused
            timeouts = self._wait_timeouts

        return {
            "available_connections": available,
            "active_connections": active,
            "total_connections": total,
            "max_connections": self.max_connections,
            "connections_created": created,
            "connections_reused": reused,
            "wait_timeouts": timeouts,
            "reuse_rate": (
                round(reused / (created + reused) * 100, 2)
                if (created + reused) > 0
                else 0
            ),
        }


# Global connection pool instance
_connection_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def get_connection_pool(
    max_connections: int = 10, reset: bool = False
) -> ConnectionPool:
    """
    Get or create the global connection pool.

    Args:
        max_connections: Maximum number of connections (only used on first call)
        reset: If True, close existing pool and create new one

    Returns:
        ConnectionPool: The global connection pool instance
    """
    global _connection_pool

    with _pool_lock:
        if reset and _connection_pool:
            _connection_pool.close_all()
            _connection_pool = None

        if _connection_pool is None:
            _connection_pool = ConnectionPool(max_connections=max_connections)

        return _connection_pool


def close_connection_pool() -> None:
    """Close the global connection pool."""
    global _connection_pool

    with _pool_lock:
        if _connection_pool:
            _connection_pool.close_all()
            _connection_pool = None
