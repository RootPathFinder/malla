"""
Log service for capturing and exposing application logs.

Provides an in-memory ring buffer of recent log entries that can be
accessed via the admin API for real-time log viewing.
"""

import logging
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any

# Maximum number of log entries to keep in memory
MAX_LOG_ENTRIES = 1000

# Log levels for filtering
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


@dataclass
class LogEntry:
    """Represents a single log entry."""

    timestamp: float
    level: str
    level_no: int
    logger_name: str
    message: str
    module: str
    func_name: str
    line_no: int
    exc_info: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp,
            "level": self.level,
            "level_no": self.level_no,
            "logger_name": self.logger_name,
            "message": self.message,
            "module": self.module,
            "func_name": self.func_name,
            "line_no": self.line_no,
            "exc_info": self.exc_info,
        }


class MemoryLogHandler(logging.Handler):
    """
    A logging handler that stores log entries in a thread-safe ring buffer.

    This handler captures log entries and makes them available for querying
    via the admin interface.
    """

    def __init__(self, max_entries: int = MAX_LOG_ENTRIES):
        super().__init__()
        self.max_entries = max_entries
        self._buffer: deque[LogEntry] = deque(maxlen=max_entries)
        self._lock = threading.Lock()
        self._entry_id = 0

    def emit(self, record: logging.LogRecord) -> None:
        """Process a log record and add it to the buffer."""
        try:
            # Format exception info if present
            exc_info = None
            if record.exc_info:
                exc_info = (
                    self.formatter.formatException(record.exc_info)
                    if self.formatter
                    else str(record.exc_info)
                )

            entry = LogEntry(
                timestamp=record.created,
                level=record.levelname,
                level_no=record.levelno,
                logger_name=record.name,
                message=record.getMessage(),
                module=record.module,
                func_name=record.funcName,
                line_no=record.lineno,
                exc_info=exc_info,
            )

            with self._lock:
                self._buffer.append(entry)
                self._entry_id += 1

        except Exception:
            # Don't let logging errors crash the application
            self.handleError(record)

    def get_logs(
        self,
        limit: int = 100,
        min_level: str = "DEBUG",
        logger_filter: str | None = None,
        search: str | None = None,
        since_timestamp: float | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get recent log entries with optional filtering.

        Args:
            limit: Maximum number of entries to return
            min_level: Minimum log level to include
            logger_filter: Filter by logger name (partial match)
            search: Search text in message
            since_timestamp: Only return entries after this timestamp

        Returns:
            List of log entry dictionaries, newest first
        """
        min_level_no = LOG_LEVELS.get(min_level.upper(), logging.DEBUG)

        with self._lock:
            # Filter entries
            filtered = []
            for entry in reversed(self._buffer):
                # Level filter
                if entry.level_no < min_level_no:
                    continue

                # Timestamp filter
                if since_timestamp and entry.timestamp <= since_timestamp:
                    continue

                # Logger filter
                if (
                    logger_filter
                    and logger_filter.lower() not in entry.logger_name.lower()
                ):
                    continue

                # Search filter
                if search and search.lower() not in entry.message.lower():
                    continue

                filtered.append(entry.to_dict())

                if len(filtered) >= limit:
                    break

            return filtered

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about the log buffer."""
        with self._lock:
            level_counts = {
                "DEBUG": 0,
                "INFO": 0,
                "WARNING": 0,
                "ERROR": 0,
                "CRITICAL": 0,
            }
            for entry in self._buffer:
                if entry.level in level_counts:
                    level_counts[entry.level] += 1

            oldest_timestamp = self._buffer[0].timestamp if self._buffer else None
            newest_timestamp = self._buffer[-1].timestamp if self._buffer else None

            return {
                "total_entries": len(self._buffer),
                "max_entries": self.max_entries,
                "level_counts": level_counts,
                "oldest_timestamp": oldest_timestamp,
                "newest_timestamp": newest_timestamp,
            }

    def clear(self) -> None:
        """Clear all log entries from the buffer."""
        with self._lock:
            self._buffer.clear()


# Global log handler instance
_log_handler: MemoryLogHandler | None = None
_log_handler_lock = threading.Lock()


def get_log_handler() -> MemoryLogHandler:
    """Get or create the global log handler instance."""
    global _log_handler
    with _log_handler_lock:
        if _log_handler is None:
            _log_handler = MemoryLogHandler()
            # Set a formatter for exception formatting
            _log_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
        return _log_handler


def install_log_handler(level: int = logging.DEBUG) -> None:
    """
    Install the memory log handler on the root logger.

    This should be called early in application startup to capture all logs.
    """
    handler = get_log_handler()
    handler.setLevel(level)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)


def get_logs(
    limit: int = 100,
    min_level: str = "DEBUG",
    logger_filter: str | None = None,
    search: str | None = None,
    since_timestamp: float | None = None,
) -> list[dict[str, Any]]:
    """
    Get recent log entries.

    Convenience function that delegates to the global handler.
    """
    handler = get_log_handler()
    return handler.get_logs(
        limit=limit,
        min_level=min_level,
        logger_filter=logger_filter,
        search=search,
        since_timestamp=since_timestamp,
    )


def get_log_stats() -> dict[str, Any]:
    """Get log statistics."""
    handler = get_log_handler()
    return handler.get_stats()


def clear_logs() -> None:
    """Clear all captured logs."""
    handler = get_log_handler()
    handler.clear()
