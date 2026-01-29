"""
Safe logging utilities for background threads.

Provides logging functions that gracefully handle Python shutdown scenarios
where the logging subsystem may already be closed.
"""

import logging
import sys
from typing import Any


def safe_log(
    logger: logging.Logger, level: int, message: str, *args: Any, **kwargs: Any
) -> None:
    """
    Log a message safely, suppressing errors during Python shutdown.

    During interpreter shutdown, logging handlers may be closed which causes
    "I/O operation on closed file" errors. This function suppresses those
    errors to prevent noisy traceback output.

    Args:
        logger: The logger instance to use
        level: The logging level (e.g., logging.INFO, logging.ERROR)
        message: The log message
        *args: Positional arguments passed to the logger
        **kwargs: Keyword arguments passed to the logger (e.g., exc_info=True)
    """
    if sys.is_finalizing():
        # Python is shutting down, skip logging entirely
        return

    # Temporarily suppress logging errors
    old_raise = logging.raiseExceptions
    try:
        logging.raiseExceptions = False
        logger.log(level, message, *args, **kwargs)
    except (ValueError, OSError, AttributeError):
        # Ignore logging errors during shutdown
        pass
    finally:
        logging.raiseExceptions = old_raise


def safe_debug(logger: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
    """Log a debug message safely."""
    safe_log(logger, logging.DEBUG, message, *args, **kwargs)


def safe_info(logger: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
    """Log an info message safely."""
    safe_log(logger, logging.INFO, message, *args, **kwargs)


def safe_warning(
    logger: logging.Logger, message: str, *args: Any, **kwargs: Any
) -> None:
    """Log a warning message safely."""
    safe_log(logger, logging.WARNING, message, *args, **kwargs)


def safe_error(logger: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
    """Log an error message safely."""
    safe_log(logger, logging.ERROR, message, *args, **kwargs)
