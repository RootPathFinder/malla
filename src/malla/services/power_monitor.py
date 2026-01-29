"""
Background service for periodic power type detection and monitoring.
"""

import logging
import sys
import threading
import time

from ..database.repositories import BatteryAnalyticsRepository

logger = logging.getLogger(__name__)

# Global state for the background monitor
_monitor_thread = None
_monitor_stop_event = threading.Event()
_monitor_interval_seconds = 1800  # Default: run every 30 minutes


def _safe_log(level: int, message: str) -> None:
    """
    Safely log a message, handling the case where logging is shutting down.

    During Python interpreter shutdown, the logging module may have closed
    its handlers, causing "I/O operation on closed file" errors.
    We also temporarily disable the logger's error handler to prevent
    the logging module from printing its own error messages.
    """
    if sys.is_finalizing():
        return

    # Temporarily suppress logging errors during shutdown
    old_raiseExceptions = logging.raiseExceptions
    try:
        logging.raiseExceptions = False
        logger.log(level, message)
    except (ValueError, OSError, AttributeError):
        # Logging system is shutting down, silently ignore
        pass
    finally:
        logging.raiseExceptions = old_raiseExceptions


def start_power_monitor(interval_seconds: int = 1800) -> None:
    """
    Start background thread for periodic power type detection.

    Args:
        interval_seconds: How often to run detection (default: 1800 = 30 minutes)
    """
    global _monitor_thread, _monitor_interval_seconds

    if _monitor_thread is not None and _monitor_thread.is_alive():
        logger.warning("Power monitor thread already running")
        return

    _monitor_interval_seconds = interval_seconds
    _monitor_stop_event.clear()

    _monitor_thread = threading.Thread(
        target=_power_monitor_worker, name="PowerMonitorThread", daemon=True
    )
    _monitor_thread.start()
    logger.info(f"Power monitor started (running every {interval_seconds}s)")


def stop_power_monitor() -> None:
    """Stop the background power monitor thread."""
    global _monitor_thread

    if _monitor_thread is None or not _monitor_thread.is_alive():
        _safe_log(logging.DEBUG, "Power monitor thread not running")
        return

    _safe_log(logging.INFO, "Stopping power monitor thread...")
    _monitor_stop_event.set()
    _monitor_thread.join(timeout=5)
    _monitor_thread = None
    _safe_log(logging.INFO, "Power monitor thread stopped")


def _power_monitor_worker() -> None:
    """
    Background worker that periodically detects and updates power types.
    Runs every 30 minutes by default (configurable).
    """
    _safe_log(logging.INFO, "Power monitor worker started")

    # Run shortly after startup (after a brief delay for system to stabilize)
    time.sleep(30)  # Wait 30 seconds after startup

    while not _monitor_stop_event.is_set():
        try:
            _safe_log(logging.INFO, "Running scheduled power type detection...")
            results = BatteryAnalyticsRepository.detect_and_update_power_types()

            total_updated = sum(results.values())
            if total_updated > 0:
                _safe_log(
                    logging.INFO,
                    f"Power type detection complete: {results} ({total_updated} nodes updated)",
                )
            else:
                _safe_log(
                    logging.DEBUG, "Power type detection complete: no updates needed"
                )

        except Exception as e:
            _safe_log(logging.ERROR, f"Error in power monitor worker: {e}")

        # Wait for next interval or stop signal
        _monitor_stop_event.wait(_monitor_interval_seconds)

    _safe_log(logging.INFO, "Power monitor worker stopped")
