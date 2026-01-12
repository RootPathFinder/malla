"""
Background service for periodic power type detection and monitoring.
"""

import logging
import threading
import time

from ..database.repositories import BatteryAnalyticsRepository

logger = logging.getLogger(__name__)

# Global state for the background monitor
_monitor_thread = None
_monitor_stop_event = threading.Event()
_monitor_interval_seconds = 3600  # Default: run every hour


def start_power_monitor(interval_seconds: int = 3600) -> None:
    """
    Start background thread for periodic power type detection.

    Args:
        interval_seconds: How often to run detection (default: 3600 = 1 hour)
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
        logger.debug("Power monitor thread not running")
        return

    logger.info("Stopping power monitor thread...")
    _monitor_stop_event.set()
    _monitor_thread.join(timeout=5)
    _monitor_thread = None
    logger.info("Power monitor thread stopped")


def _power_monitor_worker() -> None:
    """
    Background worker that periodically detects and updates power types.
    Runs every hour by default.
    """
    logger.info("Power monitor worker started")

    # Run immediately on startup (after a short delay)
    time.sleep(60)  # Wait 1 minute after startup

    while not _monitor_stop_event.is_set():
        try:
            logger.info("Running scheduled power type detection...")
            results = BatteryAnalyticsRepository.detect_and_update_power_types()

            total_updated = sum(results.values())
            if total_updated > 0:
                logger.info(
                    f"Power type detection complete: {results} ({total_updated} nodes updated)"
                )
            else:
                logger.debug("Power type detection complete: no updates needed")

        except Exception as e:
            logger.error(f"Error in power monitor worker: {e}", exc_info=True)

        # Wait for next interval or stop signal
        _monitor_stop_event.wait(_monitor_interval_seconds)

    logger.info("Power monitor worker stopped")
