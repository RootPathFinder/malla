"""
Performance monitoring and profiling utilities.

This module provides decorators and utilities for tracking performance metrics,
identifying slow operations, and monitoring application health.
"""

import functools
import logging
import time
from collections import defaultdict
from datetime import datetime
from threading import Lock
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """Thread-safe container for performance metrics."""

    def __init__(self):
        """Initialize performance metrics storage."""
        self._metrics: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "call_count": 0,
                "total_time": 0.0,
                "min_time": float("inf"),
                "max_time": 0.0,
                "errors": 0,
                "last_called": None,
            }
        )
        self._lock = Lock()

    def record_call(
        self, function_name: str, duration: float, error: bool = False
    ) -> None:
        """
        Record a function call with its duration.

        Args:
            function_name: Name of the function
            duration: Execution time in seconds
            error: Whether the call resulted in an error
        """
        with self._lock:
            metrics = self._metrics[function_name]
            metrics["call_count"] += 1
            metrics["total_time"] += duration
            metrics["min_time"] = min(metrics["min_time"], duration)
            metrics["max_time"] = max(metrics["max_time"], duration)
            metrics["last_called"] = datetime.now().isoformat()

            if error:
                metrics["errors"] += 1

    def get_metrics(self, function_name: Optional[str] = None) -> dict:
        """
        Get performance metrics.

        Args:
            function_name: Specific function name, or None for all metrics

        Returns:
            dict: Performance metrics
        """
        with self._lock:
            if function_name:
                if function_name not in self._metrics:
                    return {}

                metrics = dict(self._metrics[function_name])
                metrics["avg_time"] = (
                    metrics["total_time"] / metrics["call_count"]
                    if metrics["call_count"] > 0
                    else 0.0
                )
                metrics["error_rate"] = (
                    metrics["errors"] / metrics["call_count"] * 100
                    if metrics["call_count"] > 0
                    else 0.0
                )
                return metrics

            # Return all metrics
            result = {}
            for name, metrics in self._metrics.items():
                m = dict(metrics)
                m["avg_time"] = (
                    m["total_time"] / m["call_count"] if m["call_count"] > 0 else 0.0
                )
                m["error_rate"] = (
                    m["errors"] / m["call_count"] * 100 if m["call_count"] > 0 else 0.0
                )
                result[name] = m

            return result

    def get_slow_functions(self, threshold: float = 1.0, limit: int = 10) -> list[dict]:
        """
        Get list of slow functions exceeding threshold.

        Args:
            threshold: Minimum average time in seconds
            limit: Maximum number of results

        Returns:
            list[dict]: List of slow functions with their metrics
        """
        with self._lock:
            slow_funcs = []

            for name, metrics in self._metrics.items():
                if metrics["call_count"] == 0:
                    continue

                avg_time = metrics["total_time"] / metrics["call_count"]
                if avg_time >= threshold:
                    slow_funcs.append(
                        {
                            "function": name,
                            "avg_time": round(avg_time, 3),
                            "max_time": round(metrics["max_time"], 3),
                            "call_count": metrics["call_count"],
                            "total_time": round(metrics["total_time"], 3),
                        }
                    )

            # Sort by average time descending
            slow_funcs.sort(key=lambda x: x["avg_time"], reverse=True)
            return slow_funcs[:limit]

    def reset(self, function_name: Optional[str] = None) -> None:
        """
        Reset metrics.

        Args:
            function_name: Specific function to reset, or None to reset all
        """
        with self._lock:
            if function_name:
                if function_name in self._metrics:
                    del self._metrics[function_name]
            else:
                self._metrics.clear()


# Global metrics instance
_metrics = PerformanceMetrics()


def get_metrics(function_name: Optional[str] = None) -> dict:
    """
    Get performance metrics.

    Args:
        function_name: Specific function name, or None for all metrics

    Returns:
        dict: Performance metrics
    """
    return _metrics.get_metrics(function_name)


def get_slow_functions(threshold: float = 1.0, limit: int = 10) -> list[dict]:
    """
    Get list of slow functions.

    Args:
        threshold: Minimum average time in seconds
        limit: Maximum number of results

    Returns:
        list[dict]: List of slow functions
    """
    return _metrics.get_slow_functions(threshold, limit)


def reset_metrics(function_name: Optional[str] = None) -> None:
    """
    Reset performance metrics.

    Args:
        function_name: Specific function to reset, or None to reset all
    """
    _metrics.reset(function_name)


def track_performance(
    log_threshold: float = 1.0, log_all: bool = False
) -> Callable:
    """
    Decorator to track function performance.

    Args:
        log_threshold: Log warning if execution exceeds this threshold (seconds)
        log_all: If True, log all calls regardless of duration

    Returns:
        Callable: Decorated function

    Example:
        @track_performance(log_threshold=0.5)
        def slow_function():
            time.sleep(1)
    """

    def decorator(func: Callable) -> Callable:
        function_name = f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            error_occurred = False

            try:
                result = func(*args, **kwargs)
                return result

            except Exception as e:
                error_occurred = True
                raise

            finally:
                duration = time.time() - start_time
                _metrics.record_call(function_name, duration, error_occurred)

                if log_all or duration >= log_threshold:
                    level = logging.WARNING if duration >= log_threshold else logging.DEBUG
                    logger.log(
                        level,
                        f"Performance: {function_name} took {duration:.3f}s"
                        + (f" [ERROR]" if error_occurred else ""),
                    )

        return wrapper

    return decorator


def track_db_query(warn_threshold: float = 1.0) -> Callable:
    """
    Decorator specifically for tracking database query performance.

    Args:
        warn_threshold: Warn if query exceeds this threshold (seconds)

    Returns:
        Callable: Decorated function

    Example:
        @track_db_query(warn_threshold=0.5)
        def get_nodes():
            # ... database query ...
            pass
    """

    def decorator(func: Callable) -> Callable:
        function_name = f"DB:{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            error_occurred = False

            try:
                result = func(*args, **kwargs)
                return result

            except Exception as e:
                error_occurred = True
                raise

            finally:
                duration = time.time() - start_time
                _metrics.record_call(function_name, duration, error_occurred)

                if duration >= warn_threshold:
                    logger.warning(
                        f"Slow database query: {function_name} took {duration:.3f}s"
                        + (f" [ERROR]" if error_occurred else "")
                    )
                else:
                    logger.debug(f"DB query: {function_name} took {duration:.3f}s")

        return wrapper

    return decorator


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str, log_threshold: float = 0.0):
        """
        Initialize timer.

        Args:
            name: Name for the timed operation
            log_threshold: Log if duration exceeds threshold (0 = always log)
        """
        self.name = name
        self.log_threshold = log_threshold
        self.start_time = None
        self.duration = None

    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and log if needed."""
        self.duration = time.time() - self.start_time

        if self.duration >= self.log_threshold:
            logger.info(f"Timer '{self.name}': {self.duration:.3f}s")

        return False  # Don't suppress exceptions


def measure_time(operation: str) -> Timer:
    """
    Create a timer for measuring operation duration.

    Args:
        operation: Description of the operation being timed

    Returns:
        Timer: Context manager for timing

    Example:
        with measure_time("expensive operation"):
            # ... code to time ...
            pass
    """
    return Timer(operation, log_threshold=0.5)
