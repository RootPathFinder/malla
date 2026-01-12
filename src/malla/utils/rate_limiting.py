"""
Rate limiting middleware for API endpoints.

Provides configurable rate limiting to prevent abuse and ensure fair resource usage.
Supports per-IP and global rate limits with customizable time windows.
"""

import functools
import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Callable, Optional

from flask import Request, jsonify, request

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe rate limiter with sliding window algorithm."""

    def __init__(
        self,
        max_requests: int = 100,
        window_seconds: int = 60,
        cleanup_interval: int = 300,
    ):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed per window
            window_seconds: Time window in seconds
            cleanup_interval: How often to clean up old entries (seconds)
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.cleanup_interval = cleanup_interval

        # Store timestamps of requests per client
        self._requests: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()
        self._last_cleanup = time.time()

        # Statistics
        self._total_requests = 0
        self._blocked_requests = 0

    def _get_client_id(self, req: Request) -> str:
        """
        Get unique identifier for client.

        Args:
            req: Flask request object

        Returns:
            str: Client identifier (IP address)
        """
        # Try to get real IP behind proxy
        forwarded_for = req.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take first IP in the chain
            return forwarded_for.split(",")[0].strip()

        return req.remote_addr or "unknown"

    def _cleanup_old_entries(self) -> None:
        """Remove expired request timestamps to prevent memory growth."""
        current_time = time.time()

        # Only cleanup periodically
        if current_time - self._last_cleanup < self.cleanup_interval:
            return

        cutoff_time = current_time - self.window_seconds

        with self._lock:
            for client_id in list(self._requests.keys()):
                # Remove timestamps older than window
                self._requests[client_id] = [
                    ts for ts in self._requests[client_id] if ts > cutoff_time
                ]

                # Remove client if no recent requests
                if not self._requests[client_id]:
                    del self._requests[client_id]

            self._last_cleanup = current_time

        logger.debug(f"Rate limiter cleanup: {len(self._requests)} active clients")

    def is_allowed(self, req: Request) -> tuple[bool, dict]:
        """
        Check if request is allowed under rate limit.

        Args:
            req: Flask request object

        Returns:
            tuple[bool, dict]: (allowed, rate_limit_info)
        """
        client_id = self._get_client_id(req)
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds

        self._cleanup_old_entries()

        with self._lock:
            # Get recent requests for this client
            client_requests = self._requests[client_id]

            # Remove old requests
            client_requests = [ts for ts in client_requests if ts > cutoff_time]
            self._requests[client_id] = client_requests

            # Check if limit exceeded
            request_count = len(client_requests)
            allowed = request_count < self.max_requests

            if allowed:
                # Add this request
                self._requests[client_id].append(current_time)
                self._total_requests += 1
            else:
                self._blocked_requests += 1

            # Calculate reset time
            if client_requests:
                oldest_request = min(client_requests)
                reset_time = oldest_request + self.window_seconds
            else:
                reset_time = current_time + self.window_seconds

            rate_limit_info = {
                "limit": self.max_requests,
                "remaining": max(0, self.max_requests - request_count - (0 if allowed else 1)),
                "reset": int(reset_time),
                "window": self.window_seconds,
            }

            return allowed, rate_limit_info

    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.

        Returns:
            dict: Statistics about rate limiting
        """
        with self._lock:
            active_clients = len(self._requests)
            total_tracked_requests = sum(len(reqs) for reqs in self._requests.values())

        return {
            "active_clients": active_clients,
            "total_requests": self._total_requests,
            "blocked_requests": self._blocked_requests,
            "block_rate": (
                round(self._blocked_requests / self._total_requests * 100, 2)
                if self._total_requests > 0
                else 0
            ),
            "tracked_requests": total_tracked_requests,
            "max_requests": self.max_requests,
            "window_seconds": self.window_seconds,
        }


# Global rate limiters for different tiers
_default_limiter = RateLimiter(max_requests=100, window_seconds=60)
_strict_limiter = RateLimiter(max_requests=20, window_seconds=60)
_generous_limiter = RateLimiter(max_requests=1000, window_seconds=60)


def rate_limit(
    max_requests: int = 100,
    window_seconds: int = 60,
    tier: Optional[str] = None,
) -> Callable:
    """
    Decorator to apply rate limiting to Flask routes.

    Args:
        max_requests: Maximum requests per window
        window_seconds: Time window in seconds
        tier: Predefined tier ('strict', 'default', 'generous') or None for custom

    Returns:
        Callable: Decorated function

    Example:
        @app.route('/api/data')
        @rate_limit(tier='strict')
        def get_data():
            return jsonify({"data": "..."})
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Select rate limiter based on tier
            if tier == "strict":
                limiter = _strict_limiter
            elif tier == "generous":
                limiter = _generous_limiter
            elif tier is None:
                # Use custom limiter
                limiter = RateLimiter(
                    max_requests=max_requests, window_seconds=window_seconds
                )
            else:
                limiter = _default_limiter

            # Check rate limit
            allowed, rate_info = limiter.is_allowed(request)

            if not allowed:
                logger.warning(
                    f"Rate limit exceeded for {limiter._get_client_id(request)} "
                    f"on {request.path}"
                )

                response = jsonify(
                    {
                        "error": "Rate limit exceeded",
                        "message": f"Too many requests. Please try again in {rate_info['reset'] - int(time.time())} seconds.",
                        "rate_limit": rate_info,
                    }
                )
                response.status_code = 429
                response.headers["X-RateLimit-Limit"] = str(rate_info["limit"])
                response.headers["X-RateLimit-Remaining"] = "0"
                response.headers["X-RateLimit-Reset"] = str(rate_info["reset"])
                response.headers["Retry-After"] = str(
                    rate_info["reset"] - int(time.time())
                )
                return response

            # Add rate limit headers to successful response
            result = func(*args, **kwargs)

            # Add headers if result is a Response object
            if hasattr(result, "headers"):
                result.headers["X-RateLimit-Limit"] = str(rate_info["limit"])
                result.headers["X-RateLimit-Remaining"] = str(rate_info["remaining"])
                result.headers["X-RateLimit-Reset"] = str(rate_info["reset"])

            return result

        return wrapper

    return decorator


def get_rate_limiter_stats() -> dict:
    """
    Get statistics from all rate limiters.

    Returns:
        dict: Statistics from all tiers
    """
    return {
        "default": _default_limiter.get_stats(),
        "strict": _strict_limiter.get_stats(),
        "generous": _generous_limiter.get_stats(),
    }
