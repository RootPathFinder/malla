"""
Caching utilities for improving API performance.

Provides simple in-memory caching with TTL support for API responses.
"""

import functools
import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Simple in-memory cache with TTL
_cache: dict[str, tuple[float, Any]] = {}
_cache_stats = {"hits": 0, "misses": 0}


def cache_response(ttl_seconds: int = 60):
    """
    Decorator to cache Flask route responses.

    Args:
        ttl_seconds: Time to live for cached responses in seconds (default: 60)

    Usage:
        @api_bp.route("/stats")
        @cache_response(ttl_seconds=30)
        def api_stats():
            return jsonify({"data": expensive_operation()})
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Import here to avoid circular dependency
            from flask import request

            # Create cache key from route and query parameters
            cache_key = f"{request.path}:{request.query_string.decode()}"

            # Check if we have a valid cached response
            now = time.time()
            if cache_key in _cache:
                cached_time, cached_response = _cache[cache_key]
                if now - cached_time < ttl_seconds:
                    _cache_stats["hits"] += 1
                    logger.debug(
                        f"Cache hit for {cache_key} (age: {now - cached_time:.1f}s)"
                    )
                    return cached_response
                else:
                    # Expired, remove from cache
                    del _cache[cache_key]

            # Cache miss - execute the function
            _cache_stats["misses"] += 1
            logger.debug(f"Cache miss for {cache_key}")

            response = func(*args, **kwargs)

            # Store in cache
            _cache[cache_key] = (now, response)

            return response

        return wrapper

    return decorator


def clear_cache():
    """Clear all cached responses."""
    global _cache
    _cache.clear()
    logger.info("Cache cleared")


def get_cache_stats() -> dict[str, Any]:
    """Get cache statistics."""
    total_requests = _cache_stats["hits"] + _cache_stats["misses"]
    hit_rate = (
        (_cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0
    )

    return {
        "cache_size": len(_cache),
        "hits": _cache_stats["hits"],
        "misses": _cache_stats["misses"],
        "hit_rate": f"{hit_rate:.1f}%",
    }


def cache_cleanup(max_age_seconds: int = 3600):
    """
    Remove expired entries from cache.

    Args:
        max_age_seconds: Maximum age for cache entries (default: 1 hour)
    """
    now = time.time()
    expired_keys = [
        key
        for key, (cached_time, _) in _cache.items()
        if now - cached_time > max_age_seconds
    ]

    for key in expired_keys:
        del _cache[key]

    if expired_keys:
        logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
