"""
Input validation utilities for API endpoints and user input.

This module provides comprehensive validation functions to ensure data
integrity, security, and better error messages for users.
"""

import re
from typing import Any, Optional


class ValidationError(ValueError):
    """Custom exception for validation errors with user-friendly messages."""

    def __init__(self, message: str, field: Optional[str] = None):
        """
        Initialize validation error.

        Args:
            message: Human-readable error message
            field: Optional field name that failed validation
        """
        self.field = field
        super().__init__(message)


def validate_node_id(node_id: Any, field_name: str = "node_id") -> int:
    """
    Validate and convert node ID to integer.

    Args:
        node_id: Node ID to validate (can be string or int)
        field_name: Name of the field for error messages

    Returns:
        int: Validated node ID

    Raises:
        ValidationError: If node_id is invalid
    """
    if node_id is None:
        raise ValidationError(f"{field_name} is required", field=field_name)

    try:
        node_id_int = int(node_id)
    except (ValueError, TypeError):
        raise ValidationError(
            f"{field_name} must be a valid integer, got: {node_id}",
            field=field_name,
        )

    # Meshtastic node IDs are typically 32-bit unsigned integers
    if node_id_int < 0 or node_id_int > 0xFFFFFFFF:
        raise ValidationError(
            f"{field_name} must be between 0 and {0xFFFFFFFF}, got: {node_id_int}",
            field=field_name,
        )

    return node_id_int


def validate_pagination(
    page: Any = 1, per_page: Any = 50, max_per_page: int = 1000
) -> tuple[int, int]:
    """
    Validate pagination parameters.

    Args:
        page: Page number (1-indexed)
        per_page: Items per page
        max_per_page: Maximum allowed items per page

    Returns:
        tuple[int, int]: Validated (page, per_page)

    Raises:
        ValidationError: If parameters are invalid
    """
    try:
        page_int = int(page) if page is not None else 1
    except (ValueError, TypeError):
        raise ValidationError(f"page must be a valid integer, got: {page}", field="page")

    try:
        per_page_int = int(per_page) if per_page is not None else 50
    except (ValueError, TypeError):
        raise ValidationError(
            f"per_page must be a valid integer, got: {per_page}", field="per_page"
        )

    if page_int < 1:
        raise ValidationError(
            f"page must be at least 1, got: {page_int}", field="page"
        )

    if per_page_int < 1:
        raise ValidationError(
            f"per_page must be at least 1, got: {per_page_int}", field="per_page"
        )

    if per_page_int > max_per_page:
        raise ValidationError(
            f"per_page cannot exceed {max_per_page}, got: {per_page_int}",
            field="per_page",
        )

    return page_int, per_page_int


def validate_time_range(
    start_time: Any = None, end_time: Any = None
) -> tuple[Optional[float], Optional[float]]:
    """
    Validate time range parameters (Unix timestamps).

    Args:
        start_time: Start timestamp (seconds since epoch)
        end_time: End timestamp (seconds since epoch)

    Returns:
        tuple[Optional[float], Optional[float]]: Validated (start_time, end_time)

    Raises:
        ValidationError: If timestamps are invalid
    """
    start_float = None
    end_float = None

    if start_time is not None:
        try:
            start_float = float(start_time)
        except (ValueError, TypeError):
            raise ValidationError(
                f"start_time must be a valid timestamp, got: {start_time}",
                field="start_time",
            )

        if start_float < 0:
            raise ValidationError(
                f"start_time must be non-negative, got: {start_float}",
                field="start_time",
            )

    if end_time is not None:
        try:
            end_float = float(end_time)
        except (ValueError, TypeError):
            raise ValidationError(
                f"end_time must be a valid timestamp, got: {end_time}",
                field="end_time",
            )

        if end_float < 0:
            raise ValidationError(
                f"end_time must be non-negative, got: {end_float}", field="end_time"
            )

    if start_float is not None and end_float is not None and start_float > end_float:
        raise ValidationError(
            f"start_time ({start_float}) must be before end_time ({end_float})",
            field="time_range",
        )

    return start_float, end_float


def validate_signal_value(
    value: Any, field_name: str, min_value: Optional[float] = None, max_value: Optional[float] = None
) -> Optional[float]:
    """
    Validate signal-related values (RSSI, SNR, etc.).

    Args:
        value: Signal value to validate
        field_name: Name of the field for error messages
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)

    Returns:
        Optional[float]: Validated signal value or None

    Raises:
        ValidationError: If value is invalid
    """
    if value is None or value == "":
        return None

    try:
        value_float = float(value)
    except (ValueError, TypeError):
        raise ValidationError(
            f"{field_name} must be a valid number, got: {value}", field=field_name
        )

    if min_value is not None and value_float < min_value:
        raise ValidationError(
            f"{field_name} must be at least {min_value}, got: {value_float}",
            field=field_name,
        )

    if max_value is not None and value_float > max_value:
        raise ValidationError(
            f"{field_name} cannot exceed {max_value}, got: {value_float}",
            field=field_name,
        )

    return value_float


def validate_gateway_id(gateway_id: Any) -> Optional[str]:
    """
    Validate gateway ID format.

    Args:
        gateway_id: Gateway ID to validate

    Returns:
        Optional[str]: Validated gateway ID or None

    Raises:
        ValidationError: If gateway_id is invalid
    """
    if gateway_id is None or gateway_id == "":
        return None

    if not isinstance(gateway_id, str):
        raise ValidationError(
            f"gateway_id must be a string, got: {type(gateway_id).__name__}",
            field="gateway_id",
        )

    # Gateway IDs should be hex strings (e.g., "!fa6c1234")
    # Allow alphanumeric and some special characters
    if not re.match(r"^[!a-zA-Z0-9_-]+$", gateway_id):
        raise ValidationError(
            f"gateway_id contains invalid characters: {gateway_id}",
            field="gateway_id",
        )

    if len(gateway_id) > 100:
        raise ValidationError(
            f"gateway_id is too long (max 100 characters): {gateway_id}",
            field="gateway_id",
        )

    return gateway_id


def validate_sort_params(
    sort_by: Any = None, sort_order: Any = None, allowed_fields: Optional[list[str]] = None
) -> tuple[Optional[str], str]:
    """
    Validate sorting parameters.

    Args:
        sort_by: Field to sort by
        sort_order: Sort order ('asc' or 'desc')
        allowed_fields: List of allowed field names for sorting

    Returns:
        tuple[Optional[str], str]: Validated (sort_by, sort_order)

    Raises:
        ValidationError: If parameters are invalid
    """
    # Validate sort_order
    if sort_order is None:
        sort_order = "desc"
    elif not isinstance(sort_order, str):
        raise ValidationError(
            f"sort_order must be a string, got: {type(sort_order).__name__}",
            field="sort_order",
        )
    
    sort_order = sort_order.lower()
    if sort_order not in ("asc", "desc"):
        raise ValidationError(
            f"sort_order must be 'asc' or 'desc', got: {sort_order}",
            field="sort_order",
        )

    # Validate sort_by
    if sort_by is None:
        return None, sort_order

    if not isinstance(sort_by, str):
        raise ValidationError(
            f"sort_by must be a string, got: {type(sort_by).__name__}",
            field="sort_by",
        )

    if allowed_fields is not None and sort_by not in allowed_fields:
        raise ValidationError(
            f"sort_by must be one of {allowed_fields}, got: {sort_by}",
            field="sort_by",
        )

    return sort_by, sort_order


def validate_hop_count(hop_count: Any) -> Optional[int]:
    """
    Validate hop count parameter.

    Args:
        hop_count: Hop count to validate

    Returns:
        Optional[int]: Validated hop count or None

    Raises:
        ValidationError: If hop_count is invalid
    """
    if hop_count is None or hop_count == "":
        return None

    try:
        hop_count_int = int(hop_count)
    except (ValueError, TypeError):
        raise ValidationError(
            f"hop_count must be a valid integer, got: {hop_count}",
            field="hop_count",
        )

    # Meshtastic supports up to 7 hops
    if hop_count_int < 0 or hop_count_int > 7:
        raise ValidationError(
            f"hop_count must be between 0 and 7, got: {hop_count_int}",
            field="hop_count",
        )

    return hop_count_int


def validate_limit(limit: Any, default: int = 100, max_limit: int = 10000) -> int:
    """
    Validate limit parameter for query results.

    Args:
        limit: Limit value to validate
        default: Default limit if None
        max_limit: Maximum allowed limit

    Returns:
        int: Validated limit

    Raises:
        ValidationError: If limit is invalid
    """
    if limit is None or limit == "":
        return default

    try:
        limit_int = int(limit)
    except (ValueError, TypeError):
        raise ValidationError(
            f"limit must be a valid integer, got: {limit}", field="limit"
        )

    if limit_int < 1:
        raise ValidationError(
            f"limit must be at least 1, got: {limit_int}", field="limit"
        )

    if limit_int > max_limit:
        raise ValidationError(
            f"limit cannot exceed {max_limit}, got: {limit_int}", field="limit"
        )

    return limit_int


def sanitize_search_query(query: Any, max_length: int = 200) -> Optional[str]:
    """
    Sanitize and validate search query string.

    Args:
        query: Search query to sanitize
        max_length: Maximum allowed query length

    Returns:
        Optional[str]: Sanitized query or None

    Raises:
        ValidationError: If query is invalid
    """
    if query is None or query == "":
        return None

    if not isinstance(query, str):
        raise ValidationError(
            f"search query must be a string, got: {type(query).__name__}",
            field="query",
        )

    # Strip whitespace
    query = query.strip()

    if len(query) > max_length:
        raise ValidationError(
            f"search query is too long (max {max_length} characters)",
            field="query",
        )

    # Remove potentially dangerous characters for SQL injection
    # (Note: We still use parameterized queries, but this is defense in depth)
    # Allow alphanumeric, spaces, and some common punctuation
    if not re.match(r"^[a-zA-Z0-9\s\-_.,!?@#()]+$", query):
        raise ValidationError(
            "search query contains invalid characters", field="query"
        )

    return query
