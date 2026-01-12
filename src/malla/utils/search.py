"""
Advanced search utilities with fuzzy matching and filtering.

Provides enhanced search capabilities for finding nodes, packets, and other data
with fuzzy matching, ranking, and advanced filters.
"""

import logging
import re
from difflib import SequenceMatcher
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def fuzzy_match(query: str, text: str, threshold: float = 0.6) -> tuple[bool, float]:
    """
    Perform fuzzy string matching.

    Args:
        query: Search query
        text: Text to search in
        threshold: Minimum similarity ratio (0.0 to 1.0)

    Returns:
        tuple[bool, float]: (matched, similarity_score)
    """
    if not query or not text:
        return False, 0.0

    query = query.lower().strip()
    text = text.lower().strip()

    # Exact match
    if query == text:
        return True, 1.0

    # Exact substring match
    if query in text:
        return True, 0.9

    # Use SequenceMatcher for fuzzy matching
    ratio = SequenceMatcher(None, query, text).ratio()

    return ratio >= threshold, ratio


def calculate_relevance_score(
    query: str,
    item: dict[str, Any],
    search_fields: list[str],
    weights: Optional[dict[str, float]] = None,
) -> float:
    """
    Calculate relevance score for a search result.

    Args:
        query: Search query
        item: Item dictionary to score
        search_fields: Fields to search in
        weights: Optional field weights (higher = more important)

    Returns:
        float: Relevance score (0.0 to 1.0)
    """
    if not query:
        return 0.0

    if weights is None:
        weights = {field: 1.0 for field in search_fields}

    total_score = 0.0
    total_weight = 0.0

    for field in search_fields:
        if field not in item:
            continue

        value = str(item[field]) if item[field] is not None else ""
        weight = weights.get(field, 1.0)

        matched, score = fuzzy_match(query, value)
        if matched:
            total_score += score * weight
            total_weight += weight

    return total_score / total_weight if total_weight > 0 else 0.0


def search_nodes(
    nodes: list[dict[str, Any]],
    query: str,
    fuzzy: bool = True,
    threshold: float = 0.6,
) -> list[dict[str, Any]]:
    """
    Search nodes with fuzzy matching.

    Args:
        nodes: List of node dictionaries
        query: Search query
        fuzzy: Enable fuzzy matching
        threshold: Fuzzy match threshold

    Returns:
        list[dict[str, Any]]: Matching nodes sorted by relevance
    """
    if not query:
        return nodes

    results = []

    # Define search fields and their weights
    search_fields = ["node_name", "long_name", "short_name", "hardware_model", "role"]
    weights = {
        "node_name": 2.0,  # Node name is most important
        "long_name": 1.5,
        "short_name": 1.5,
        "hardware_model": 1.0,
        "role": 0.8,
    }

    for node in nodes:
        if fuzzy:
            score = calculate_relevance_score(query, node, search_fields, weights)
            if score >= threshold:
                node_copy = node.copy()
                node_copy["_relevance_score"] = score
                results.append(node_copy)
        else:
            # Exact match on any field
            query_lower = query.lower()
            for field in search_fields:
                if field in node and node[field]:
                    if query_lower in str(node[field]).lower():
                        node_copy = node.copy()
                        node_copy["_relevance_score"] = 1.0
                        results.append(node_copy)
                        break

    # Sort by relevance score descending
    results.sort(key=lambda x: x["_relevance_score"], reverse=True)

    return results


def advanced_filter(
    items: list[dict[str, Any]], filters: dict[str, Any]
) -> list[dict[str, Any]]:
    """
    Apply advanced filters to a list of items.

    Supports:
    - Exact matches: {"field": "value"}
    - Range filters: {"field_min": value, "field_max": value}
    - Multiple values: {"field_in": [value1, value2]}
    - Null checks: {"field_null": true/false}
    - Pattern matching: {"field_pattern": "regex_pattern"}

    Args:
        items: List of item dictionaries
        filters: Filter criteria

    Returns:
        list[dict[str, Any]]: Filtered items
    """
    if not filters:
        return items

    result = items

    for key, value in filters.items():
        # Skip None values
        if value is None:
            continue

        # Range filters (min/max)
        if key.endswith("_min"):
            field = key[:-4]
            result = [
                item
                for item in result
                if field in item
                and item[field] is not None
                and item[field] >= value
            ]
        elif key.endswith("_max"):
            field = key[:-4]
            result = [
                item
                for item in result
                if field in item
                and item[field] is not None
                and item[field] <= value
            ]

        # Multiple values (in)
        elif key.endswith("_in"):
            field = key[:-3]
            result = [
                item for item in result if field in item and item[field] in value
            ]

        # Null checks
        elif key.endswith("_null"):
            field = key[:-5]
            if value:  # Check if null
                result = [
                    item for item in result if field not in item or item[field] is None
                ]
            else:  # Check if not null
                result = [
                    item for item in result if field in item and item[field] is not None
                ]

        # Pattern matching
        elif key.endswith("_pattern"):
            field = key[:-8]
            try:
                pattern = re.compile(value, re.IGNORECASE)
                result = [
                    item
                    for item in result
                    if field in item
                    and item[field] is not None
                    and pattern.search(str(item[field]))
                ]
            except re.error:
                logger.warning(f"Invalid regex pattern: {value}")

        # Exact match
        else:
            result = [item for item in result if item.get(key) == value]

    return result


def search_packets(
    packets: list[dict[str, Any]],
    query: str,
    search_in: list[str] = None,
) -> list[dict[str, Any]]:
    """
    Search packets with text matching in specified fields.

    Args:
        packets: List of packet dictionaries
        query: Search query
        search_in: Fields to search in (default: text content and node names)

    Returns:
        list[dict[str, Any]]: Matching packets
    """
    if not query:
        return packets

    if search_in is None:
        search_in = [
            "decoded_text",
            "from_node_name",
            "to_node_name",
            "portnum_name",
            "gateway_id",
        ]

    query_lower = query.lower()
    results = []

    for packet in packets:
        score = 0.0
        matched = False

        for field in search_in:
            if field in packet and packet[field]:
                value = str(packet[field]).lower()
                if query_lower in value:
                    matched = True
                    # Boost score for exact matches
                    if query_lower == value:
                        score += 2.0
                    # Boost score for matches in node names
                    elif field.endswith("_name"):
                        score += 1.5
                    else:
                        score += 1.0

        if matched:
            packet_copy = packet.copy()
            packet_copy["_relevance_score"] = score
            results.append(packet_copy)

    # Sort by relevance score descending
    results.sort(key=lambda x: x["_relevance_score"], reverse=True)

    return results


def create_filter_builder() -> "FilterBuilder":
    """
    Create a new filter builder for constructing complex filters.

    Returns:
        FilterBuilder: New filter builder instance
    """
    return FilterBuilder()


class FilterBuilder:
    """
    Fluent interface for building complex filters.

    Example:
        builder = FilterBuilder()
        filters = (builder
            .equals("role", "ROUTER")
            .range("battery_level", min_value=20, max_value=80)
            .is_not_null("latitude")
            .build())
    """

    def __init__(self):
        """Initialize filter builder."""
        self._filters = {}

    def equals(self, field: str, value: Any) -> "FilterBuilder":
        """Add exact match filter."""
        self._filters[field] = value
        return self

    def range(
        self,
        field: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
    ) -> "FilterBuilder":
        """Add range filter."""
        if min_value is not None:
            self._filters[f"{field}_min"] = min_value
        if max_value is not None:
            self._filters[f"{field}_max"] = max_value
        return self

    def in_list(self, field: str, values: list[Any]) -> "FilterBuilder":
        """Add 'in list' filter."""
        self._filters[f"{field}_in"] = values
        return self

    def is_null(self, field: str) -> "FilterBuilder":
        """Add 'is null' filter."""
        self._filters[f"{field}_null"] = True
        return self

    def is_not_null(self, field: str) -> "FilterBuilder":
        """Add 'is not null' filter."""
        self._filters[f"{field}_null"] = False
        return self

    def matches_pattern(self, field: str, pattern: str) -> "FilterBuilder":
        """Add regex pattern filter."""
        self._filters[f"{field}_pattern"] = pattern
        return self

    def build(self) -> dict[str, Any]:
        """Build and return the filters dictionary."""
        return self._filters.copy()


def rank_search_results(
    results: list[dict[str, Any]], boost_recent: bool = True, recency_weight: float = 0.2
) -> list[dict[str, Any]]:
    """
    Re-rank search results considering recency and other factors.

    Args:
        results: Search results with _relevance_score
        boost_recent: Whether to boost recent items
        recency_weight: Weight for recency factor (0.0 to 1.0)

    Returns:
        list[dict[str, Any]]: Re-ranked results
    """
    if not results or not boost_recent:
        return results

    import time

    current_time = time.time()

    for item in results:
        base_score = item.get("_relevance_score", 0.0)

        # Calculate recency score (exponential decay)
        if "last_seen" in item and item["last_seen"]:
            try:
                last_seen = float(item["last_seen"])
                age_hours = (current_time - last_seen) / 3600
                recency_score = 1.0 / (1.0 + age_hours / 24.0)  # Decay over 24 hours
                item["_final_score"] = (
                    base_score * (1.0 - recency_weight) + recency_score * recency_weight
                )
            except (ValueError, TypeError):
                item["_final_score"] = base_score
        else:
            item["_final_score"] = base_score

    # Sort by final score
    results.sort(key=lambda x: x.get("_final_score", 0.0), reverse=True)

    return results
