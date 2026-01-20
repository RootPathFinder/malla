"""
Configuration comparison utilities following Meshtastic web client protocols.

This module provides deep comparison utilities for Meshtastic configuration
objects, matching the behavior of the official web client's deepCompareConfig.

Key features:
- Recursive deep comparison of nested config objects
- Proper handling of bytes (base64/hex) vs protobuf field comparison
- Support for allow_undefined to skip missing fields (one-way comparison)
- Difference tracking with field paths
"""

import base64
import logging
from typing import Any

logger = logging.getLogger(__name__)


def is_empty_value(value: Any) -> bool:
    """
    Check if a value is considered "empty" in Meshtastic config terms.

    Empty values include:
    - None
    - Empty strings
    - Empty lists/tuples
    - Empty dicts
    - Zero values (int/float) when representing unset
    - Empty bytes
    """
    if value is None:
        return True
    if isinstance(value, str) and value == "":
        return True
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return True
    if isinstance(value, dict) and len(value) == 0:
        return True
    if isinstance(value, bytes) and len(value) == 0:
        return True
    return False


def normalize_bytes_value(value: Any) -> bytes | None:
    """
    Normalize a bytes-like value to bytes for comparison.

    Handles:
    - Raw bytes
    - Base64 encoded strings
    - Hex encoded strings
    - Lists of integers (byte arrays)

    Returns:
        bytes object or None if empty/invalid
    """
    if value is None:
        return None

    if isinstance(value, bytes):
        return value if value else None

    if isinstance(value, (list, tuple)):
        # List of integers (byte values)
        try:
            return bytes(value) if value else None
        except (TypeError, ValueError):
            return None

    if isinstance(value, str):
        if not value:
            return None

        # Try base64 first (Meshtastic standard)
        try:
            decoded = base64.b64decode(value)
            if decoded:
                return decoded
        except Exception:
            pass

        # Try hex decoding
        try:
            decoded = bytes.fromhex(value)
            if decoded:
                return decoded
        except Exception:
            pass

        # Return as UTF-8 bytes if nothing else works
        return value.encode("utf-8") if value else None

    return None


def values_equal(
    expected: Any,
    actual: Any,
    field_name: str = "",
    is_bytes_field: bool = False,
) -> bool:
    """
    Compare two values for equality with type coercion.

    Args:
        expected: The expected/template value
        actual: The actual/node value
        field_name: Field name for debugging
        is_bytes_field: Whether this is known to be a bytes field (PSK, keys, etc.)

    Returns:
        True if values are considered equal
    """
    # Handle None/empty equivalence
    if is_empty_value(expected) and is_empty_value(actual):
        return True

    # One is empty, other is not
    if is_empty_value(expected) != is_empty_value(actual):
        return False

    # Same type - direct comparison
    if type(expected) is type(actual):
        if isinstance(expected, dict):
            return compare_dicts(expected, actual) == []
        if isinstance(expected, (list, tuple)):
            return compare_lists(list(expected), list(actual), field_name) == []
        return expected == actual

    # Bytes field comparison with normalization
    if is_bytes_field or field_name.lower() in (
        "psk",
        "admin_key",
        "private_key",
        "public_key",
    ):
        expected_bytes = normalize_bytes_value(expected)
        actual_bytes = normalize_bytes_value(actual)
        if expected_bytes is None and actual_bytes is None:
            return True
        if expected_bytes is None or actual_bytes is None:
            return False
        return expected_bytes == actual_bytes

    # Numeric comparison (int vs float)
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        # For integers, compare directly
        if isinstance(expected, int) and isinstance(actual, int):
            return expected == actual
        # For floats, use approximate comparison
        return abs(float(expected) - float(actual)) < 1e-9

    # String to number conversion
    if isinstance(expected, str) and isinstance(actual, (int, float)):
        try:
            return float(expected) == float(actual)
        except ValueError:
            return False

    if isinstance(expected, (int, float)) and isinstance(actual, str):
        try:
            return float(expected) == float(actual)
        except ValueError:
            return False

    # Boolean comparisons
    if isinstance(expected, bool) or isinstance(actual, bool):
        # Convert to bool for comparison
        return bool(expected) == bool(actual)

    # String comparison
    return str(expected) == str(actual)


def compare_lists(
    expected: list,
    actual: list,
    field_name: str = "",
) -> list[dict]:
    """
    Compare two lists and return differences.

    For key lists (admin_key, etc.), uses set comparison.
    For ordered lists, uses element-by-element comparison.
    """
    differences = []

    # Filter out empty values
    expected_filtered = [e for e in expected if not is_empty_value(e)]
    actual_filtered = [a for a in actual if not is_empty_value(a)]

    # For bytes-like fields, normalize before comparison
    if field_name.lower() in ("admin_key", "private_key", "public_key", "psk"):
        expected_normalized = [normalize_bytes_value(e) for e in expected_filtered]
        expected_normalized = [e for e in expected_normalized if e is not None]
        actual_normalized = [normalize_bytes_value(a) for a in actual_filtered]
        actual_normalized = [a for a in actual_normalized if a is not None]

        if set(expected_normalized) != set(actual_normalized):
            differences.append(
                {
                    "field": field_name,
                    "expected": expected_filtered,
                    "actual": actual_filtered,
                    "type": "mismatch",
                }
            )
        return differences

    # For string lists, use set comparison (order-independent)
    if all(isinstance(e, str) for e in expected_filtered) and all(
        isinstance(a, str) for a in actual_filtered
    ):
        if set(expected_filtered) != set(actual_filtered):
            differences.append(
                {
                    "field": field_name,
                    "expected": expected_filtered,
                    "actual": actual_filtered,
                    "type": "mismatch",
                }
            )
        return differences

    # For other lists, compare element by element
    if expected_filtered != actual_filtered:
        differences.append(
            {
                "field": field_name,
                "expected": expected_filtered,
                "actual": actual_filtered,
                "type": "mismatch",
            }
        )

    return differences


def compare_dicts(
    expected: dict,
    actual: dict,
    path: str = "",
) -> list[dict]:
    """
    Compare two dictionaries recursively.

    Returns list of differences found.
    """
    differences = []

    for key, expected_value in expected.items():
        full_path = f"{path}.{key}" if path else key

        if key not in actual:
            # Field missing in actual - only report if expected is non-empty
            if not is_empty_value(expected_value):
                differences.append(
                    {
                        "field": full_path,
                        "expected": expected_value,
                        "actual": None,
                        "type": "missing",
                    }
                )
            continue

        actual_value = actual[key]

        # Recursive dict comparison
        if isinstance(expected_value, dict):
            if isinstance(actual_value, dict):
                differences.extend(
                    compare_dicts(expected_value, actual_value, full_path)
                )
            elif not is_empty_value(expected_value):
                differences.append(
                    {
                        "field": full_path,
                        "expected": expected_value,
                        "actual": actual_value,
                        "type": "type_mismatch",
                    }
                )
            continue

        # List comparison
        if isinstance(expected_value, (list, tuple)):
            if isinstance(actual_value, (list, tuple)):
                list_diffs = compare_lists(
                    list(expected_value), list(actual_value), full_path
                )
                differences.extend(list_diffs)
            elif not is_empty_value(expected_value):
                differences.append(
                    {
                        "field": full_path,
                        "expected": expected_value,
                        "actual": actual_value,
                        "type": "type_mismatch",
                    }
                )
            continue

        # Check if this is a bytes field
        is_bytes_field = key.lower() in (
            "psk",
            "admin_key",
            "private_key",
            "public_key",
        )

        # Value comparison
        if not values_equal(expected_value, actual_value, key, is_bytes_field):
            differences.append(
                {
                    "field": full_path,
                    "expected": expected_value,
                    "actual": actual_value,
                    "type": "mismatch",
                }
            )

    return differences


def deep_compare_config(
    expected_config: dict,
    actual_config: dict,
    config_type: str | None = None,
    allow_undefined: bool = True,
) -> list[dict]:
    """
    Deep compare configuration objects following Meshtastic web client protocol.

    This function provides recursive comparison of configuration dictionaries,
    matching the behavior of the official web client's deepCompareConfig utility.

    Args:
        expected_config: The template/expected configuration
        actual_config: The actual node configuration
        config_type: Optional config type (device, security, lora, etc.) for unwrapping
        allow_undefined: If True, fields missing from expected are ignored (one-way comparison)
                        If False, fields in actual but not in expected are reported

    Returns:
        List of difference objects with fields:
        - field: Dot-separated path to the different field
        - expected: The expected value (from template)
        - actual: The actual value (from node)
        - type: "missing", "mismatch", or "type_mismatch"
    """
    if not expected_config:
        return []

    if not actual_config:
        logger.warning(
            f"deep_compare_config: actual_config is empty/None. "
            f"config_type={config_type}, expected keys={list(expected_config.keys())}"
        )
        # Return all expected fields as missing
        differences = []
        for key, value in expected_config.items():
            if not is_empty_value(value):
                differences.append(
                    {
                        "field": key,
                        "expected": value,
                        "actual": None,
                        "type": "missing",
                    }
                )
        return differences

    # Unwrap expected config if wrapped in type key
    unwrapped_expected = expected_config
    if len(expected_config) == 1:
        key = list(expected_config.keys())[0]
        if key in (
            "device",
            "lora",
            "position",
            "power",
            "network",
            "display",
            "bluetooth",
            "security",
            "channel",
        ):
            unwrapped_expected = expected_config[key]
            logger.debug(f"deep_compare_config: unwrapped expected via key={key}")

    # Unwrap actual config if wrapped in type key
    unwrapped_actual = actual_config
    if len(actual_config) == 1:
        key = list(actual_config.keys())[0]
        if key in (
            "device",
            "lora",
            "position",
            "power",
            "network",
            "display",
            "bluetooth",
            "security",
            "channel",
        ):
            unwrapped_actual = actual_config[key]
            logger.debug(f"deep_compare_config: unwrapped actual via key={key}")
    elif config_type and config_type in actual_config:
        unwrapped_actual = actual_config[config_type]
        logger.debug(
            f"deep_compare_config: unwrapped actual via config_type={config_type}"
        )

    # Perform the comparison
    differences = compare_dicts(unwrapped_expected, unwrapped_actual)

    # If not allow_undefined, also check for extra fields in actual
    if not allow_undefined and isinstance(unwrapped_actual, dict):
        for key in unwrapped_actual:
            if key not in unwrapped_expected:
                value = unwrapped_actual[key]
                if not is_empty_value(value):
                    differences.append(
                        {
                            "field": key,
                            "expected": None,
                            "actual": value,
                            "type": "extra",
                        }
                    )

    return differences


def configs_are_equal(
    expected_config: dict,
    actual_config: dict,
    config_type: str | None = None,
) -> bool:
    """
    Check if two configurations are equal.

    This is a convenience function that returns True if there are no differences.

    Args:
        expected_config: The template/expected configuration
        actual_config: The actual node configuration
        config_type: Optional config type for unwrapping

    Returns:
        True if configurations are equal, False otherwise
    """
    differences = deep_compare_config(expected_config, actual_config, config_type)
    return len(differences) == 0


def get_config_diff_summary(differences: list[dict]) -> str:
    """
    Generate a human-readable summary of configuration differences.

    Args:
        differences: List of difference objects from deep_compare_config

    Returns:
        Human-readable summary string
    """
    if not differences:
        return "Configurations are identical"

    lines = [f"{len(differences)} difference(s) found:"]

    for diff in differences[:10]:  # Limit to first 10
        field = diff["field"]
        diff_type = diff["type"]
        expected = diff.get("expected")
        actual = diff.get("actual")

        if diff_type == "missing":
            lines.append(f"  - {field}: missing (expected: {expected})")
        elif diff_type == "extra":
            lines.append(f"  - {field}: extra field (value: {actual})")
        elif diff_type == "type_mismatch":
            lines.append(
                f"  - {field}: type mismatch (expected: {type(expected).__name__}, "
                f"actual: {type(actual).__name__})"
            )
        else:
            lines.append(f"  - {field}: {expected} != {actual}")

    if len(differences) > 10:
        lines.append(f"  ... and {len(differences) - 10} more")

    return "\n".join(lines)
