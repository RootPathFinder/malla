"""
Unit tests for config_compare utility.

Tests the deep_compare_config function and related utilities that match
the Meshtastic web client's comparison protocol.
"""

import pytest

from malla.utils.config_compare import (
    compare_dicts,
    compare_lists,
    configs_are_equal,
    deep_compare_config,
    get_config_diff_summary,
    is_empty_value,
    normalize_bytes_value,
    values_equal,
)

pytestmark = pytest.mark.unit


class TestIsEmptyValue:
    """Tests for is_empty_value function."""

    def test_none_is_empty(self):
        assert is_empty_value(None) is True

    def test_empty_string_is_empty(self):
        assert is_empty_value("") is True

    def test_non_empty_string_is_not_empty(self):
        assert is_empty_value("hello") is False

    def test_empty_list_is_empty(self):
        assert is_empty_value([]) is True

    def test_non_empty_list_is_not_empty(self):
        assert is_empty_value([1, 2, 3]) is False

    def test_empty_dict_is_empty(self):
        assert is_empty_value({}) is True

    def test_non_empty_dict_is_not_empty(self):
        assert is_empty_value({"key": "value"}) is False

    def test_empty_bytes_is_empty(self):
        assert is_empty_value(b"") is True

    def test_non_empty_bytes_is_not_empty(self):
        assert is_empty_value(b"hello") is False

    def test_zero_is_not_empty(self):
        # Zero is a valid value, not empty
        assert is_empty_value(0) is False

    def test_false_is_not_empty(self):
        # False is a valid value, not empty
        assert is_empty_value(False) is False


class TestNormalizeBytesValue:
    """Tests for normalize_bytes_value function."""

    def test_bytes_passthrough(self):
        result = normalize_bytes_value(b"hello")
        assert result == b"hello"

    def test_empty_bytes_returns_none(self):
        result = normalize_bytes_value(b"")
        assert result is None

    def test_base64_string(self):
        # "hello" in base64 is "aGVsbG8="
        result = normalize_bytes_value("aGVsbG8=")
        assert result == b"hello"

    def test_hex_string(self):
        # "hello" in hex is "68656c6c6f"
        result = normalize_bytes_value("68656c6c6f")
        assert result == b"hello"

    def test_list_of_ints(self):
        result = normalize_bytes_value([104, 101, 108, 108, 111])  # "hello"
        assert result == b"hello"

    def test_none_returns_none(self):
        result = normalize_bytes_value(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = normalize_bytes_value("")
        assert result is None


class TestValuesEqual:
    """Tests for values_equal function."""

    def test_equal_strings(self):
        assert values_equal("hello", "hello") is True

    def test_unequal_strings(self):
        assert values_equal("hello", "world") is False

    def test_equal_integers(self):
        assert values_equal(42, 42) is True

    def test_unequal_integers(self):
        assert values_equal(42, 43) is False

    def test_equal_booleans(self):
        assert values_equal(True, True) is True
        assert values_equal(False, False) is True

    def test_unequal_booleans(self):
        assert values_equal(True, False) is False

    def test_both_empty_values(self):
        assert values_equal(None, None) is True
        assert values_equal("", "") is True
        assert values_equal([], []) is True

    def test_one_empty_one_not(self):
        assert values_equal(None, "value") is False
        assert values_equal("value", None) is False

    def test_numeric_comparison(self):
        # Int vs float should compare equal if values match
        assert values_equal(42, 42.0) is True

    def test_bytes_field_base64_vs_bytes(self):
        # Compare base64 string to raw bytes
        assert values_equal("aGVsbG8=", b"hello", is_bytes_field=True) is True

    def test_bytes_field_hex_vs_bytes(self):
        # Compare hex string to raw bytes
        assert values_equal("68656c6c6f", b"hello", is_bytes_field=True) is True

    def test_psk_field_detection(self):
        # Field name "psk" should be treated as bytes field
        assert values_equal("aGVsbG8=", b"hello", field_name="psk") is True


class TestCompareLists:
    """Tests for compare_lists function."""

    def test_equal_string_lists(self):
        diff = compare_lists(["a", "b", "c"], ["a", "b", "c"])
        assert diff == []

    def test_equal_string_lists_different_order(self):
        # String lists use set comparison - order doesn't matter
        diff = compare_lists(["a", "b", "c"], ["c", "b", "a"], "test_field")
        assert diff == []

    def test_unequal_string_lists(self):
        diff = compare_lists(["a", "b"], ["a", "c"], "test_field")
        assert len(diff) == 1
        assert diff[0]["type"] == "mismatch"

    def test_empty_values_filtered(self):
        # Empty values should be filtered before comparison
        diff = compare_lists(["a", "", None, "b"], ["a", "b"], "test_field")
        assert diff == []

    def test_bytes_field_normalization(self):
        # PSK field should normalize values
        diff = compare_lists(
            ["aGVsbG8="],  # base64 "hello"
            [b"hello"],
            "psk",
        )
        # After normalization, these should be equal
        assert diff == []


class TestCompareDicts:
    """Tests for compare_dicts function."""

    def test_equal_dicts(self):
        diff = compare_dicts({"a": 1, "b": 2}, {"a": 1, "b": 2})
        assert diff == []

    def test_missing_field(self):
        diff = compare_dicts({"a": 1, "b": 2}, {"a": 1})
        assert len(diff) == 1
        assert diff[0]["field"] == "b"
        assert diff[0]["type"] == "missing"

    def test_mismatch_field(self):
        diff = compare_dicts({"a": 1}, {"a": 2})
        assert len(diff) == 1
        assert diff[0]["field"] == "a"
        assert diff[0]["type"] == "mismatch"

    def test_nested_dict(self):
        expected = {"outer": {"inner": 1}}
        actual = {"outer": {"inner": 2}}
        diff = compare_dicts(expected, actual)
        assert len(diff) == 1
        assert diff[0]["field"] == "outer.inner"

    def test_empty_expected_vs_non_empty_actual(self):
        # Empty expected vs non-empty actual should report a mismatch
        # This is correct behavior - we want to detect when the node has a value
        # that differs from what's expected (even if expected is empty)
        diff = compare_dicts({"a": ""}, {"a": "value"})
        assert len(diff) == 1
        assert diff[0]["type"] == "mismatch"


class TestDeepCompareConfig:
    """Tests for deep_compare_config function."""

    def test_identical_configs(self):
        config = {"device": {"role": 1, "serial_enabled": True}}
        diff = deep_compare_config(config, config)
        assert diff == []

    def test_wrapped_config_unwrapping(self):
        # Template at root, node wrapped
        expected = {"role": 1, "serial_enabled": True}
        actual = {"device": {"role": 1, "serial_enabled": True}}
        diff = deep_compare_config(expected, actual, config_type="device")
        assert diff == []

    def test_both_wrapped_configs(self):
        expected = {"device": {"role": 1}}
        actual = {"device": {"role": 1}}
        diff = deep_compare_config(expected, actual)
        assert diff == []

    def test_difference_in_value(self):
        expected = {"device": {"role": 1}}
        actual = {"device": {"role": 2}}
        diff = deep_compare_config(expected, actual)
        assert len(diff) == 1
        assert diff[0]["expected"] == 1
        assert diff[0]["actual"] == 2

    def test_empty_actual_config(self):
        expected = {"role": 1, "name": "test"}
        diff = deep_compare_config(expected, {})
        assert len(diff) == 2

    def test_empty_expected_config(self):
        diff = deep_compare_config({}, {"role": 1})
        assert diff == []

    def test_channel_config_comparison(self):
        expected = {
            "channel": {
                "role": 1,
                "settings": {
                    "name": "LongFast",
                    "psk": "aGVsbG8=",  # base64 "hello"
                },
            }
        }
        actual = {
            "channel": {
                "role": 1,
                "settings": {
                    "name": "LongFast",
                    "psk": b"hello",  # raw bytes
                },
            }
        }
        diff = deep_compare_config(expected, actual)
        # PSK values should be equal after normalization
        assert len(diff) == 0

    def test_bluetooth_config(self):
        """Test bluetooth config comparison - a common problem area."""
        expected = {
            "bluetooth": {
                "enabled": True,
                "mode": 1,
                "fixed_pin": 123456,
            }
        }
        actual = {
            "bluetooth": {
                "enabled": True,
                "mode": 1,
                "fixed_pin": 123456,
            }
        }
        diff = deep_compare_config(expected, actual)
        assert diff == []

    def test_security_config_with_admin_key(self):
        """Test security config with admin_key list."""
        expected = {
            "security": {
                "admin_key": ["key1", "key2"],
                "is_managed": True,
            }
        }
        actual = {
            "security": {
                "admin_key": ["key2", "key1"],  # Different order
                "is_managed": True,
            }
        }
        diff = deep_compare_config(expected, actual)
        # Order shouldn't matter for key lists
        assert diff == []

    def test_allow_undefined_true(self):
        """Test that extra fields in actual are ignored by default."""
        expected = {"a": 1}
        actual = {"a": 1, "b": 2}
        diff = deep_compare_config(expected, actual, allow_undefined=True)
        assert diff == []

    def test_allow_undefined_false(self):
        """Test that extra fields in actual are reported when allow_undefined=False."""
        expected = {"a": 1}
        actual = {"a": 1, "b": 2}
        diff = deep_compare_config(expected, actual, allow_undefined=False)
        assert len(diff) == 1
        assert diff[0]["type"] == "extra"


class TestConfigsAreEqual:
    """Tests for configs_are_equal convenience function."""

    def test_equal_configs(self):
        assert configs_are_equal({"a": 1}, {"a": 1}) is True

    def test_unequal_configs(self):
        assert configs_are_equal({"a": 1}, {"a": 2}) is False


class TestGetConfigDiffSummary:
    """Tests for get_config_diff_summary function."""

    def test_empty_differences(self):
        summary = get_config_diff_summary([])
        assert "identical" in summary.lower()

    def test_single_difference(self):
        diff = [{"field": "role", "expected": 1, "actual": 2, "type": "mismatch"}]
        summary = get_config_diff_summary(diff)
        assert "1 difference" in summary
        assert "role" in summary

    def test_missing_field_summary(self):
        diff = [
            {"field": "name", "expected": "test", "actual": None, "type": "missing"}
        ]
        summary = get_config_diff_summary(diff)
        assert "missing" in summary.lower()

    def test_truncation_with_many_differences(self):
        diff = [
            {"field": f"field_{i}", "expected": i, "actual": i + 1, "type": "mismatch"}
            for i in range(15)
        ]
        summary = get_config_diff_summary(diff)
        assert "and 5 more" in summary
