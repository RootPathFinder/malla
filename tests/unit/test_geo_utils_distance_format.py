"""Unit tests for C/F-linked distance formatting."""

import pytest

from malla.utils.geo_utils import format_distance_for_unit


@pytest.mark.unit
def test_format_distance_metric_for_celsius_preference():
    assert format_distance_for_unit(12.4, "C") == "12.4 km"
    assert format_distance_for_unit(0.35, "C") == "350 m"
    assert format_distance_for_unit(0.0, "C") == "0 m"


@pytest.mark.unit
def test_format_distance_imperial_for_fahrenheit_preference():
    assert format_distance_for_unit(12.4, "F") == "7.7 mi"
    assert format_distance_for_unit(0.35, "F") == "1148 ft"
    # Under 1 mile stays in feet; at/above shows miles
    assert format_distance_for_unit(1.0, "F") == "3281 ft"
    assert format_distance_for_unit(1.61, "F") == "1.0 mi"


@pytest.mark.unit
def test_format_distance_handles_invalid_values():
    assert format_distance_for_unit(None, "C") == "N/A"
    assert format_distance_for_unit(float("nan"), "F") == "N/A"
