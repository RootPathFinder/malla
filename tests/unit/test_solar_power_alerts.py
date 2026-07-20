"""Unit tests for unified solar power condition alerts."""

from unittest.mock import MagicMock, patch

import pytest

from malla.services.alert_service import AlertService, AlertType


@pytest.mark.unit
def test_check_solar_power_condition_creates_and_resolves_alerts():
    """At-risk nodes raise alerts; healthy nodes resolve them."""
    conditions = {
        "at_risk": [
            {
                "node_id": 101,
                "name": "Ridge Solar",
                "issues": ["No near-full charge in 6 day(s)"],
                "outlook": "May not recover without intervention",
                "health_score": 35,
                "hours_to_critical": 18,
                "solar": {"days_since_full_charge": 6},
            }
        ],
        "watching": [
            {
                "node_id": 102,
                "name": "Valley Solar",
                "issues": ["Weak/no daytime charge gain for 2 consecutive day(s)"],
                "outlook": "Watch for another weak day",
                "health_score": 60,
                "hours_to_critical": None,
                "solar": {},
            }
        ],
        "healthy": [
            {
                "node_id": 103,
                "name": "Peak Solar",
                "issues": [],
                "outlook": "Healthy",
                "health_score": 90,
                "hours_to_critical": None,
                "solar": {},
            }
        ],
        "unknown": [],
        "counts": {"at_risk": 1, "watching": 1, "healthy": 1, "unknown": 0},
        "total": 3,
    }

    with (
        patch(
            "malla.power_analysis.get_solar_power_conditions",
            return_value=conditions,
        ),
        patch.object(AlertService, "add_alert") as mock_add,
        patch.object(AlertService, "resolve_alert", return_value=True) as mock_resolve,
        patch(
            "malla.services.alert_service.get_db_connection",
            return_value=MagicMock(),
        ),
    ):
        result = AlertService._check_solar_power_condition()

    assert result["alerts"] == 2
    assert result["resolved"] >= 1

    added_types = [call.args[0].alert_type for call in mock_add.call_args_list]
    assert AlertType.SOLAR_POWER_AT_RISK in added_types
    assert AlertType.SOLAR_POWER_WATCHING in added_types

    # Healthy node should trigger resolve for both alert types
    resolve_calls = mock_resolve.call_args_list
    resolved_for_103 = [
        c for c in resolve_calls if c.args[1] == 103
    ]
    assert len(resolved_for_103) >= 2
