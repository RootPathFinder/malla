"""Guardrails so temperature unit preference stays consistent across the UI."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src" / "malla"


def test_base_loads_user_preferences_before_temperature_toggle():
    base = (SRC / "templates" / "base.html").read_text(encoding="utf-8")
    assert "user-preferences.js" in base
    assert "temperature-toggle.js" in base
    assert base.index("user-preferences.js") < base.index("temperature-toggle.js")


def test_profile_does_not_double_load_user_preferences():
    profile = (SRC / "templates" / "profile.html").read_text(encoding="utf-8")
    assert "user-preferences.js" not in profile


def test_node_detail_live_telemetry_uses_temperature_toggle_not_legacy_key():
    node_detail = (SRC / "templates" / "node_detail.html").read_text(encoding="utf-8")
    assert "localStorage.getItem('temperatureUnit')" not in node_detail
    assert "TemperatureToggle.formatTemperature" in node_detail
    assert "data-temperature-celsius" in node_detail


def test_temperature_toggle_prefers_user_preferences():
    toggle_js = (SRC / "static" / "js" / "temperature-toggle.js").read_text(
        encoding="utf-8"
    )
    assert "UserPreferences.getTemperatureUnit" in toggle_js
    assert "userPreferencesReady" in toggle_js


def test_user_preferences_migrates_obsolete_temperature_key():
    prefs_js = (SRC / "static" / "js" / "user-preferences.js").read_text(
        encoding="utf-8"
    )
    assert "temperatureUnit" in prefs_js
    assert "malla-temperature-unit" in prefs_js
    assert "syncCacheToLocalStorage" in prefs_js
    assert "fahrenheit" in prefs_js
