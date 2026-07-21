"""Unit tests for hardware-aware battery / charger voltage models."""

import time

import pytest

from malla.battery_voltage_model import (
    DEFAULT_OCV_MV,
    resolve_battery_model,
)
from malla.power_analysis import (
    analyze_solar_degradation,
    classify_power_source,
)


class TestBatteryVoltageModel:
    @pytest.mark.unit
    def test_rak4631_near_full_at_90_pct(self):
        model = resolve_battery_model("RAK4631")
        assert model.key == "rak4631"
        assert model.near_full_pct == 90
        assert model.charge_full_voltage == pytest.approx(4.07)
        assert model.is_near_full(91, None) is True
        assert model.is_near_full(90, None) is True
        assert model.is_near_full(89, None) is False
        assert model.is_near_full(None, 4.07) is True
        assert model.is_near_full(None, 4.05) is False

    @pytest.mark.unit
    def test_tbeam_axp_near_full_at_95(self):
        model = resolve_battery_model("TBEAM")
        assert model.key == "axp_lipo"
        assert model.near_full_pct == 95
        assert model.is_near_full(95, None) is True
        assert model.is_near_full(94, None) is False
        assert model.is_near_full(None, 4.15) is True

    @pytest.mark.unit
    def test_heltec_profile(self):
        model = resolve_battery_model("HELTEC_V3")
        assert model.key == "heltec"
        assert model.near_full_pct == 93

    @pytest.mark.unit
    def test_default_profile(self):
        model = resolve_battery_model(None)
        assert model.key == "default"
        assert model.near_full_pct == 95

    @pytest.mark.unit
    def test_ocv_interpolation_matches_firmware_endpoints(self):
        model = resolve_battery_model(None)
        assert model.voltage_to_pct(DEFAULT_OCV_MV[0] / 1000.0) == 100.0
        assert model.voltage_to_pct(DEFAULT_OCV_MV[-1] / 1000.0) == 0.0
        # 4.07V sits near ~91% on the default OCV curve
        pct_407 = model.voltage_to_pct(4.07)
        assert pct_407 is not None
        assert 90.0 <= pct_407 <= 93.0


class TestHwAwareSolarAnalysis:
    @pytest.mark.unit
    def test_rak_solar_cycling_detected_at_91_pct(self):
        """RAK solar that tops out ~91% should still classify as solar."""
        model = resolve_battery_model("RAK4631")
        now = time.time()
        timestamps = [now - i * 3600 for i in range(12, 0, -1)]
        # Cycle 70% → 91% over the window
        batteries = [70, 72, 75, 78, 80, 83, 85, 87, 88, 90, 91, 91]
        voltages = [None] * len(batteries)
        ptype, reason, conf = classify_power_source(
            timestamps, voltages, batteries, model=model
        )
        assert ptype == "solar"
        assert "90%" in reason or "91" in reason
        assert conf >= 0.8

    @pytest.mark.unit
    def test_rak_near_full_resets_days_since_full(self):
        model = resolve_battery_model("RAK4631")
        now = time.time()
        timestamps = [now - 86400 * 2, now - 3600]
        voltages = [3.95, 4.07]
        batteries = [85, 91]
        info = analyze_solar_degradation(
            timestamps, voltages, batteries, model=model
        )
        assert info["days_since_full_charge"] == 0
        assert info["near_full_pct"] == 90
        assert info["voltage_model"] == "rak4631"

    @pytest.mark.unit
    def test_generic_still_requires_higher_pct(self):
        model = resolve_battery_model(None)
        now = time.time()
        timestamps = [now - 86400 * 2, now - 3600]
        voltages = [3.95, 4.07]
        batteries = [85, 91]
        info = analyze_solar_degradation(
            timestamps, voltages, batteries, model=model
        )
        # 91% / 4.07V is not "full" for generic 95%/4.10V model
        assert info["days_since_full_charge"] != 0
