"""Unit tests for hardware-aware battery / charger voltage models."""

import sqlite3
import time

import pytest

from malla.battery_voltage_model import (
    DEFAULT_OCV_MV,
    hw_model_coverage_stats,
    resolve_battery_model,
)
from malla.power_analysis import (
    analyze_node_power,
    analyze_solar_degradation,
    classify_power_source,
    set_battery_voltage_override,
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
    def test_all_rak_boards_use_rak_profile(self):
        for hw in ("RAK11200", "RAK11310", "RAK2560", "RAK3172", "RAK3312", "RAK4631"):
            assert resolve_battery_model(hw).key == "rak4631", hw

    @pytest.mark.unit
    def test_heltec_mesh_solar_uses_soft_cutoff(self):
        model = resolve_battery_model("HELTEC_MESH_SOLAR")
        assert model.key == "solar_board"
        assert model.near_full_pct == 90
        assert model.is_near_full(90, None) is True

    @pytest.mark.unit
    def test_seeed_solar_node_uses_soft_cutoff(self):
        model = resolve_battery_model("SEEED_SOLAR_NODE")
        assert model.key == "solar_board"

    @pytest.mark.unit
    def test_tbeam_axp_near_full_at_95(self):
        model = resolve_battery_model("TBEAM")
        assert model.key == "axp_lipo"
        assert model.near_full_pct == 95
        assert model.is_near_full(95, None) is True
        assert model.is_near_full(94, None) is False
        assert model.is_near_full(None, 4.15) is True

    @pytest.mark.unit
    def test_m5stack_and_nano_use_axp(self):
        assert resolve_battery_model("M5STACK_CORE2").key == "axp_lipo"
        assert resolve_battery_model("NANO_G2_ULTRA").key == "axp_lipo"

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
    def test_node_overrides_applied(self):
        model = resolve_battery_model(
            "RAK4631", charge_full_voltage=4.12, near_full_pct=92
        )
        assert model.key == "rak4631"
        assert model.has_node_override is True
        assert model.charge_full_voltage == pytest.approx(4.12)
        assert model.near_full_pct == 92
        assert model.is_near_full(92, None) is True
        assert model.is_near_full(91, None) is False
        assert "(custom)" in model.label

    @pytest.mark.unit
    def test_ocv_interpolation_matches_firmware_endpoints(self):
        model = resolve_battery_model(None)
        assert model.voltage_to_pct(DEFAULT_OCV_MV[0] / 1000.0) == 100.0
        assert model.voltage_to_pct(DEFAULT_OCV_MV[-1] / 1000.0) == 0.0
        # 4.07V sits near ~91% on the default OCV curve
        pct_407 = model.voltage_to_pct(4.07)
        assert pct_407 is not None
        assert 90.0 <= pct_407 <= 93.0

    @pytest.mark.unit
    def test_hw_coverage_majority_matched(self):
        stats = hw_model_coverage_stats()
        assert stats["total"] >= 100
        # Expanded matchers should cover well over half of known HW enums
        assert stats["non_default"] >= 70
        assert stats["by_key"].get("rak4631", 0) >= 8
        assert stats["by_key"].get("solar_board", 0) >= 2


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


class TestBatteryVoltageOverridePersistence:
    @pytest.mark.unit
    def test_set_and_clear_overrides(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE node_info (
                node_id INTEGER PRIMARY KEY,
                hex_id TEXT,
                long_name TEXT,
                hw_model TEXT,
                power_type TEXT DEFAULT 'unknown',
                power_type_locked INTEGER DEFAULT 0,
                power_type_reason TEXT,
                power_analysis_timestamp REAL,
                battery_health_score INTEGER,
                battery_charge_full_voltage REAL,
                battery_near_full_pct INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE telemetry_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                node_id INTEGER,
                battery_level INTEGER,
                voltage REAL
            )
            """
        )
        node_id = 0xABCDEF01
        cur.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, hw_model, power_type)
            VALUES (?, ?, ?, ?, ?)
            """,
            (node_id, "!abcdef01", "RAK Solar", "RAK4631", "solar"),
        )
        now = time.time()
        for i, (bat, volt) in enumerate(
            [(70, 3.90), (80, 4.00), (91, 4.07), (91, 4.07)]
        ):
            cur.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, battery_level, voltage)
                VALUES (?, ?, ?, ?)
                """,
                (now - (3 - i) * 3600, node_id, bat, volt),
            )
        conn.commit()

        status = analyze_node_power(node_id, conn)
        assert status["voltage_model"]["key"] == "rak4631"
        assert status["voltage_model"]["near_full_pct"] == 90
        assert status["voltage_model"]["has_node_override"] is False

        status = set_battery_voltage_override(
            node_id,
            conn,
            charge_full_voltage=4.20,
            near_full_pct=97,
            update_voltage=True,
            update_pct=True,
        )
        assert status["voltage_model"]["has_node_override"] is True
        assert status["voltage_model"]["charge_full_voltage"] == pytest.approx(4.20)
        assert status["voltage_model"]["near_full_pct"] == 97

        row = cur.execute(
            """
            SELECT battery_charge_full_voltage, battery_near_full_pct
            FROM node_info WHERE node_id = ?
            """,
            (node_id,),
        ).fetchone()
        assert row["battery_charge_full_voltage"] == pytest.approx(4.20)
        assert row["battery_near_full_pct"] == 97

        status = set_battery_voltage_override(node_id, conn, clear=True)
        assert status["voltage_model"]["has_node_override"] is False
        assert status["voltage_model"]["near_full_pct"] == 90
        row = cur.execute(
            """
            SELECT battery_charge_full_voltage, battery_near_full_pct
            FROM node_info WHERE node_id = ?
            """,
            (node_id,),
        ).fetchone()
        assert row["battery_charge_full_voltage"] is None
        assert row["battery_near_full_pct"] is None
        conn.close()
