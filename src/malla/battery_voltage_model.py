"""
Hardware-aware LiPo battery / charger voltage models.

Meshtastic reports battery % from an OCV lookup (see firmware ``power.h`` /
``Power.cpp``). Many solar boards stop charging *below* a true 4.2 V LiPo
full charge, so reported % never reaches 95–100%. Solar “near-full” detection
must use the charger’s termination voltage (and the matching %) per HW type.

Default OCV curve (mV, 100% → 0%) matches Meshtastic firmware:
``4190, 4050, 3990, 3890, 3800, 3720, 3630, 3530, 3420, 3300, 3100``
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

# Meshtastic firmware default OCV_ARRAY (single-cell mV, full → empty)
DEFAULT_OCV_MV: tuple[int, ...] = (
    4190,
    4050,
    3990,
    3890,
    3800,
    3720,
    3630,
    3530,
    3420,
    3300,
    3100,
)


@dataclass(frozen=True)
class BatteryVoltageModel:
    """Voltage / charge thresholds for one hardware family."""

    key: str
    label: str
    ocv_mv: tuple[int, ...] = DEFAULT_OCV_MV
    # Charger terminates / float band (volts). Solar “full” for this board.
    charge_full_voltage: float = 4.10
    # Optional: charger typically resumes below this (volts).
    charge_resume_voltage: float | None = 4.00
    critical_voltage: float = 3.20
    warning_voltage: float = 3.40
    # Explicit near-full % override; if None, derived from charge_full_voltage.
    near_full_pct_override: int | None = None
    # True when per-node Admin Power values replaced the HW defaults.
    has_node_override: bool = False

    @property
    def near_full_pct(self) -> int:
        if self.near_full_pct_override is not None:
            return int(self.near_full_pct_override)
        pct = self.voltage_to_pct(self.charge_full_voltage)
        if pct is None:
            return 95
        # Slight slack so float/termination noise still counts as full.
        return max(85, min(98, int(pct) - 1))

    @property
    def near_full_voltage(self) -> float:
        return float(self.charge_full_voltage)

    def voltage_to_pct(self, voltage: float | None) -> float | None:
        """Map cell voltage (V) → SOC % using the Meshtastic OCV interpolation."""
        if voltage is None:
            return None
        try:
            mv = float(voltage) * 1000.0
        except (TypeError, ValueError):
            return None
        ocv = self.ocv_mv
        n = len(ocv)
        if n < 2:
            return None
        if mv >= ocv[0]:
            return 100.0
        if mv <= ocv[-1]:
            return 0.0
        for i in range(1, n):
            if ocv[i] <= mv:
                # Firmware Power.cpp interpolation
                frac = (mv - ocv[i]) / (ocv[i - 1] - ocv[i])
                return (100.0 / (n - 1.0)) * ((n - 1.0 - i) + frac)
        return 0.0

    def is_near_full(
        self,
        battery_level: int | float | None = None,
        voltage: float | None = None,
    ) -> bool:
        """True when level or voltage indicates a full charge for this HW."""
        if battery_level is not None:
            try:
                level = int(battery_level)
            except (TypeError, ValueError):
                level = None
            else:
                if 0 <= level <= 100 and level >= self.near_full_pct:
                    return True
        if voltage is not None:
            try:
                v = float(voltage)
            except (TypeError, ValueError):
                return False
            if v >= self.near_full_voltage:
                return True
        return False

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "charge_full_voltage": self.charge_full_voltage,
            "charge_resume_voltage": self.charge_resume_voltage,
            "near_full_pct": self.near_full_pct,
            "near_full_voltage": self.near_full_voltage,
            "critical_voltage": self.critical_voltage,
            "warning_voltage": self.warning_voltage,
            "has_node_override": self.has_node_override,
            "ocv_mv": list(self.ocv_mv),
        }

    def with_overrides(
        self,
        *,
        charge_full_voltage: float | None = None,
        near_full_pct: int | None = None,
    ) -> BatteryVoltageModel:
        """Return a copy with optional per-node Admin Power overrides applied."""
        updates: dict[str, Any] = {}
        if charge_full_voltage is not None:
            updates["charge_full_voltage"] = float(charge_full_voltage)
        if near_full_pct is not None:
            updates["near_full_pct_override"] = int(near_full_pct)
        if not updates:
            return self
        updates["has_node_override"] = True
        # Keep key/label as HW family but annotate that overrides are active.
        label = self.label
        if not self.has_node_override:
            label = f"{self.label} (custom)"
            updates["label"] = label
        return replace(self, **updates)


# ---------------------------------------------------------------------------
# Hardware profiles
# ---------------------------------------------------------------------------
# RAK4631 + common solar baseboards (RAK19007 / CN3165-class etc.) often
# terminate around 4.07–4.10 V rather than a true 4.2 V LiPo top-off, so the
# firmware OCV table reports ~90–91% when “full”.
_RAK4631 = BatteryVoltageModel(
    key="rak4631",
    label="RAK / WisBlock",
    charge_full_voltage=4.07,
    charge_resume_voltage=3.95,
    near_full_pct_override=90,
)

# Dedicated solar products that commonly float below a true 4.2 V top-off.
_SOLAR_BOARD = BatteryVoltageModel(
    key="solar_board",
    label="Solar board (soft cutoff)",
    charge_full_voltage=4.07,
    charge_resume_voltage=3.95,
    near_full_pct_override=90,
)

# AXP192 / AXP2101 PMUs (T-Beam, many LilyGO / M5 boards) target 4.2 V.
_AXP_LIPO = BatteryVoltageModel(
    key="axp_lipo",
    label="AXP LiPo (4.2V charge)",
    charge_full_voltage=4.15,
    charge_resume_voltage=4.05,
    near_full_pct_override=95,
)

# Heltec / T-LoRa style ADC boards: typically charge to ~4.2 V on USB;
# solar add-ons vary — use a slightly softer full than AXP.
_HELTEC = BatteryVoltageModel(
    key="heltec",
    label="Heltec / T-LoRa",
    charge_full_voltage=4.12,
    charge_resume_voltage=4.00,
    near_full_pct_override=93,
)

# Nordic T-Echo / similar nRF boards with onboard charger ~4.2 V.
_TECHO = BatteryVoltageModel(
    key="t_echo",
    label="T-Echo / nRF charger",
    charge_full_voltage=4.15,
    charge_resume_voltage=4.00,
    near_full_pct_override=95,
)

# SenseCAP / Station-G / Tracker boards with PMU ≈ 4.2 V.
_SENSECAP = BatteryVoltageModel(
    key="sensecap",
    label="SenseCAP / Station",
    charge_full_voltage=4.15,
    charge_resume_voltage=4.05,
    near_full_pct_override=95,
)

_DEFAULT = BatteryVoltageModel(
    key="default",
    label="Generic LiPo",
    charge_full_voltage=4.10,
    charge_resume_voltage=4.00,
    near_full_pct_override=95,
)

# Substring match order matters — more specific keys first.
_HW_MATCHERS: tuple[tuple[tuple[str, ...], BatteryVoltageModel], ...] = (
    # Dedicated solar SKUs before broader HELTEC / SEEED families.
    (("HELTEC_MESH_SOLAR", "SEEED_SOLAR", "MESH_SOLAR"), _SOLAR_BOARD),
    # All RAK* / WisMesh / WisBlock share soft solar-charger behaviour.
    (("RAK", "WISMESH", "WISBLOCK", "THINKNODE"), _RAK4631),
    (
        (
            "TBEAM",
            "T_BEAM",
            "T-BEAM",
            "LILYGO_TBEAM",
            "TLORA_T3S3",
            "T_DECK",
            "TDECK",
            "T_WATCH",
            "T_ETH",
            "T_LORA_PAGER",
            "M5STACK",
            "NANO_G",
        ),
        _AXP_LIPO,
    ),
    (("T_ECHO", "TECHO", "T-ECHO", "XIAO_NRF"), _TECHO),
    (
        (
            "HELTEC",
            "TLORA",
            "LILYGO",
            "WIRELESS_PAPER",
            "WIRELESS_TRACKER",
            "VISION_MASTER",
            "MESH_NODE_T114",
            "MESH_POCKET",
            "CAPSULE_SENSOR",
            "SENSELORA",
            "CDEBYTE",
            "EBYTE",
        ),
        _HELTEC,
    ),
    (("SENSECAP", "STATION_G", "TRACKER_T1000", "SEEED", "WIO_"), _SENSECAP),
)


def normalize_hw_model_key(hw_model: Any) -> str:
    """Normalize DB / protobuf HW labels to an uppercase match key."""
    if hw_model is None:
        return ""
    if isinstance(hw_model, int):
        try:
            from meshtastic import mesh_pb2

            hw_enum = mesh_pb2.HardwareModel.ValueType(hw_model)
            name = mesh_pb2.HardwareModel.Name(hw_enum)
            return str(name).upper().replace("-", "_").replace(" ", "_")
        except Exception:
            return str(hw_model)
    text = str(hw_model).strip().upper()
    return text.replace("-", "_").replace(" ", "_")


def resolve_battery_model(
    hw_model: Any = None,
    *,
    charge_full_voltage: float | None = None,
    near_full_pct: int | None = None,
) -> BatteryVoltageModel:
    """Return the best battery/charger model for a Meshtastic ``hw_model``.

    Optional ``charge_full_voltage`` / ``near_full_pct`` apply per-node Admin
    Power overrides on top of the HW family profile.
    """
    key = normalize_hw_model_key(hw_model)
    model = _DEFAULT
    if key:
        for needles, candidate in _HW_MATCHERS:
            matched = False
            for needle in needles:
                if needle in key:
                    model = candidate
                    matched = True
                    break
            if matched:
                break
    return model.with_overrides(
        charge_full_voltage=charge_full_voltage,
        near_full_pct=near_full_pct,
    )


def hw_model_coverage_stats(hw_names: list[str] | None = None) -> dict[str, Any]:
    """Return matcher coverage counts (useful for tests / diagnostics)."""
    if hw_names is None:
        try:
            from meshtastic import mesh_pb2

            hw_names = list(mesh_pb2.HardwareModel.keys())
        except Exception:
            hw_names = []
    by_key: dict[str, list[str]] = {}
    for name in hw_names:
        model = resolve_battery_model(name)
        by_key.setdefault(model.key, []).append(str(name))
    total = len(hw_names)
    non_default = total - len(by_key.get("default", []))
    return {
        "total": total,
        "non_default": non_default,
        "by_key": {k: len(v) for k, v in sorted(by_key.items())},
        "default_models": sorted(by_key.get("default", [])),
    }
