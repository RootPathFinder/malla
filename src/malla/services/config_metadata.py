"""
Configuration field metadata for Meshtastic admin settings.

This module defines field types, enum values, and validation for each
configurable field to enable proper form rendering and editing.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class FieldType(Enum):
    """Types of config fields."""

    TEXT = "text"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ENUM = "enum"
    PASSWORD = "password"
    READONLY = "readonly"


@dataclass
class FieldMetadata:
    """Metadata for a single config field."""

    name: str
    label: str
    field_type: FieldType
    description: str = ""
    min_value: int | None = None
    max_value: int | None = None
    enum_values: dict[int, str] | None = None
    readonly: bool = False
    unit: str = ""


# ============================================================================
# Enum Definitions from Meshtastic Protobufs
# ============================================================================

DEVICE_ROLE_ENUM = {
    0: "CLIENT",
    1: "CLIENT_MUTE",
    2: "ROUTER",
    3: "ROUTER_CLIENT",
    4: "REPEATER",
    5: "TRACKER",
    6: "SENSOR",
    7: "TAK",
    8: "CLIENT_HIDDEN",
    9: "LOST_AND_FOUND",
    10: "TAK_TRACKER",
    11: "ROUTER_LATE",
    12: "CLIENT_BASE",
}

REBROADCAST_MODE_ENUM = {
    0: "ALL",
    1: "ALL_SKIP_DECODING",
    2: "LOCAL_ONLY",
    3: "KNOWN_ONLY",
    4: "NONE",
    5: "CORE_PORTNUMS_ONLY",
}

GPS_MODE_ENUM = {
    0: "DISABLED",
    1: "ENABLED",
    2: "NOT_PRESENT",
}

DISPLAY_UNITS_ENUM = {
    0: "METRIC",
    1: "IMPERIAL",
}

GPS_FORMAT_ENUM = {
    0: "DEC",
    1: "DMS",
    2: "UTM",
    3: "MGRS",
    4: "OLC",
    5: "OSGR",
}

LORA_REGION_ENUM = {
    0: "UNSET",
    1: "US",
    2: "EU_433",
    3: "EU_868",
    4: "CN",
    5: "JP",
    6: "ANZ",
    7: "KR",
    8: "TW",
    9: "RU",
    10: "IN",
    11: "NZ_865",
    12: "TH",
    13: "LORA_24",
    14: "UA_433",
    15: "UA_868",
    16: "MY_433",
    17: "MY_919",
    18: "SG_923",
    19: "PH_433",
    20: "PH_868",
    21: "PH_915",
    22: "UNSET_2",
    23: "VE",
    24: "WLAN",
    25: "IL_868",
}

MODEM_PRESET_ENUM = {
    0: "LONG_FAST",
    1: "LONG_SLOW",
    2: "VERY_LONG_SLOW",
    3: "MEDIUM_SLOW",
    4: "MEDIUM_FAST",
    5: "SHORT_SLOW",
    6: "SHORT_FAST",
    7: "LONG_MODERATE",
    8: "SHORT_TURBO",
}

BLUETOOTH_MODE_ENUM = {
    0: "RANDOM_PIN",
    1: "FIXED_PIN",
    2: "NO_PIN",
}

CHANNEL_ROLE_ENUM = {
    0: "DISABLED",
    1: "PRIMARY",
    2: "SECONDARY",
}


# ============================================================================
# Config Field Definitions
# ============================================================================

DEVICE_CONFIG_FIELDS = [
    FieldMetadata(
        name="role",
        label="Device Role",
        field_type=FieldType.ENUM,
        description="The role this node plays in the mesh network",
        enum_values=DEVICE_ROLE_ENUM,
    ),
    FieldMetadata(
        name="serial_enabled",
        label="Serial Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable serial console for CLI access",
    ),
    FieldMetadata(
        name="button_gpio",
        label="Button GPIO",
        field_type=FieldType.NUMBER,
        description="GPIO pin for the user button",
        min_value=0,
        max_value=48,
    ),
    FieldMetadata(
        name="buzzer_gpio",
        label="Buzzer GPIO",
        field_type=FieldType.NUMBER,
        description="GPIO pin for the buzzer",
        min_value=0,
        max_value=48,
    ),
    FieldMetadata(
        name="rebroadcast_mode",
        label="Rebroadcast Mode",
        field_type=FieldType.ENUM,
        description="How this node rebroadcasts messages",
        enum_values=REBROADCAST_MODE_ENUM,
    ),
    FieldMetadata(
        name="node_info_broadcast_secs",
        label="Node Info Broadcast Interval",
        field_type=FieldType.NUMBER,
        description="How often to send node info broadcasts",
        min_value=0,
        max_value=86400,
        unit="seconds",
    ),
]

POSITION_CONFIG_FIELDS = [
    FieldMetadata(
        name="position_broadcast_secs",
        label="Position Broadcast Interval",
        field_type=FieldType.NUMBER,
        description="How often to send position updates",
        min_value=0,
        max_value=86400,
        unit="seconds",
    ),
    FieldMetadata(
        name="position_broadcast_smart_enabled",
        label="Smart Position Broadcast",
        field_type=FieldType.BOOLEAN,
        description="Enable smart position broadcasting (only when moving)",
    ),
    FieldMetadata(
        name="gps_enabled",
        label="GPS Mode",
        field_type=FieldType.ENUM,
        description="GPS operating mode",
        enum_values=GPS_MODE_ENUM,
    ),
    FieldMetadata(
        name="fixed_position",
        label="Fixed Position",
        field_type=FieldType.BOOLEAN,
        description="Use fixed position instead of GPS",
    ),
]

POWER_CONFIG_FIELDS = [
    FieldMetadata(
        name="is_power_saving",
        label="Power Saving Mode",
        field_type=FieldType.BOOLEAN,
        description="Enable power saving features",
    ),
    FieldMetadata(
        name="on_battery_shutdown_after_secs",
        label="Battery Shutdown Delay",
        field_type=FieldType.NUMBER,
        description="Seconds before shutdown on low battery (0 = disabled)",
        min_value=0,
        max_value=86400,
        unit="seconds",
    ),
    FieldMetadata(
        name="adc_multiplier_override",
        label="ADC Multiplier Override",
        field_type=FieldType.NUMBER,
        description="Override for battery ADC multiplier",
        min_value=0,
        max_value=10,
    ),
    FieldMetadata(
        name="wait_bluetooth_secs",
        label="Wait Bluetooth Seconds",
        field_type=FieldType.NUMBER,
        description="Seconds to wait for Bluetooth before sleeping",
        min_value=0,
        max_value=3600,
        unit="seconds",
    ),
    FieldMetadata(
        name="sds_secs",
        label="Super Deep Sleep Seconds",
        field_type=FieldType.NUMBER,
        description="Seconds before entering super deep sleep",
        min_value=0,
        max_value=604800,
        unit="seconds",
    ),
    FieldMetadata(
        name="ls_secs",
        label="Light Sleep Seconds",
        field_type=FieldType.NUMBER,
        description="Seconds before entering light sleep",
        min_value=0,
        max_value=86400,
        unit="seconds",
    ),
    FieldMetadata(
        name="min_wake_secs",
        label="Minimum Wake Seconds",
        field_type=FieldType.NUMBER,
        description="Minimum seconds to stay awake after waking",
        min_value=0,
        max_value=3600,
        unit="seconds",
    ),
]

NETWORK_CONFIG_FIELDS = [
    FieldMetadata(
        name="wifi_enabled",
        label="WiFi Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable WiFi connectivity",
    ),
    FieldMetadata(
        name="wifi_ssid",
        label="WiFi SSID",
        field_type=FieldType.TEXT,
        description="WiFi network name",
    ),
    FieldMetadata(
        name="eth_enabled",
        label="Ethernet Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable Ethernet connectivity",
    ),
]

DISPLAY_CONFIG_FIELDS = [
    FieldMetadata(
        name="screen_on_secs",
        label="Screen On Duration",
        field_type=FieldType.NUMBER,
        description="Seconds before screen turns off",
        min_value=0,
        max_value=3600,
        unit="seconds",
    ),
    FieldMetadata(
        name="gps_format",
        label="GPS Coordinate Format",
        field_type=FieldType.ENUM,
        description="Format for displaying GPS coordinates",
        enum_values=GPS_FORMAT_ENUM,
    ),
    FieldMetadata(
        name="auto_screen_carousel_secs",
        label="Screen Carousel Interval",
        field_type=FieldType.NUMBER,
        description="Seconds between screen carousel rotations (0 = disabled)",
        min_value=0,
        max_value=300,
        unit="seconds",
    ),
    FieldMetadata(
        name="compass_north_top",
        label="Compass North at Top",
        field_type=FieldType.BOOLEAN,
        description="Show compass with North at top",
    ),
    FieldMetadata(
        name="flip_screen",
        label="Flip Screen",
        field_type=FieldType.BOOLEAN,
        description="Flip the display 180 degrees",
    ),
    FieldMetadata(
        name="units",
        label="Display Units",
        field_type=FieldType.ENUM,
        description="Unit system for display",
        enum_values=DISPLAY_UNITS_ENUM,
    ),
]

LORA_CONFIG_FIELDS = [
    FieldMetadata(
        name="use_preset",
        label="Use Modem Preset",
        field_type=FieldType.BOOLEAN,
        description="Use a predefined modem configuration",
    ),
    FieldMetadata(
        name="modem_preset",
        label="Modem Preset",
        field_type=FieldType.ENUM,
        description="Predefined LoRa modem configuration",
        enum_values=MODEM_PRESET_ENUM,
    ),
    FieldMetadata(
        name="bandwidth",
        label="Bandwidth",
        field_type=FieldType.NUMBER,
        description="LoRa bandwidth in Hz (only when not using preset)",
        min_value=0,
        max_value=500000,
        unit="Hz",
    ),
    FieldMetadata(
        name="spread_factor",
        label="Spread Factor",
        field_type=FieldType.NUMBER,
        description="LoRa spread factor (7-12)",
        min_value=7,
        max_value=12,
    ),
    FieldMetadata(
        name="coding_rate",
        label="Coding Rate",
        field_type=FieldType.NUMBER,
        description="LoRa coding rate (5-8)",
        min_value=5,
        max_value=8,
    ),
    FieldMetadata(
        name="frequency_offset",
        label="Frequency Offset",
        field_type=FieldType.NUMBER,
        description="Frequency offset in Hz",
        min_value=-1000000,
        max_value=1000000,
        unit="Hz",
    ),
    FieldMetadata(
        name="region",
        label="Region",
        field_type=FieldType.ENUM,
        description="LoRa frequency region",
        enum_values=LORA_REGION_ENUM,
    ),
    FieldMetadata(
        name="hop_limit",
        label="Hop Limit",
        field_type=FieldType.NUMBER,
        description="Maximum number of hops for messages",
        min_value=0,
        max_value=7,
    ),
    FieldMetadata(
        name="tx_enabled",
        label="TX Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable radio transmission",
    ),
    FieldMetadata(
        name="tx_power",
        label="TX Power",
        field_type=FieldType.NUMBER,
        description="Transmission power in dBm",
        min_value=0,
        max_value=30,
        unit="dBm",
    ),
    FieldMetadata(
        name="channel_num",
        label="Channel Number",
        field_type=FieldType.NUMBER,
        description="LoRa channel number within the region",
        min_value=0,
        max_value=255,
    ),
]

BLUETOOTH_CONFIG_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Bluetooth Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable Bluetooth connectivity",
    ),
    FieldMetadata(
        name="mode",
        label="Bluetooth Pairing Mode",
        field_type=FieldType.ENUM,
        description="Bluetooth pairing security mode",
        enum_values=BLUETOOTH_MODE_ENUM,
    ),
    FieldMetadata(
        name="fixed_pin",
        label="Fixed PIN",
        field_type=FieldType.NUMBER,
        description="Fixed PIN code for Bluetooth pairing (when using FIXED_PIN mode)",
        min_value=0,
        max_value=999999,
    ),
]

CHANNEL_FIELDS = [
    FieldMetadata(
        name="index",
        label="Channel Index",
        field_type=FieldType.READONLY,
        description="Channel slot number (0-7)",
        readonly=True,
    ),
    FieldMetadata(
        name="role",
        label="Channel Role",
        field_type=FieldType.ENUM,
        description="Role of this channel",
        enum_values=CHANNEL_ROLE_ENUM,
    ),
    FieldMetadata(
        name="name",
        label="Channel Name",
        field_type=FieldType.TEXT,
        description="Display name for this channel",
    ),
    FieldMetadata(
        name="psk",
        label="Pre-Shared Key",
        field_type=FieldType.PASSWORD,
        description="Encryption key for this channel (hex)",
    ),
    FieldMetadata(
        name="position_precision",
        label="Position Precision",
        field_type=FieldType.NUMBER,
        description="Bits of position precision to share (0=full precision, 32=no location)",
        min_value=0,
        max_value=32,
    ),
]


# ============================================================================
# Config Metadata Lookup
# ============================================================================

CONFIG_METADATA: dict[str, list[FieldMetadata]] = {
    "device": DEVICE_CONFIG_FIELDS,
    "position": POSITION_CONFIG_FIELDS,
    "power": POWER_CONFIG_FIELDS,
    "network": NETWORK_CONFIG_FIELDS,
    "display": DISPLAY_CONFIG_FIELDS,
    "lora": LORA_CONFIG_FIELDS,
    "bluetooth": BLUETOOTH_CONFIG_FIELDS,
    "channel": CHANNEL_FIELDS,
}


def get_config_metadata(config_type: str) -> list[FieldMetadata]:
    """
    Get field metadata for a config type.

    Args:
        config_type: The config type (device, position, etc.)

    Returns:
        List of FieldMetadata for the config type
    """
    return CONFIG_METADATA.get(config_type.lower(), [])


def get_config_schema(config_type: str) -> list[dict[str, Any]]:
    """
    Get config field schema as JSON-serializable dict.

    Args:
        config_type: The config type (device, position, etc.)

    Returns:
        List of field definitions suitable for JSON response
    """
    fields = get_config_metadata(config_type)
    return [
        {
            "name": f.name,
            "label": f.label,
            "type": f.field_type.value,
            "description": f.description,
            "min": f.min_value,
            "max": f.max_value,
            "enum": f.enum_values,
            "readonly": f.readonly,
            "unit": f.unit,
        }
        for f in fields
    ]


def get_all_config_schemas() -> dict[str, list[dict[str, Any]]]:
    """
    Get all config schemas.

    Returns:
        Dict of config type to field schemas
    """
    return {
        config_type: get_config_schema(config_type)
        for config_type in CONFIG_METADATA.keys()
    }
