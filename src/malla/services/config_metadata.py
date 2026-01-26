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

SECURITY_CONFIG_FIELDS = [
    FieldMetadata(
        name="public_key",
        label="Public Key",
        field_type=FieldType.PASSWORD,
        description="Node's public key for PKI encryption (hex). Read-only, generated by device.",
        readonly=True,
    ),
    FieldMetadata(
        name="private_key",
        label="Private Key",
        field_type=FieldType.PASSWORD,
        description="Node's private key for PKI encryption (hex). Sensitive, handle with care.",
        readonly=True,
    ),
    FieldMetadata(
        name="admin_key",
        label="Admin Keys",
        field_type=FieldType.TEXT,
        description="List of admin public keys (hex, comma-separated). Up to 3 keys allowed.",
    ),
    FieldMetadata(
        name="is_managed",
        label="Managed Mode",
        field_type=FieldType.BOOLEAN,
        description="If true, device is in managed mode (certain settings locked)",
    ),
    FieldMetadata(
        name="serial_enabled",
        label="Serial Console Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable serial console access for debugging/CLI",
    ),
    FieldMetadata(
        name="debug_log_api_enabled",
        label="Debug Log API Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable debug logging via API",
    ),
    FieldMetadata(
        name="admin_channel_enabled",
        label="Admin Channel Enabled",
        field_type=FieldType.BOOLEAN,
        description="Allow admin commands over the admin channel (not just PKI)",
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
# Module Config Field Definitions
# ============================================================================

MQTT_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable MQTT module",
    ),
    FieldMetadata(
        name="address",
        label="Server Address",
        field_type=FieldType.TEXT,
        description="MQTT broker address",
    ),
    FieldMetadata(
        name="username",
        label="Username",
        field_type=FieldType.TEXT,
        description="MQTT username",
    ),
    FieldMetadata(
        name="password",
        label="Password",
        field_type=FieldType.PASSWORD,
        description="MQTT password",
    ),
    FieldMetadata(
        name="encryption_enabled",
        label="Encryption Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable encryption for MQTT messages",
    ),
    FieldMetadata(
        name="json_enabled",
        label="JSON Enabled",
        field_type=FieldType.BOOLEAN,
        description="Send messages as JSON",
    ),
    FieldMetadata(
        name="tls_enabled",
        label="TLS Enabled",
        field_type=FieldType.BOOLEAN,
        description="Use TLS for MQTT connection",
    ),
    FieldMetadata(
        name="root",
        label="Root Topic",
        field_type=FieldType.TEXT,
        description="Root topic for MQTT messages",
    ),
    FieldMetadata(
        name="proxy_to_client_enabled",
        label="Proxy to Client",
        field_type=FieldType.BOOLEAN,
        description="Proxy messages to connected clients",
    ),
    FieldMetadata(
        name="map_reporting_enabled",
        label="Map Reporting",
        field_type=FieldType.BOOLEAN,
        description="Report node position to map server",
    ),
]

SERIAL_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable serial module",
    ),
    FieldMetadata(
        name="echo",
        label="Echo",
        field_type=FieldType.BOOLEAN,
        description="Echo received characters",
    ),
    FieldMetadata(
        name="rxd",
        label="RXD Pin",
        field_type=FieldType.NUMBER,
        description="RX pin number",
    ),
    FieldMetadata(
        name="txd",
        label="TXD Pin",
        field_type=FieldType.NUMBER,
        description="TX pin number",
    ),
    FieldMetadata(
        name="baud",
        label="Baud Rate",
        field_type=FieldType.NUMBER,
        description="Serial baud rate",
    ),
    FieldMetadata(
        name="timeout",
        label="Timeout",
        field_type=FieldType.NUMBER,
        description="Timeout in seconds",
    ),
]

TELEMETRY_MODULE_FIELDS = [
    FieldMetadata(
        name="device_update_interval",
        label="Device Update Interval",
        field_type=FieldType.NUMBER,
        description="Interval for device metrics (seconds)",
        unit="seconds",
    ),
    FieldMetadata(
        name="environment_update_interval",
        label="Environment Update Interval",
        field_type=FieldType.NUMBER,
        description="Interval for environment metrics (seconds)",
        unit="seconds",
    ),
    FieldMetadata(
        name="environment_measurement_enabled",
        label="Environment Measurement",
        field_type=FieldType.BOOLEAN,
        description="Enable environment sensor readings",
    ),
    FieldMetadata(
        name="environment_screen_enabled",
        label="Environment on Screen",
        field_type=FieldType.BOOLEAN,
        description="Show environment data on screen",
    ),
    FieldMetadata(
        name="environment_display_fahrenheit",
        label="Display Fahrenheit",
        field_type=FieldType.BOOLEAN,
        description="Show temperature in Fahrenheit",
    ),
    FieldMetadata(
        name="air_quality_enabled",
        label="Air Quality",
        field_type=FieldType.BOOLEAN,
        description="Enable air quality sensor",
    ),
    FieldMetadata(
        name="air_quality_interval",
        label="Air Quality Interval",
        field_type=FieldType.NUMBER,
        description="Interval for air quality readings (seconds)",
        unit="seconds",
    ),
    FieldMetadata(
        name="power_measurement_enabled",
        label="Power Measurement",
        field_type=FieldType.BOOLEAN,
        description="Enable power measurement",
    ),
    FieldMetadata(
        name="power_update_interval",
        label="Power Update Interval",
        field_type=FieldType.NUMBER,
        description="Interval for power measurements (seconds)",
        unit="seconds",
    ),
    FieldMetadata(
        name="power_screen_enabled",
        label="Power on Screen",
        field_type=FieldType.BOOLEAN,
        description="Show power data on screen",
    ),
    FieldMetadata(
        name="device_telemetry_enabled",
        label="Device Telemetry",
        field_type=FieldType.BOOLEAN,
        description="Enable device telemetry broadcast (battery, voltage, utilization)",
    ),
    FieldMetadata(
        name="health_measurement_enabled",
        label="Health Measurement",
        field_type=FieldType.BOOLEAN,
        description="Enable health sensor readings",
    ),
    FieldMetadata(
        name="health_update_interval",
        label="Health Update Interval",
        field_type=FieldType.NUMBER,
        description="Interval for health measurements (seconds)",
        unit="seconds",
    ),
    FieldMetadata(
        name="health_screen_enabled",
        label="Health on Screen",
        field_type=FieldType.BOOLEAN,
        description="Show health data on screen",
    ),
]

EXTNOTIF_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable external notifications",
    ),
    FieldMetadata(
        name="output_ms",
        label="Output Duration",
        field_type=FieldType.NUMBER,
        description="Duration of notification in milliseconds",
        unit="ms",
    ),
    FieldMetadata(
        name="output",
        label="Output Pin",
        field_type=FieldType.NUMBER,
        description="GPIO pin for notification output",
    ),
    FieldMetadata(
        name="output_vibra",
        label="Vibration Pin",
        field_type=FieldType.NUMBER,
        description="GPIO pin for vibration motor",
    ),
    FieldMetadata(
        name="output_buzzer",
        label="Buzzer Pin",
        field_type=FieldType.NUMBER,
        description="GPIO pin for buzzer",
    ),
    FieldMetadata(
        name="active",
        label="Active High",
        field_type=FieldType.BOOLEAN,
        description="Output is active high",
    ),
    FieldMetadata(
        name="alert_message",
        label="Alert on Message",
        field_type=FieldType.BOOLEAN,
        description="Alert on incoming messages",
    ),
    FieldMetadata(
        name="alert_message_vibra",
        label="Vibrate on Message",
        field_type=FieldType.BOOLEAN,
        description="Vibrate on incoming messages",
    ),
    FieldMetadata(
        name="alert_message_buzzer",
        label="Buzzer on Message",
        field_type=FieldType.BOOLEAN,
        description="Buzzer on incoming messages",
    ),
    FieldMetadata(
        name="alert_bell",
        label="Alert on Bell",
        field_type=FieldType.BOOLEAN,
        description="Alert on bell character",
    ),
    FieldMetadata(
        name="alert_bell_vibra",
        label="Vibrate on Bell",
        field_type=FieldType.BOOLEAN,
        description="Vibrate on bell character",
    ),
    FieldMetadata(
        name="alert_bell_buzzer",
        label="Buzzer on Bell",
        field_type=FieldType.BOOLEAN,
        description="Buzzer on bell character",
    ),
    FieldMetadata(
        name="use_pwm",
        label="Use PWM",
        field_type=FieldType.BOOLEAN,
        description="Use PWM for buzzer tones",
    ),
]

STOREFORWARD_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable store and forward module",
    ),
    FieldMetadata(
        name="heartbeat",
        label="Heartbeat",
        field_type=FieldType.BOOLEAN,
        description="Send heartbeat messages",
    ),
    FieldMetadata(
        name="records",
        label="Records",
        field_type=FieldType.NUMBER,
        description="Maximum number of records to store",
    ),
    FieldMetadata(
        name="history_return_max",
        label="History Return Max",
        field_type=FieldType.NUMBER,
        description="Maximum messages to return in history",
    ),
    FieldMetadata(
        name="history_return_window",
        label="History Return Window",
        field_type=FieldType.NUMBER,
        description="Time window for history in minutes",
        unit="minutes",
    ),
]

RANGETEST_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable range test module",
    ),
    FieldMetadata(
        name="sender",
        label="Sender",
        field_type=FieldType.NUMBER,
        description="Sender interval in seconds (0=receiver mode)",
        unit="seconds",
    ),
    FieldMetadata(
        name="save",
        label="Save Results",
        field_type=FieldType.BOOLEAN,
        description="Save results to file system",
    ),
]

CANNEDMSG_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable canned message module",
    ),
    FieldMetadata(
        name="allow_input_source",
        label="Allow Input Source",
        field_type=FieldType.NUMBER,
        description="Input source for canned messages",
    ),
    FieldMetadata(
        name="send_bell",
        label="Send Bell",
        field_type=FieldType.BOOLEAN,
        description="Send bell character with messages",
    ),
]

AUDIO_MODULE_FIELDS = [
    FieldMetadata(
        name="codec2_enabled",
        label="Codec2 Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable Codec2 audio codec",
    ),
    FieldMetadata(
        name="ptt_pin",
        label="PTT Pin",
        field_type=FieldType.NUMBER,
        description="GPIO pin for push-to-talk",
    ),
    FieldMetadata(
        name="bitrate",
        label="Bitrate",
        field_type=FieldType.NUMBER,
        description="Audio bitrate",
    ),
    FieldMetadata(
        name="i2s_ws",
        label="I2S WS Pin",
        field_type=FieldType.NUMBER,
        description="I2S word select pin",
    ),
    FieldMetadata(
        name="i2s_sd",
        label="I2S SD Pin",
        field_type=FieldType.NUMBER,
        description="I2S serial data pin",
    ),
    FieldMetadata(
        name="i2s_din",
        label="I2S DIN Pin",
        field_type=FieldType.NUMBER,
        description="I2S data input pin",
    ),
    FieldMetadata(
        name="i2s_sck",
        label="I2S SCK Pin",
        field_type=FieldType.NUMBER,
        description="I2S serial clock pin",
    ),
]

REMOTEHARDWARE_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable remote hardware module",
    ),
    FieldMetadata(
        name="allow_undefined_pin_access",
        label="Allow Undefined Pin Access",
        field_type=FieldType.BOOLEAN,
        description="Allow access to undefined GPIO pins",
    ),
]

NEIGHBORINFO_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable neighbor info module",
    ),
    FieldMetadata(
        name="update_interval",
        label="Update Interval",
        field_type=FieldType.NUMBER,
        description="Interval for neighbor info updates (seconds)",
        unit="seconds",
    ),
]

AMBIENTLIGHTING_MODULE_FIELDS = [
    FieldMetadata(
        name="led_state",
        label="LED State",
        field_type=FieldType.BOOLEAN,
        description="LED on/off state",
    ),
    FieldMetadata(
        name="current",
        label="Current",
        field_type=FieldType.NUMBER,
        description="LED current in mA",
        unit="mA",
    ),
    FieldMetadata(
        name="red",
        label="Red",
        field_type=FieldType.NUMBER,
        description="Red component (0-255)",
        min_value=0,
        max_value=255,
    ),
    FieldMetadata(
        name="green",
        label="Green",
        field_type=FieldType.NUMBER,
        description="Green component (0-255)",
        min_value=0,
        max_value=255,
    ),
    FieldMetadata(
        name="blue",
        label="Blue",
        field_type=FieldType.NUMBER,
        description="Blue component (0-255)",
        min_value=0,
        max_value=255,
    ),
]

DETECTIONSENSOR_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable detection sensor module",
    ),
    FieldMetadata(
        name="minimum_broadcast_secs",
        label="Minimum Broadcast Interval",
        field_type=FieldType.NUMBER,
        description="Minimum seconds between broadcasts",
        unit="seconds",
    ),
    FieldMetadata(
        name="state_broadcast_secs",
        label="State Broadcast Interval",
        field_type=FieldType.NUMBER,
        description="Seconds between state broadcasts",
        unit="seconds",
    ),
    FieldMetadata(
        name="send_bell",
        label="Send Bell",
        field_type=FieldType.BOOLEAN,
        description="Send bell character with detection",
    ),
    FieldMetadata(
        name="name",
        label="Sensor Name",
        field_type=FieldType.TEXT,
        description="Name of the detection sensor",
    ),
    FieldMetadata(
        name="monitor_pin",
        label="Monitor Pin",
        field_type=FieldType.NUMBER,
        description="GPIO pin to monitor",
    ),
    FieldMetadata(
        name="detection_triggered_high",
        label="Triggered High",
        field_type=FieldType.BOOLEAN,
        description="Detection triggers on high signal",
    ),
    FieldMetadata(
        name="use_pullup",
        label="Use Pullup",
        field_type=FieldType.BOOLEAN,
        description="Enable internal pullup resistor",
    ),
]

PAXCOUNTER_MODULE_FIELDS = [
    FieldMetadata(
        name="enabled",
        label="Enabled",
        field_type=FieldType.BOOLEAN,
        description="Enable PAX counter module",
    ),
    FieldMetadata(
        name="paxcounter_update_interval",
        label="Update Interval",
        field_type=FieldType.NUMBER,
        description="Interval for PAX counter updates (seconds)",
        unit="seconds",
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
    "security": SECURITY_CONFIG_FIELDS,
    "channel": CHANNEL_FIELDS,
}

MODULE_CONFIG_METADATA: dict[str, list[FieldMetadata]] = {
    "mqtt": MQTT_MODULE_FIELDS,
    "serial": SERIAL_MODULE_FIELDS,
    "extnotif": EXTNOTIF_MODULE_FIELDS,
    "storeforward": STOREFORWARD_MODULE_FIELDS,
    "rangetest": RANGETEST_MODULE_FIELDS,
    "telemetry": TELEMETRY_MODULE_FIELDS,
    "cannedmsg": CANNEDMSG_MODULE_FIELDS,
    "audio": AUDIO_MODULE_FIELDS,
    "remotehardware": REMOTEHARDWARE_MODULE_FIELDS,
    "neighborinfo": NEIGHBORINFO_MODULE_FIELDS,
    "ambientlighting": AMBIENTLIGHTING_MODULE_FIELDS,
    "detectionsensor": DETECTIONSENSOR_MODULE_FIELDS,
    "paxcounter": PAXCOUNTER_MODULE_FIELDS,
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


def get_module_config_metadata(module_type: str) -> list[FieldMetadata]:
    """
    Get field metadata for a module config type.

    Args:
        module_type: The module type (mqtt, serial, etc.)

    Returns:
        List of FieldMetadata for the module type
    """
    return MODULE_CONFIG_METADATA.get(module_type.lower(), [])


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


def get_module_config_schema(module_type: str) -> list[dict[str, Any]]:
    """
    Get module config field schema as JSON-serializable dict.

    Args:
        module_type: The module type (mqtt, serial, etc.)

    Returns:
        List of field definitions suitable for JSON response
    """
    fields = get_module_config_metadata(module_type)
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


def get_all_module_config_schemas() -> dict[str, list[dict[str, Any]]]:
    """
    Get all module config schemas.

    Returns:
        Dict of module type to field schemas
    """
    return {
        module_type: get_module_config_schema(module_type)
        for module_type in MODULE_CONFIG_METADATA.keys()
    }
