"""YAML configuration loader for meshbridge."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class MeshtasticConfig:
    mqtt_broker: str = "127.0.0.1"
    mqtt_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_topic: str = "msh/+/+/+/+"
    channel_name: str = "MeshCore"
    channel_key: str = ""
    channel_index: int = 1
    malla_bot_url: str = "http://127.0.0.1:5008/api/bot/send"
    gateway_node_id: str = ""


@dataclass
class MeshcoreConfig:
    companion_serial: str = ""
    channel_index: int = 1
    channel_name: str = "#meshtastic"
    mqtt_broker: str = ""
    mqtt_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_topic: str = "meshcore/+/+/packets"
    companion_pubkey: str = ""


@dataclass
class BridgeConfig:
    prefix_from_meshcore: str = "[MC]"
    prefix_from_meshtastic: str = "[MT]"
    min_send_interval_sec: float = 2.0
    max_meshcore_chars: int = 133
    max_meshtastic_chars: int = 228
    fingerprint_ttl_sec: float = 120.0


@dataclass
class AppConfig:
    dry_run: bool = True
    log_level: str = "INFO"
    meshtastic: MeshtasticConfig = field(default_factory=MeshtasticConfig)
    meshcore: MeshcoreConfig = field(default_factory=MeshcoreConfig)
    bridge: BridgeConfig = field(default_factory=BridgeConfig)


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key) or {}
    if not isinstance(value, dict):
        raise ValueError(f"Config section '{key}' must be a mapping")
    return value


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load config from YAML path or MESHBRIDGE_CONFIG env."""
    if path is None:
        path = os.environ.get("MESHBRIDGE_CONFIG", "config.yaml")
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Config not found: {config_path}. Copy config.sample.yaml to config.yaml"
        )

    with config_path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping")

    mt = _section(raw, "meshtastic")
    mc = _section(raw, "meshcore")
    br = _section(raw, "bridge")

    return AppConfig(
        dry_run=bool(raw.get("dry_run", True)),
        log_level=str(raw.get("log_level", "INFO")).upper(),
        meshtastic=MeshtasticConfig(
            mqtt_broker=str(mt.get("mqtt_broker", "127.0.0.1")),
            mqtt_port=int(mt.get("mqtt_port", 1883)),
            mqtt_username=str(mt.get("mqtt_username") or ""),
            mqtt_password=str(mt.get("mqtt_password") or ""),
            mqtt_topic=str(mt.get("mqtt_topic", "msh/+/+/+/+")),
            channel_name=str(mt.get("channel_name", "MeshCore")),
            channel_key=str(mt.get("channel_key") or ""),
            channel_index=int(mt.get("channel_index", 1)),
            malla_bot_url=str(
                mt.get("malla_bot_url", "http://127.0.0.1:5008/api/bot/send")
            ),
            gateway_node_id=str(mt.get("gateway_node_id") or ""),
        ),
        meshcore=MeshcoreConfig(
            companion_serial=str(mc.get("companion_serial") or ""),
            channel_index=int(mc.get("channel_index", 1)),
            channel_name=str(mc.get("channel_name", "#meshtastic")),
            mqtt_broker=str(mc.get("mqtt_broker") or ""),
            mqtt_port=int(mc.get("mqtt_port", 1883)),
            mqtt_username=str(mc.get("mqtt_username") or ""),
            mqtt_password=str(mc.get("mqtt_password") or ""),
            mqtt_topic=str(mc.get("mqtt_topic", "meshcore/+/+/packets")),
            companion_pubkey=str(mc.get("companion_pubkey") or ""),
        ),
        bridge=BridgeConfig(
            prefix_from_meshcore=str(br.get("prefix_from_meshcore", "[MC]")),
            prefix_from_meshtastic=str(br.get("prefix_from_meshtastic", "[MT]")),
            min_send_interval_sec=float(br.get("min_send_interval_sec", 2.0)),
            max_meshcore_chars=int(br.get("max_meshcore_chars", 133)),
            max_meshtastic_chars=int(br.get("max_meshtastic_chars", 228)),
            fingerprint_ttl_sec=float(br.get("fingerprint_ttl_sec", 120.0)),
        ),
    )
