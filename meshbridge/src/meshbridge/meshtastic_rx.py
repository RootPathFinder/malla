"""Meshtastic MQTT receiver for TEXT_MESSAGE_APP on the MeshCore channel."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import paho.mqtt.client as mqtt
from meshtastic import mesh_pb2, mqtt_pb2, portnums_pb2
from paho.mqtt.enums import CallbackAPIVersion

from meshbridge.config import MeshtasticConfig
from meshbridge.meshtastic_crypto import try_decrypt_keys
from meshbridge.models import BridgeMessage, Direction

logger = logging.getLogger(__name__)

OnMessage = Callable[[BridgeMessage], Awaitable[None]]


class MeshtasticMqttReceiver:
    """Subscribe to Meshtastic MQTT and emit BridgeMessages for channel text."""

    def __init__(self, config: MeshtasticConfig, on_message: OnMessage) -> None:
        self.config = config
        self.on_message = on_message
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: mqtt.Client | None = None
        self._seen_packet_ids: set[int] = set()

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="meshbridge-meshtastic",
        )
        if self.config.mqtt_username:
            client.username_pw_set(
                self.config.mqtt_username, self.config.mqtt_password or None
            )
        client.on_connect = self._on_connect
        client.on_message = self._on_mqtt_message
        self._client = client
        await self._loop.run_in_executor(
            None,
            lambda: client.connect(self.config.mqtt_broker, self.config.mqtt_port, 60),
        )
        client.loop_start()
        logger.info(
            "Meshtastic MQTT connected to %s:%s topic=%s channel=%s",
            self.config.mqtt_broker,
            self.config.mqtt_port,
            self.config.mqtt_topic,
            self.config.channel_name,
        )

    async def stop(self) -> None:
        if self._client is None:
            return
        client = self._client
        self._client = None
        await asyncio.get_running_loop().run_in_executor(None, client.loop_stop)
        await asyncio.get_running_loop().run_in_executor(None, client.disconnect)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        rc = getattr(reason_code, "value", reason_code)
        if rc == 0:
            client.subscribe(self.config.mqtt_topic)
            logger.info("Subscribed to %s", self.config.mqtt_topic)
        else:
            logger.error("Meshtastic MQTT connect failed rc=%s", reason_code)

    def _on_mqtt_message(
        self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ) -> None:
        if "/json/" in msg.topic:
            return
        try:
            bridge_msg = self._parse_envelope(msg.topic, msg.payload)
        except Exception:  # noqa: BLE001
            logger.exception("Failed parsing Meshtastic MQTT message")
            return
        if bridge_msg is None or self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self.on_message(bridge_msg), self._loop)

    def _parse_envelope(self, topic: str, payload: bytes) -> BridgeMessage | None:
        envelope = mqtt_pb2.ServiceEnvelope()
        envelope.ParseFromString(payload)
        packet = envelope.packet
        channel_id = getattr(envelope, "channel_id", "") or ""

        # Prefer ServiceEnvelope channel_id match for the MeshCore channel.
        if self.config.channel_name and channel_id:
            if channel_id.lower() != self.config.channel_name.lower():
                return None

        from_id = int(getattr(packet, "from"))
        packet_id = int(getattr(packet, "id", 0) or 0)
        if packet_id and packet_id in self._seen_packet_ids:
            return None
        if packet_id:
            self._seen_packet_ids.add(packet_id)
            if len(self._seen_packet_ids) > 4096:
                # Drop arbitrary older half
                keep = list(self._seen_packet_ids)[-2048:]
                self._seen_packet_ids = set(keep)

        decoded = self._ensure_decoded(packet, channel_id)
        if decoded is None:
            return None
        if decoded.portnum != portnums_pb2.PortNum.TEXT_MESSAGE_APP:
            return None

        # If channel_id was empty, fall back to MeshPacket.channel == configured index
        # (some gateways omit channel_id; local index matching is best-effort).
        if not channel_id and self.config.channel_name:
            # Without channel_id we only accept if channel field equals configured index
            # OR channel is unset/0 with primary — skip ambiguous packets.
            ch = int(getattr(packet, "channel", 0) or 0)
            if ch != int(self.config.channel_index):
                return None

        text = decoded.payload.decode("utf-8", errors="replace").strip()
        if not text:
            return None

        sender = f"!{from_id:08x}"
        return BridgeMessage(
            direction=Direction.MESHTASTIC_TO_MESHCORE,
            sender=sender,
            text=text,
            source_id=sender,
            packet_id=str(packet_id) if packet_id else "",
        )

    def _ensure_decoded(self, packet: Any, channel_id: str) -> Any | None:
        if (
            hasattr(packet, "decoded")
            and packet.decoded.portnum != portnums_pb2.PortNum.UNKNOWN_APP
            and packet.decoded.payload
        ):
            return packet.decoded

        if not getattr(packet, "encrypted", b""):
            return None
        if not self.config.channel_key:
            return None

        channel_name = channel_id or self.config.channel_name
        # For named secondary channels Meshtastic derives key from PSK+name when
        # the MQTT channel_id is present; also try empty name.
        plain = try_decrypt_keys(
            bytes(packet.encrypted),
            int(packet.id),
            int(getattr(packet, "from")),
            self.config.channel_key,
            channel_name if channel_name != "LongFast" else "",
        )
        if not plain:
            # Retry with configured channel name explicitly
            plain = try_decrypt_keys(
                bytes(packet.encrypted),
                int(packet.id),
                int(getattr(packet, "from")),
                self.config.channel_key,
                self.config.channel_name,
            )
        if not plain:
            return None
        try:
            data = mesh_pb2.Data()
            data.ParseFromString(plain)
        except Exception:  # noqa: BLE001
            return None
        if data.portnum == portnums_pb2.PortNum.UNKNOWN_APP:
            return None
        return data
