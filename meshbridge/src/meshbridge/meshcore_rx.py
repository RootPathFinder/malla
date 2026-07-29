"""MeshCore receivers: companion CHANNEL_MSG_RECV + optional MQTT Format 1."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from meshbridge.config import MeshcoreConfig
from meshbridge.meshcore_crypto import content_hash, try_decrypt_channel_message
from meshbridge.models import BridgeMessage, Direction

logger = logging.getLogger(__name__)

OnMessage = Callable[[BridgeMessage], Awaitable[None]]


class MeshcoreMqttReceiver:
    """Subscribe to MeshCore observer MQTT and decrypt #meshtastic GRP_TXT."""

    def __init__(self, config: MeshcoreConfig, on_message: OnMessage) -> None:
        self.config = config
        self.on_message = on_message
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: mqtt.Client | None = None
        self._seen_hashes: set[str] = set()

    @property
    def enabled(self) -> bool:
        return bool(self.config.mqtt_broker)

    async def start(self) -> None:
        if not self.enabled:
            logger.info("MeshCore MQTT RX disabled (no mqtt_broker)")
            return
        self._loop = asyncio.get_running_loop()
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="meshbridge-meshcore",
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
            "MeshCore MQTT connected to %s:%s topic=%s channel=%s",
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
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, client.loop_stop)
        await loop.run_in_executor(None, client.disconnect)

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
            logger.error("MeshCore MQTT connect failed rc=%s", reason_code)

    def _on_mqtt_message(
        self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ) -> None:
        try:
            bridge_msg = self._parse(msg.payload)
        except Exception:  # noqa: BLE001
            logger.exception("Failed parsing MeshCore MQTT message")
            return
        if bridge_msg is None or self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self.on_message(bridge_msg), self._loop)

    def _parse(self, payload: bytes) -> BridgeMessage | None:
        try:
            data = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        raw_hex = data.get("raw") or data.get("RAW") or ""
        if not raw_hex or not isinstance(raw_hex, str):
            return None

        chash = content_hash(raw_hex)
        if chash in self._seen_hashes:
            return None
        self._seen_hashes.add(chash)
        if len(self._seen_hashes) > 4096:
            self._seen_hashes = set(list(self._seen_hashes)[-2048:])

        plain = try_decrypt_channel_message(raw_hex, self.config.channel_name)
        if plain is None or not plain.message.strip():
            return None

        sender = plain.sender or "unknown"
        return BridgeMessage(
            direction=Direction.MESHCORE_TO_MESHTASTIC,
            sender=sender,
            text=plain.message,
            source_id=sender,
            packet_id=chash,
        )


class MeshcoreCompanionReceiver:
    """Listen for CHANNEL_MSG_RECV on a shared meshcore_py connection."""

    def __init__(
        self,
        config: MeshcoreConfig,
        on_message: OnMessage,
        mesh: Any | None = None,
    ) -> None:
        self.config = config
        self.on_message = on_message
        self._mesh = mesh
        self._owns_mesh = False

    async def start(self, mesh: Any | None = None) -> None:
        if mesh is not None:
            self._mesh = mesh
        if self._mesh is None:
            if not self.config.companion_serial:
                logger.info("MeshCore companion RX disabled (no companion_serial)")
                return
            try:
                from meshcore import MeshCore  # type: ignore[import-untyped]
            except ImportError as exc:
                raise RuntimeError(
                    "meshcore package required for companion RX. "
                    "Install with: pip install 'meshbridge[meshcore]'"
                ) from exc
            self._mesh = await MeshCore.create_serial(self.config.companion_serial)
            self._owns_mesh = True

        try:
            from meshcore import EventType  # type: ignore[import-untyped]
        except ImportError:
            logger.warning("meshcore EventType unavailable; companion RX inactive")
            return

        self._loop = asyncio.get_running_loop()
        self._mesh.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_msg_sync)
        if hasattr(self._mesh, "start_auto_message_fetching"):
            await self._mesh.start_auto_message_fetching()
        logger.info(
            "MeshCore companion RX listening on channel_index=%s",
            self.config.channel_index,
        )

    async def stop(self) -> None:
        if self._owns_mesh and self._mesh is not None:
            try:
                await self._mesh.disconnect()
            except Exception:  # noqa: BLE001
                logger.exception("Error disconnecting companion RX")
            self._mesh = None

    def _on_channel_msg_sync(self, event: Any) -> None:
        """Sync callback for meshcore_py; schedule async handling."""
        loop = getattr(self, "_loop", None)
        if loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._on_channel_msg(event), loop)

    async def _on_channel_msg(self, event: Any) -> None:
        try:
            payload = getattr(event, "payload", event) or {}
            if not isinstance(payload, dict):
                return
            channel_idx = payload.get("channel_idx", payload.get("channel_index"))
            if channel_idx is not None and int(channel_idx) != int(
                self.config.channel_index
            ):
                return
            text = (payload.get("text") or payload.get("message") or "").strip()
            if not text:
                return
            # Companion often delivers "sender: message" already formatted
            sender = str(payload.get("sender") or payload.get("name") or "").strip()
            if not sender and ": " in text:
                maybe_sender, maybe_text = text.split(": ", 1)
                if len(maybe_sender) < 50 and ":" not in maybe_sender:
                    sender, text = maybe_sender, maybe_text
            if not sender:
                sender = "unknown"

            msg = BridgeMessage(
                direction=Direction.MESHCORE_TO_MESHTASTIC,
                sender=sender,
                text=text,
                source_id=str(payload.get("pubkey_prefix") or sender),
                packet_id=str(payload.get("sender_timestamp") or ""),
            )
            await self.on_message(msg)
        except Exception:  # noqa: BLE001
            logger.exception("Companion CHANNEL_MSG_RECV handler error")
