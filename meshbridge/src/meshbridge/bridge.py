"""Core bidirectional bridge orchestration."""

from __future__ import annotations

import asyncio
import logging
import time

from meshbridge.chunking import chunk_text, format_from_meshcore, format_from_meshtastic
from meshbridge.config import AppConfig
from meshbridge.dedup import Deduper
from meshbridge.meshcore_rx import MeshcoreCompanionReceiver, MeshcoreMqttReceiver
from meshbridge.meshcore_tx import MeshcoreCompanionSender
from meshbridge.meshtastic_rx import MeshtasticMqttReceiver
from meshbridge.meshtastic_tx import MeshtasticBotSender
from meshbridge.models import BridgeMessage, Direction

logger = logging.getLogger(__name__)


class ChannelBridge:
    """Bridge MeshCore #meshtastic ↔ Meshtastic MeshCore channel."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.deduper = Deduper(
            prefix_from_meshcore=config.bridge.prefix_from_meshcore,
            prefix_from_meshtastic=config.bridge.prefix_from_meshtastic,
            gateway_node_id=config.meshtastic.gateway_node_id,
            companion_pubkey=config.meshcore.companion_pubkey,
            fingerprint_ttl_sec=config.bridge.fingerprint_ttl_sec,
        )
        self.mt_tx = MeshtasticBotSender(config.meshtastic, dry_run=config.dry_run)
        self.mc_tx = MeshcoreCompanionSender(config.meshcore, dry_run=config.dry_run)
        self.mt_rx = MeshtasticMqttReceiver(config.meshtastic, self.handle_inbound)
        self.mc_mqtt_rx = MeshcoreMqttReceiver(config.meshcore, self.handle_inbound)
        self.mc_companion_rx = MeshcoreCompanionReceiver(
            config.meshcore, self.handle_inbound
        )
        self._last_send_at = 0.0
        self._send_lock = asyncio.Lock()
        self._stats = {
            "mt_in": 0,
            "mc_in": 0,
            "mt_out": 0,
            "mc_out": 0,
            "dropped": 0,
        }

    async def start(self) -> None:
        await self.mt_tx.start()
        await self.mc_tx.start()
        await self.mt_rx.start()
        await self.mc_mqtt_rx.start()
        # Share companion connection between TX and RX when available
        await self.mc_companion_rx.start(mesh=self.mc_tx.mesh)
        logger.info(
            "Bridge started dry_run=%s mt_channel=%s/%s mc_channel=%s/%s",
            self.config.dry_run,
            self.config.meshtastic.channel_name,
            self.config.meshtastic.channel_index,
            self.config.meshcore.channel_name,
            self.config.meshcore.channel_index,
        )

    async def stop(self) -> None:
        await self.mt_rx.stop()
        await self.mc_mqtt_rx.stop()
        await self.mc_companion_rx.stop()
        await self.mc_tx.stop()
        await self.mt_tx.stop()
        logger.info("Bridge stopped stats=%s", self._stats)

    async def handle_inbound(self, msg: BridgeMessage) -> None:
        if msg.direction == Direction.MESHTASTIC_TO_MESHCORE:
            self._stats["mt_in"] += 1
        else:
            self._stats["mc_in"] += 1

        if not self.deduper.should_forward(msg):
            self._stats["dropped"] += 1
            return

        if msg.direction == Direction.MESHTASTIC_TO_MESHCORE:
            outbound = format_from_meshtastic(
                msg.sender, msg.text, self.config.bridge.prefix_from_meshtastic
            )
            chunks = chunk_text(outbound, self.config.bridge.max_meshcore_chars)
            for chunk in chunks:
                ok = await self._rate_limited_send(Direction.MESHTASTIC_TO_MESHCORE, chunk)
                if ok:
                    self._stats["mc_out"] += 1
                    self.deduper.remember_outbound(
                        Direction.MESHTASTIC_TO_MESHCORE, msg.sender, chunk
                    )
        else:
            outbound = format_from_meshcore(
                msg.sender, msg.text, self.config.bridge.prefix_from_meshcore
            )
            chunks = chunk_text(outbound, self.config.bridge.max_meshtastic_chars)
            for chunk in chunks:
                ok = await self._rate_limited_send(Direction.MESHCORE_TO_MESHTASTIC, chunk)
                if ok:
                    self._stats["mt_out"] += 1
                    self.deduper.remember_outbound(
                        Direction.MESHCORE_TO_MESHTASTIC, msg.sender, chunk
                    )

    async def _rate_limited_send(self, direction: Direction, text: str) -> bool:
        async with self._send_lock:
            interval = self.config.bridge.min_send_interval_sec
            elapsed = time.monotonic() - self._last_send_at
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)
            if direction == Direction.MESHTASTIC_TO_MESHCORE:
                ok = await self.mc_tx.send(text)
            else:
                ok = await self.mt_tx.send(text)
            self._last_send_at = time.monotonic()
            return ok

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)
