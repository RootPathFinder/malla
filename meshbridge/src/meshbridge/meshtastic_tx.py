"""Meshtastic TX via Malla bot HTTP API."""

from __future__ import annotations

import logging

import aiohttp

from meshbridge.config import MeshtasticConfig

logger = logging.getLogger(__name__)


class MeshtasticBotSender:
    """POST text to Malla /api/bot/send for the MeshCore channel index."""

    def __init__(self, config: MeshtasticConfig, *, dry_run: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send(self, text: str) -> bool:
        if self.dry_run:
            logger.info(
                "[dry-run] Meshtastic TX channel_index=%s text=%r",
                self.config.channel_index,
                text,
            )
            return True

        if self._session is None:
            await self.start()
        assert self._session is not None

        body = {
            "text": text,
            "destination": "broadcast",
            "channel_index": self.config.channel_index,
            "priority": "normal",
        }
        try:
            async with self._session.post(self.config.malla_bot_url, json=body) as resp:
                payload = await resp.json(content_type=None)
                if resp.status >= 400 or not payload.get("success", True):
                    logger.error(
                        "Malla bot send failed status=%s body=%s", resp.status, payload
                    )
                    return False
                logger.info(
                    "Meshtastic TX queued via Malla (queue_size=%s): %r",
                    payload.get("queue_size"),
                    text[:80],
                )
                return True
        except Exception:  # noqa: BLE001
            logger.exception("Malla bot send error")
            return False
