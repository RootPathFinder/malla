"""MeshCore TX via companion radio (meshcore_py)."""

from __future__ import annotations

import logging
from typing import Any

from meshbridge.config import MeshcoreConfig

logger = logging.getLogger(__name__)


class MeshcoreCompanionSender:
    """Send channel text with meshcore_py send_chan_msg."""

    def __init__(self, config: MeshcoreConfig, *, dry_run: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run
        self._mesh: Any | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.config.companion_serial) or self.dry_run

    async def start(self) -> None:
        if self.dry_run or not self.config.companion_serial:
            logger.info(
                "MeshCore companion TX %s",
                "dry-run" if self.dry_run else "disabled (no companion_serial)",
            )
            return
        try:
            from meshcore import MeshCore  # type: ignore[import-untyped]
        except ImportError as exc:
            raise RuntimeError(
                "meshcore package required for companion TX. "
                "Install with: pip install 'meshbridge[meshcore]'"
            ) from exc

        self._mesh = await MeshCore.create_serial(self.config.companion_serial)
        logger.info(
            "MeshCore companion connected on %s (channel_index=%s)",
            self.config.companion_serial,
            self.config.channel_index,
        )

    async def stop(self) -> None:
        if self._mesh is None:
            return
        try:
            await self._mesh.disconnect()
        except Exception:  # noqa: BLE001
            logger.exception("Error disconnecting MeshCore companion")
        self._mesh = None

    async def send(self, text: str) -> bool:
        if self.dry_run:
            logger.info(
                "[dry-run] MeshCore TX channel_index=%s text=%r",
                self.config.channel_index,
                text,
            )
            return True
        if self._mesh is None:
            logger.error("MeshCore companion not connected; cannot TX")
            return False
        try:
            result = await self._mesh.commands.send_chan_msg(
                self.config.channel_index, text
            )
            event_type = getattr(result, "type", None)
            name = getattr(event_type, "name", str(event_type))
            if name and "ERROR" in str(name).upper():
                logger.error("MeshCore send_chan_msg error: %s", result)
                return False
            logger.info("MeshCore TX ok: %r", text[:80])
            return True
        except Exception:  # noqa: BLE001
            logger.exception("MeshCore send_chan_msg failed")
            return False

    @property
    def mesh(self) -> Any | None:
        return self._mesh
