"""Loop prevention: prefixes, self-echo ignore, fingerprint TTL cache."""

from __future__ import annotations

import hashlib
import logging
import time
from collections import OrderedDict

from meshbridge.models import BridgeMessage, Direction

logger = logging.getLogger(__name__)


class Deduper:
    """Decide whether an inbound bridge message should be forwarded."""

    def __init__(
        self,
        *,
        prefix_from_meshcore: str = "[MC]",
        prefix_from_meshtastic: str = "[MT]",
        gateway_node_id: str = "",
        companion_pubkey: str = "",
        fingerprint_ttl_sec: float = 120.0,
        max_entries: int = 2048,
    ) -> None:
        self.prefix_from_meshcore = prefix_from_meshcore.strip()
        self.prefix_from_meshtastic = prefix_from_meshtastic.strip()
        self.gateway_node_id = _normalize_node_id(gateway_node_id)
        self.companion_pubkey = companion_pubkey.strip().lower()
        self.fingerprint_ttl_sec = fingerprint_ttl_sec
        self.max_entries = max_entries
        self._seen: OrderedDict[str, float] = OrderedDict()

    def should_forward(self, msg: BridgeMessage) -> bool:
        """Return True if this inbound message should be bridged outbound."""
        text = (msg.text or "").strip()
        if not text:
            logger.debug("Drop empty text")
            return False

        if msg.direction == Direction.MESHTASTIC_TO_MESHCORE:
            # Already tagged as coming from MeshCore → do not bounce back
            if _starts_with_prefix(text, self.prefix_from_meshcore):
                logger.debug("Drop MT inbound already tagged %s", self.prefix_from_meshcore)
                return False
            if self.gateway_node_id and _normalize_node_id(msg.source_id) == self.gateway_node_id:
                logger.debug("Drop self-echo from Meshtastic gateway node %s", msg.source_id)
                return False
        else:
            if _starts_with_prefix(text, self.prefix_from_meshtastic):
                logger.debug(
                    "Drop MC inbound already tagged %s", self.prefix_from_meshtastic
                )
                return False
            if self.companion_pubkey and msg.source_id:
                src = msg.source_id.strip().lower()
                if src.startswith(self.companion_pubkey) or self.companion_pubkey.startswith(
                    src
                ):
                    logger.debug("Drop self-echo from MeshCore companion %s", msg.source_id)
                    return False

        fp = self._fingerprint(msg)
        now = time.monotonic()
        self._purge(now)
        if fp in self._seen:
            logger.debug("Drop duplicate fingerprint %s", fp[:12])
            return False

        self._seen[fp] = now
        if len(self._seen) > self.max_entries:
            self._seen.popitem(last=False)
        return True

    def remember_outbound(self, direction: Direction, sender: str, text: str) -> None:
        """Record a message we are about to send so RX echo is suppressed."""
        # Store under the opposite direction so the echo matches inbound filter.
        echo_direction = (
            Direction.MESHCORE_TO_MESHTASTIC
            if direction == Direction.MESHTASTIC_TO_MESHCORE
            else Direction.MESHTASTIC_TO_MESHCORE
        )
        # Also store same-direction fingerprint of formatted outbound body.
        msg = BridgeMessage(direction=echo_direction, sender=sender, text=text)
        self._seen[self._fingerprint(msg)] = time.monotonic()
        # Fingerprint of the outbound formatted text as it will appear when echoed
        # is handled by prefix checks; keep normalized content too.
        content_key = hashlib.sha256(
            f"{direction.value}|{sender.strip().lower()}|{_normalize_text(text)}".encode()
        ).hexdigest()
        self._seen[content_key] = time.monotonic()

    def _fingerprint(self, msg: BridgeMessage) -> str:
        if msg.packet_id:
            return hashlib.sha256(
                f"pkt|{msg.direction.value}|{msg.packet_id}".encode()
            ).hexdigest()
        payload = (
            f"{msg.direction.value}|{msg.sender.strip().lower()}|"
            f"{_normalize_text(msg.text)}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def _purge(self, now: float) -> None:
        cutoff = now - self.fingerprint_ttl_sec
        while self._seen:
            key, ts = next(iter(self._seen.items()))
            if ts >= cutoff:
                break
            self._seen.popitem(last=False)


def _starts_with_prefix(text: str, prefix: str) -> bool:
    if not prefix:
        return False
    return text.startswith(prefix) or text.upper().startswith(prefix.upper())


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().split()).lower()


def _normalize_node_id(node_id: str) -> str:
    value = (node_id or "").strip().lower()
    if value.startswith("!"):
        value = value[1:]
    if value.startswith("0x"):
        value = value[2:]
    return value
