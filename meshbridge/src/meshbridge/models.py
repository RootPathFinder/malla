"""Shared message models for the bridge."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Direction(str, Enum):
    MESHTASTIC_TO_MESHCORE = "mt_to_mc"
    MESHCORE_TO_MESHTASTIC = "mc_to_mt"


@dataclass(frozen=True, slots=True)
class BridgeMessage:
    """Normalized inbound text ready for bridging."""

    direction: Direction
    sender: str
    text: str
    source_id: str = ""
    packet_id: str = ""
