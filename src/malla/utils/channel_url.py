"""
Meshtastic Channel URL Generator.

Generates clickable ``https://meshtastic.org/e/?add=true#…`` URLs from a
channel name and PSK so that users can **add** a channel to their radios
without replacing LongFast / existing channels.
"""

import base64
import logging

logger = logging.getLogger(__name__)

# Valid range for Meshtastic channel indices (0 = primary, 1-7 = secondary).
# Only used when generating replace-mode URLs.
_MIN_CHANNEL_INDEX = 0
_MAX_CHANNEL_INDEX = 7

# The well-known default PSK (1 byte: 0x01) used by Meshtastic for
# the "LongFast" primary channel.
_DEFAULT_PSK_BYTES = b"\x01"


def generate_channel_url(
    channel_name: str,
    psk_base64: str = "AQ==",
    channel_index: int = 1,
    *,
    add: bool = True,
) -> str | None:
    """Build a Meshtastic channel URL from a channel name and PSK.

    By default this produces an **add-mode** URL:

        ``https://meshtastic.org/e/?add=true#{fragment}``

    Clients that honor ``add=true`` append the named channel into the next
    free secondary slot and leave LongFast / other existing channels alone.

    The URL encodes a ``ChannelSet`` protobuf in base64url format.
    For add-mode we encode **only** the shared channel (no placeholder
    slots and no ``lora_config``).

    Args:
        channel_name: Human-readable channel name (< 12 bytes).
        psk_base64: Pre-shared key encoded as standard Base64.
            ``"AQ=="`` is the well-known default key.
        channel_index: Target channel slot (0-7).  Only used when
            ``add=False`` (replace mode).  Ignored for add-mode URLs
            because the client chooses the next free slot.
        add: When True (default), emit an add-only URL.  When False,
            emit a replace-config URL that overwrites the device's
            channel set (destructive — use with care).

    Returns:
        The full Meshtastic channel URL, or *None* if the meshtastic
        protobuf package is unavailable or the index is invalid.
    """
    try:
        from meshtastic.protobuf import apponly_pb2, channel_pb2

        channel_set = apponly_pb2.ChannelSet()

        if add:
            # Add-mode: encode only the channel being shared. Clients
            # place it in the next free secondary slot.
            target = channel_pb2.ChannelSettings()
            target.name = channel_name
            target.psk = base64.b64decode(psk_base64)
            channel_set.settings.append(target)
        else:
            if not (_MIN_CHANNEL_INDEX <= channel_index <= _MAX_CHANNEL_INDEX):
                logger.warning(
                    "channel_index %d out of range [%d, %d]",
                    channel_index,
                    _MIN_CHANNEL_INDEX,
                    _MAX_CHANNEL_INDEX,
                )
                return None

            # Replace-mode: positional settings overwrite the device set.
            # Pad lower slots with LongFast defaults so slot 0 is not blank.
            for _ in range(channel_index):
                placeholder = channel_pb2.ChannelSettings()
                placeholder.psk = _DEFAULT_PSK_BYTES
                channel_set.settings.append(placeholder)

            target = channel_pb2.ChannelSettings()
            target.name = channel_name
            target.psk = base64.b64decode(psk_base64)
            channel_set.settings.append(target)

        # Never include lora_config — region / modem preset must stay local.
        channel_set.ClearField("lora_config")

        proto_bytes = channel_set.SerializeToString()
        url_fragment = base64.urlsafe_b64encode(proto_bytes).decode("ascii").rstrip("=")

        if add:
            return f"https://meshtastic.org/e/?add=true#{url_fragment}"
        return f"https://meshtastic.org/e/#{url_fragment}"

    except Exception:
        logger.debug("Could not generate channel URL", exc_info=True)
        return None
