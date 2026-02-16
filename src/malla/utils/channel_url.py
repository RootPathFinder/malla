"""
Meshtastic Channel URL Generator.

Generates clickable ``https://meshtastic.org/e/#…`` URLs from a channel
name and PSK so that users can add channels to their radios with a single
tap.
"""

import base64
import logging

logger = logging.getLogger(__name__)

# The well-known default PSK (1 byte: 0x01) used by Meshtastic for
# the "LongFast" primary channel.
_DEFAULT_PSK_BYTES = b"\x01"

# Valid range for Meshtastic channel indices (0 = primary, 1-7 = secondary).
_MIN_CHANNEL_INDEX = 0
_MAX_CHANNEL_INDEX = 7


def generate_channel_url(
    channel_name: str,
    psk_base64: str = "AQ==",
    channel_index: int = 1,
) -> str | None:
    """Build a Meshtastic channel URL from a channel name and PSK.

    The URL encodes a ``ChannelSet`` protobuf in base64url format.
    The ``ChannelSet.settings`` array maps positionally to channel
    indices: ``settings[0]`` = primary (index 0), ``settings[1]`` =
    secondary (index 1), etc.

    The Meshtastic apps (iOS / Android) treat the URL as a *complete*
    channel configuration: every slot present in the array is applied
    and every slot **beyond** the array is **disabled**.  That means a
    URL with only one entry in ``settings`` would replace the user's
    primary channel and **wipe channels 1-7**.

    To avoid this, lower-indexed slots are filled with the default
    primary channel (slot 0: empty name + PSK ``0x01`` = "LongFast")
    or empty placeholder channels (slots 1+) so that existing channels
    in those positions are not clobbered.

    We intentionally omit ``lora_config`` so LoRa / region settings
    are not touched.

    Args:
        channel_name: Human-readable channel name (< 12 bytes).
        psk_base64: Pre-shared key encoded as standard Base64.
            ``"AQ=="`` is the well-known default key.
        channel_index: Target channel slot (0-7).  Defaults to **1**
            (first secondary slot) which is the safest choice for most
            users.  Slot 0 replaces the primary channel.

    Returns:
        The full ``https://meshtastic.org/e/#…`` URL, or *None* if the
        meshtastic protobuf package is unavailable or the index is
        invalid.
    """
    try:
        if not (_MIN_CHANNEL_INDEX <= channel_index <= _MAX_CHANNEL_INDEX):
            logger.warning(
                "channel_index %d out of range [%d, %d]",
                channel_index,
                _MIN_CHANNEL_INDEX,
                _MAX_CHANNEL_INDEX,
            )
            return None

        from meshtastic.protobuf import apponly_pb2, channel_pb2  # noqa: F401

        channel_set = apponly_pb2.ChannelSet()

        # Fill slots 0 … channel_index-1 with safe defaults so the app
        # does not overwrite existing channels in those positions.
        for i in range(channel_index):
            placeholder = channel_pb2.ChannelSettings()
            if i == 0:
                # Slot 0 = primary channel.  Use default PSK (0x01) so the
                # user's standard "LongFast" primary is preserved.
                placeholder.psk = _DEFAULT_PSK_BYTES
            else:
                # Slots 1+ : empty ChannelSettings entries are treated by
                # the app as "no change" / disabled-if-was-disabled.  We
                # use the default PSK so they remain harmless placeholders.
                placeholder.psk = _DEFAULT_PSK_BYTES
            channel_set.settings.append(placeholder)

        # The actual channel the user wants to share.
        target = channel_pb2.ChannelSettings()
        target.name = channel_name
        target.psk = base64.b64decode(psk_base64)
        channel_set.settings.append(target)

        # Omit lora_config so LoRa / region settings are not replaced.
        channel_set.ClearField("lora_config")

        # Serialise → base64url (strip padding for shorter URLs)
        proto_bytes = channel_set.SerializeToString()
        url_fragment = base64.urlsafe_b64encode(proto_bytes).decode("ascii").rstrip("=")
        return f"https://meshtastic.org/e/#{url_fragment}"

    except Exception:
        logger.debug("Could not generate channel URL", exc_info=True)
        return None
