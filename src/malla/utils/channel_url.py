"""
Meshtastic Channel URL Generator.

Generates clickable ``https://meshtastic.org/e/#…`` URLs from a channel
name and PSK so that users can add channels to their radios with a single
tap.
"""

import base64
import logging

logger = logging.getLogger(__name__)


def generate_channel_url(
    channel_name: str,
    psk_base64: str = "AQ==",
) -> str | None:
    """Build a Meshtastic channel URL from a channel name and PSK.

    The URL encodes a ``ChannelSet`` protobuf (channel settings + LoRa
    config) in base64url format.  Opening the link on a phone with the
    Meshtastic app (or the web client) lets the user add the channel.

    Args:
        channel_name: Human-readable channel name (< 12 bytes).
        psk_base64: Pre-shared key encoded as standard Base64.
            ``"AQ=="`` is the well-known default key.

    Returns:
        The full ``https://meshtastic.org/e/#…`` URL, or *None* if the
        meshtastic protobuf package is unavailable.
    """
    try:
        from meshtastic.protobuf import apponly_pb2, channel_pb2  # noqa: F401

        channel_set = apponly_pb2.ChannelSet()

        # Build channel settings
        ch_settings = channel_pb2.ChannelSettings()
        ch_settings.name = channel_name
        ch_settings.psk = base64.b64decode(psk_base64)
        channel_set.settings.append(ch_settings)

        # Default LoRa preset (LONG_FAST = 0).  Since 0 is the proto
        # default it won't add extra bytes to the serialisation—keeping
        # the URL short.  ``use_preset = True`` tells the firmware to
        # apply the preset rather than raw LoRa params.
        channel_set.lora_config.use_preset = True

        # Serialise → base64url (strip padding for shorter URLs)
        proto_bytes = channel_set.SerializeToString()
        url_fragment = base64.urlsafe_b64encode(proto_bytes).decode("ascii").rstrip("=")
        return f"https://meshtastic.org/e/#{url_fragment}"

    except Exception:
        logger.debug("Could not generate channel URL", exc_info=True)
        return None
