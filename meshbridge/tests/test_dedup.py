from meshbridge.dedup import Deduper
from meshbridge.models import BridgeMessage, Direction


def _msg(
    direction: Direction,
    text: str,
    sender: str = "alice",
    source_id: str = "",
    packet_id: str = "",
) -> BridgeMessage:
    return BridgeMessage(
        direction=direction,
        sender=sender,
        text=text,
        source_id=source_id or sender,
        packet_id=packet_id,
    )


def test_drops_opposite_prefix_mt_to_mc():
    d = Deduper()
    assert not d.should_forward(
        _msg(Direction.MESHTASTIC_TO_MESHCORE, "[MC] bob: hello")
    )


def test_drops_opposite_prefix_mc_to_mt():
    d = Deduper()
    assert not d.should_forward(
        _msg(Direction.MESHCORE_TO_MESHTASTIC, "[MT] !aa: hello")
    )


def test_allows_fresh_message():
    d = Deduper()
    assert d.should_forward(_msg(Direction.MESHTASTIC_TO_MESHCORE, "hello mesh"))


def test_fingerprint_dedup():
    d = Deduper()
    m = _msg(Direction.MESHCORE_TO_MESHTASTIC, "same text", packet_id="abc123")
    assert d.should_forward(m)
    assert not d.should_forward(m)


def test_self_echo_gateway_node():
    d = Deduper(gateway_node_id="!aabbccdd")
    assert not d.should_forward(
        _msg(
            Direction.MESHTASTIC_TO_MESHCORE,
            "ping",
            source_id="!aabbccdd",
        )
    )


def test_self_echo_companion_pubkey():
    d = Deduper(companion_pubkey="deadbeef")
    assert not d.should_forward(
        _msg(
            Direction.MESHCORE_TO_MESHTASTIC,
            "ping",
            source_id="deadbeef01",
        )
    )


def test_empty_dropped():
    d = Deduper()
    assert not d.should_forward(_msg(Direction.MESHTASTIC_TO_MESHCORE, "   "))
