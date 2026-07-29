import pytest

from meshbridge.bridge import ChannelBridge
from meshbridge.config import (
    AppConfig,
    BridgeConfig,
    MeshcoreConfig,
    MeshtasticConfig,
)
from meshbridge.models import BridgeMessage, Direction


@pytest.fixture
def config() -> AppConfig:
    return AppConfig(
        dry_run=True,
        meshtastic=MeshtasticConfig(
            mqtt_broker="127.0.0.1",
            channel_name="MeshCore",
            channel_key="AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
            channel_index=1,
            malla_bot_url="http://127.0.0.1:9/api/bot/send",
            gateway_node_id="!deadbeef",
        ),
        meshcore=MeshcoreConfig(
            companion_serial="",
            channel_index=1,
            channel_name="#meshtastic",
            mqtt_broker="",
        ),
        bridge=BridgeConfig(min_send_interval_sec=0.0),
    )


@pytest.mark.asyncio
async def test_bridge_mt_to_mc_dry_run(config: AppConfig):
    bridge = ChannelBridge(config)
    # Skip network start; exercise handle_inbound only
    msg = BridgeMessage(
        direction=Direction.MESHTASTIC_TO_MESHCORE,
        sender="!11223344",
        text="hello from MT",
        source_id="!11223344",
        packet_id="1",
    )
    await bridge.handle_inbound(msg)
    assert bridge.stats["mt_in"] == 1
    assert bridge.stats["mc_out"] == 1
    assert bridge.stats["dropped"] == 0


@pytest.mark.asyncio
async def test_bridge_drops_loop_prefix(config: AppConfig):
    bridge = ChannelBridge(config)
    msg = BridgeMessage(
        direction=Direction.MESHTASTIC_TO_MESHCORE,
        sender="!11223344",
        text="[MC] alice: already bridged",
        source_id="!11223344",
        packet_id="2",
    )
    await bridge.handle_inbound(msg)
    assert bridge.stats["dropped"] == 1
    assert bridge.stats["mc_out"] == 0


@pytest.mark.asyncio
async def test_bridge_mc_to_mt_dry_run(config: AppConfig):
    bridge = ChannelBridge(config)
    msg = BridgeMessage(
        direction=Direction.MESHCORE_TO_MESHTASTIC,
        sender="Repeater1",
        text="hello from MC",
        source_id="Repeater1",
        packet_id="abc",
    )
    await bridge.handle_inbound(msg)
    assert bridge.stats["mc_in"] == 1
    assert bridge.stats["mt_out"] == 1


@pytest.mark.asyncio
async def test_bridge_drops_gateway_echo(config: AppConfig):
    bridge = ChannelBridge(config)
    msg = BridgeMessage(
        direction=Direction.MESHTASTIC_TO_MESHCORE,
        sender="!deadbeef",
        text="echo",
        source_id="!deadbeef",
        packet_id="9",
    )
    await bridge.handle_inbound(msg)
    assert bridge.stats["dropped"] == 1
