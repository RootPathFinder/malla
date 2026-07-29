"""CLI entrypoint: python -m meshbridge"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from meshbridge.bridge import ChannelBridge
from meshbridge.config import load_config


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


async def _run(config_path: str | None) -> int:
    config = load_config(config_path)
    _setup_logging(config.log_level)
    logger = logging.getLogger("meshbridge")

    bridge = ChannelBridge(config)
    stop_event = asyncio.Event()

    def _request_stop(*_args: object) -> None:
        logger.info("Shutdown requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            # Windows / limited environments
            signal.signal(sig, lambda *_: _request_stop())

    await bridge.start()
    logger.info("meshbridge running (Ctrl+C to stop)")
    await stop_event.wait()
    await bridge.stop()
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Bidirectional Meshtastic ↔ MeshCore channel gateway"
    )
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to config.yaml (default: ./config.yaml or MESHBRIDGE_CONFIG)",
    )
    args = parser.parse_args(argv)
    try:
        raise SystemExit(asyncio.run(_run(args.config)))
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
