from pathlib import Path

from meshbridge.config import load_config


def test_load_sample_config(tmp_path: Path):
    sample = Path(__file__).resolve().parents[1] / "config.sample.yaml"
    cfg = load_config(sample)
    assert cfg.dry_run is True
    assert cfg.meshtastic.channel_name == "MeshCore"
    assert cfg.meshcore.channel_name == "#meshtastic"
    assert cfg.bridge.max_meshcore_chars == 133
