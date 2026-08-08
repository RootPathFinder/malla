# meshbridge

Bidirectional **channel text** gateway between Meshtastic and MeshCore.

| Side | Channel | Role |
|------|---------|------|
| MeshCore | `#meshtastic` | Hashtag channel (`SHA256("#meshtastic")[:16]`) |
| Meshtastic | `MeshCore` | Secondary encrypted channel (your PSK) |

This package lives under the Malla repo as a **standalone installable service**. It does not modify Malla or CoreScope runtime code. Malla’s bot API is used for Meshtastic TX; CoreScope remains a read-only monitor (optionally show `#meshtastic` in its UI via `channelKeys`).

```
Meshtastic nodes → MQTT → meshbridge → MeshCore companion → MeshCore #meshtastic
MeshCore nodes   → MQTT/companion → meshbridge → Malla /api/bot/send → Meshtastic MeshCore ch
```

## Prerequisites

### Meshtastic / Malla

1. Create a secondary channel named **`MeshCore`** with a dedicated PSK on the gateway node (and any nodes that should participate).
2. Note the channel’s **list index** on that node (often `1`).
3. Run Malla web with the bot **enabled**, **running**, and connected via **TCP or serial** (bot does not send chat over MQTT).
4. Point `meshtastic.malla_bot_url` at `http://<malla-host>:5008/api/bot/send` (prefer localhost / ACL — the bot send route is unauthenticated).
5. Feed the same Meshtastic MQTT broker into `meshtastic.mqtt_*` that `malla-capture` uses.

### MeshCore / CoreScope

1. Companion radio on USB (or BLE via meshcore_py) with a channel slot set to **`#meshtastic`**.
2. Configure that slot with name `#meshtastic` and secret = `SHA256("#meshtastic")[:16]` (meshcore_py `set_channel` accepts the 16-byte secret).
3. Optional wide RX: subscribe to the same observer MQTT topics CoreScope’s ingestor uses (`meshcore/+/+/packets`).
4. Optional UI: add `#meshtastic` to CoreScope `channelKeys` so bridged traffic appears in Channels.

### Network / hardware

- Host can reach both MQTT brokers and Malla HTTP.
- For MeshCore **TX**, a companion on this host is required. Without it, only MeshCore→Meshtastic (MQTT RX → Malla TX) is possible.

## Install

```bash
cd meshbridge
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,meshcore]"
cp config.sample.yaml config.yaml
# edit config.yaml
```

## Configure

See [`config.sample.yaml`](config.sample.yaml). Important fields:

- `dry_run: true` — log would-send actions without TX (start here).
- `meshtastic.channel_name` / `channel_key` / `channel_index`
- `meshtastic.malla_bot_url` / `gateway_node_id`
- `meshcore.companion_serial` / `channel_index` / `channel_name`
- `meshcore.mqtt_broker` — optional Format-1 observer RX

### Message format & loop prevention

- MeshCore → Meshtastic: `[MC] {sender}: {message}`
- Meshtastic → MeshCore: `[MT] {shortname|nodeid}: {message}`
- Drops opposite-prefix echoes (`[MC]` on MT→MC, `[MT]` on MC→MT), companion self-echo, and short-TTL fingerprints.
- Gateway-node traffic is **not** blanket-dropped so Malla bot replies on the MeshCore channel can reach MeshCore users.
- Chunks at ~133 chars (MeshCore) and ~228 (Meshtastic).

## Run

```bash
meshbridge -c config.yaml
# or
python -m meshbridge -c config.yaml
```

Docker (companion device passthrough):

```bash
docker compose up --build
```

## Tests

```bash
cd meshbridge
pip install -e ".[dev]"
pytest -q
```

## Non-goals (v1)

- No DM / position / telemetry bridging
- No RF TX through CoreScope HTTP
- No Meshtastic chat publish via MQTT admin publisher
