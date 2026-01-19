# Multi-Connection Support

## Overview

Malla now supports multiple simultaneous connections to Meshtastic nodes, each with designated roles for different purposes.

**Important**: Multi-connection support is for **active bidirectional communication** (sending messages, commands). It complements but does not replace the MQTT capture process (`malla-capture`), which passively monitors all mesh traffic for database storage.

## Connection Roles

### Admin Connection
Used for administrative operations that modify node configuration or perform sensitive actions:
- Device backups and restores
- Compliance monitoring
- Configuration changes (device, position, power, network, LoRa, etc.)
- Module configuration (MQTT, serial, telemetry, etc.)
- Channel management
- Node management (reboot, shutdown)
- Security configuration

### Client Connection
Used for basic mesh activities and **sending** operations:
- Sending chat messages to the mesh
- Initiating traceroute requests
- Bot activities (automated mesh interactions)
- Sending position updates
- Publishing telemetry data

**Note**: Client connections are for **active participation** (sending). For **passive monitoring** (receiving all mesh traffic), continue using `malla-capture` with MQTT.

## MQTT Capture vs Active Connections

### MQTT Capture (`malla-capture`)
- **Purpose**: Passive monitoring and data collection
- **Direction**: Receive-only (listens to mesh traffic)
- **Scope**: All mesh packets (entire network)
- **Data storage**: Writes to SQLite database
- **Use case**: Historical analysis, maps, node tracking, packet browser
- **Process**: Runs as separate background service

### Active Connections (Client/Admin)
- **Purpose**: Active mesh participation
- **Direction**: Bidirectional (send and receive)
- **Scope**: Your node's direct communication
- **Data storage**: Does not write to database (use MQTT capture for that)
- **Use case**: Sending chat, bot commands, admin operations
- **Process**: Integrated with web UI

### Recommended Setup

For comprehensive mesh interaction:

1. **Run `malla-capture`** for passive monitoring (required for web UI data):
   ```bash
   malla-capture  # Captures all MQTT traffic to database
   ```

2. **Configure client connection** for active participation (optional):
   ```yaml
   connections:
     - id: "bot_serial"
       type: "serial"
       role: "client"
       port: "/dev/ttyUSB0"
       description: "Local USB node for chat/bot operations"
   ```

This setup gives you:
- Complete mesh visibility via MQTT (passive monitoring)
- Ability to send messages via client connection (active participation)
- No duplicate packet capture (MQTT handles receiving, client handles sending)

## Configuration

### Legacy Single-Connection Mode (Backward Compatible)

The traditional configuration still works for simple deployments:

```yaml
admin_enabled: true
admin_connection_type: "tcp"  # or "serial" or "mqtt"
admin_tcp_host: "192.168.1.1"
admin_tcp_port: 4403
```

### Multi-Connection Mode

Define multiple connections with specific roles:

```yaml
connections:
  - id: "admin_tcp"
    type: "tcp"
    role: "admin"
    description: "TCP connection for administrative operations"
    host: "192.168.1.1"
    port: 4403
    auto_connect: true
    
  - id: "client_serial"
    type: "serial"
    role: "client"
    description: "USB connection for client/bot operations"
    port: "/dev/ttyUSB0"
    auto_connect: true
```

### Connection Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `id` | Yes | Unique identifier for this connection |
| `type` | Yes | Connection type: "tcp", "serial", or "mqtt" |
| `role` | Yes | Connection role: "admin" or "client" |
| `description` | No | Human-readable description |
| `auto_connect` | No | Auto-connect on startup (default: true) |
| `host` | TCP only | IP address or hostname |
| `port` | TCP/Serial | Port number (TCP) or device path (Serial) |

## Use Cases

### Scenario 1: Separate Admin and Bot Connections

```yaml
# malla-capture runs separately for passive MQTT monitoring

connections:
  # Admin connection via TCP to a remote node
  - id: "remote_admin"
    type: "tcp"
    role: "admin"
    host: "192.168.1.100"
    port: 4403
    description: "Remote node for admin operations"
    
  # Client connection via USB for bot activities
  - id: "local_bot"
    type: "serial"
    role: "client"
    port: "/dev/ttyUSB0"
    description: "Local USB node for sending chat/bot messages"
```

**Benefits:**
- MQTT capture receives all mesh traffic (comprehensive monitoring)
- Admin operations via TCP don't interfere with bot activities
- Bot can send messages continuously via USB
- Clear separation: MQTT = receive, Client = send

## API Endpoints

### List All Connections
```
GET /api/admin/connections
```

### Get Specific Connection
```
GET /api/admin/connections/{connection_id}
```

### Connect/Disconnect
```
POST /api/admin/connections/{connection_id}/connect
POST /api/admin/connections/{connection_id}/disconnect
```

### Change Role
```
PUT /api/admin/connections/{connection_id}/role
{
  "role": "admin"  // or "client"
}
```

### Bulk Operations
```
POST /api/admin/connections/connect-all?role=admin
POST /api/admin/connections/disconnect-all?role=client
```

## Migration Guide

### From Single to Multi-Connection

**Before:**
```yaml
admin_connection_type: "tcp"
admin_tcp_host: "192.168.1.1"
admin_tcp_port: 4403
```

**After:**
```yaml
connections:
  - id: "primary"
    type: "tcp"
    role: "admin"
    host: "192.168.1.1"
    port: 4403
```

No code changes required! Full backward compatibility is maintained.
