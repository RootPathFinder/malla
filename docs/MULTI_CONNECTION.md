# Multi-Connection Support

## Overview

Malla now supports multiple simultaneous connections to Meshtastic nodes, each with designated roles for different purposes.

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
Used for basic mesh activities and read operations:
- Chat interface
- Traceroute operations
- General mesh monitoring
- Node discovery
- Packet monitoring
- Bot activities (automated mesh interactions)

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
    description: "Local USB node for bot interactions"
```

**Benefits:**
- Admin operations don't interfere with bot activities
- Bot can run continuously while admin performs configuration changes
- Separate devices can be used for different security domains

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
