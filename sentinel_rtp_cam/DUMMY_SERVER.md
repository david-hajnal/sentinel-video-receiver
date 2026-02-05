# Dummy Server for Testing

A lightweight HTTP server for testing the Sentinel agent integration locally without needing the production VPS server.

## Features

- **SSE Config Stream**: Server-Sent Events endpoint for dynamic configuration updates
- **Event Endpoints**: Accepts motion events, heartbeat, and clip metadata
- **Admin UI**: Web-based config editor with live broadcast to connected agents
- **Bearer Token Auth**: Simple authentication matching the agent's expectations
- **In-Memory Storage**: No database required - events are logged and deduplicated in memory

## Quick Start

### 1. Start the Server

```bash
# From sentinel_rtp_cam directory
cargo run --bin dummy_server

# Or with custom settings
DUMMY_SERVER_BIND=0.0.0.0:8080 \
DUMMY_SERVER_TOKEN=my-secret-token \
cargo run --bin dummy_server
```

**Default settings:**
- Bind address: `127.0.0.1:8080`
- Bearer token: `test-token-12345`

### 2. Configure the Agent

Update your `.env` file:

```env
# Server integration
MOTION_ENABLED=true
SERVER_ENABLED=true
SERVER_BASE_URL=http://127.0.0.1:8080
SERVER_BEARER_TOKEN=test-token-12345
SERVER_RETRY_INTERVAL_SECS=30

# Camera identification
CAMERA_ID=cam-test-001
```

### 3. Start the Agent

```bash
cargo run --bin app
```

The agent will automatically:
- Connect to the SSE config stream
- Send motion events when detected
- Post heartbeat every 30 seconds
- Upload clip metadata when recordings complete

## Endpoints

### Admin Interface

**`GET /admin`** - Web UI for editing configuration

Open http://127.0.0.1:8080/admin in your browser to:
- View current config JSON
- Edit and update configuration
- Changes broadcast immediately to all connected agents via SSE

### API Endpoints (Require Bearer Token)

**`GET /api/v1/config/stream?camera_id=CAM_ID`** - SSE config stream
- Sends current config immediately on connect
- Broadcasts config updates as they happen
- Keeps connection alive with periodic keepalive messages

**`POST /api/events/motion`** - Motion event endpoint
```json
{
  "camera_id": "cam-001",
  "event_id": "01HMQK3V2F5T8N9ZGXJ4YK7R3E",
  "rule": "CellMotionDetector",
  "active": true,
  "timestamp": "2026-01-17T10:30:45.123Z"
}
```

**`POST /api/heartbeat`** - Heartbeat endpoint
```json
{
  "camera_id": "cam-001",
  "timestamp": "2026-01-17T10:30:45.123Z"
}
```

**`POST /api/clips`** - Clip metadata endpoint
```json
{
  "camera_id": "cam-001",
  "event_id": "01HMQK3V2F5T8N9ZGXJ4YK7R3E",
  "rule": "CellMotionDetector",
  "file_path": "clips/20260117_103045.123Z_cam-001_01HMQK3V2F5T8N9ZGXJ4YK7R3E_CellMotionDetector.mp4",
  "file_size": 1234567,
  "started_at": "2026-01-17T10:30:45.123Z",
  "ended_at": "2026-01-17T10:30:55.456Z",
  "duration_secs": 10
}
```

## Testing Workflow

### 1. Verify SSE Connection

```bash
# Watch server logs for:
Client connected to SSE config stream
```

### 2. Test Motion Events

Trigger motion on your camera, watch for:
```
Motion event received camera_id=cam-001 event_id=01HM... rule=CellMotionDetector active=true
```

### 3. Update Config via Admin

1. Open http://127.0.0.1:8080/admin
2. Edit the JSON (e.g., change `motion.enabled` to `false`)
3. Submit - agent receives update immediately via SSE

### 4. Monitor Heartbeat

Every 30 seconds you should see:
```
Heartbeat received camera_id=cam-001
```

### 5. Check Clip Uploads

After a recording completes:
```
Clip metadata received camera_id=cam-001 event_id=01HM... file_size_mb=1 duration_secs=10
```

## Event Deduplication

The server tracks seen events using `(camera_id, event_id, active)` tuples to avoid logging duplicates from retry logic.

First occurrence: Logged with full details
Duplicate: `Duplicate motion event ignored`

## Architecture

```
┌─────────────────────────────────────────┐
│          Dummy Server (Port 8080)       │
├─────────────────────────────────────────┤
│                                         │
│  Admin UI (/admin)                      │
│    ↓                                    │
│  Config Store (RwLock<String>)          │
│    ↓                                    │
│  Broadcast Channel                      │
│    ↓                                    │
│  SSE Stream (/api/v1/config/stream)        │
│    ↓                                    │
│  Connected Agents (multiple)            │
│                                         │
│  API Endpoints:                         │
│  • POST /api/events/motion              │
│  • POST /api/heartbeat                  │
│  • POST /api/clips                      │
│    ↓                                    │
│  In-Memory Event Store + Dedupe         │
│                                         │
└─────────────────────────────────────────┘
```

## Troubleshooting

### Agent can't connect to SSE

- Check `SERVER_BASE_URL` matches server bind address
- Verify bearer token matches between agent and server
- Check server logs for connection attempts

### Motion events not appearing

- Verify `MOTION_ENABLED=true` in agent config
- Check agent logs for HTTP errors
- Confirm bearer token is correct

### Config updates not propagating

- Check browser console for admin page errors
- Verify JSON is valid before submitting
- Watch server logs for broadcast confirmation

## Production Differences

This dummy server is for **local testing only**. Production differences:

| Feature | Dummy Server | Production |
|---------|-------------|------------|
| Persistence | In-memory | PostgreSQL |
| Auth | Simple bearer token | JWT with expiry |
| Config | Manual admin page | Database-backed |
| Scalability | Single instance | Load-balanced |
| Monitoring | Console logs | Structured logging + metrics |
| File Upload | Not implemented | S3/object storage |

## Environment Variables

```env
# Dummy server configuration
DUMMY_SERVER_BIND=127.0.0.1:8080  # Server bind address
DUMMY_SERVER_TOKEN=test-token-12345  # Bearer token for API auth
```

## Development Tips

### Watch Server Logs

```bash
RUST_LOG=debug cargo run --bin dummy_server
```

### Test SSE with curl

```bash
curl -N -H "Accept: text/event-stream" \
  "http://127.0.0.1:8080/api/v1/config/stream?camera_id=cam-001"
```

### Manual Event Posting

```bash
curl -X POST http://127.0.0.1:8080/api/events/motion \
  -H "Authorization: Bearer test-token-12345" \
  -H "Content-Type: application/json" \
  -d '{
    "camera_id": "cam-001",
    "event_id": "test-event-001",
    "rule": "TestRule",
    "active": true,
    "timestamp": "2026-01-17T10:00:00Z"
  }'
```

### Concurrent Agent Testing

Start multiple agents with different camera IDs to test multi-camera scenarios:

```bash
# Terminal 1
CAMERA_ID=cam-001 cargo run --bin app

# Terminal 2
CAMERA_ID=cam-002 cargo run --bin app
```

Both will appear in the dummy server logs with their respective camera IDs.
