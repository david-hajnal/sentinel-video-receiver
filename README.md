# Sentinel Video Receiver

RTSP/RTP receiver and forward agent for Raspberry Pi with ONVIF motion detection.

## Quick install (recommended)

Uses the tooling repo: `david-hajnal/sentinel-tooling`.

```bash
curl -fsSL https://raw.githubusercontent.com/david-hajnal/sentinel-tooling/main/init.sh -o /tmp/sentinel-init.sh
sudo bash /tmp/sentinel-init.sh
```

The installer drops `sentinel-manage` and runs `sentinel-manage init`.
When the wizard finishes, start the agent:

```bash
sudo sentinel-manage start
```

## Configuration

The agent reads JSON config files in `/etc/sentinel_rtp_cam`:

- `server.json` (admin server base URL + bearer token)
- `camera.json` (camera + forward config)

Use the manage tool (from `sentinel-tooling`) to edit them:

```bash
sudo sentinel-manage config server
sudo sentinel-manage config camera
```

If a server is configured, the agent will pull camera config from it.

## Local development with `sentinel-admin-server`

For end-to-end local live playback testing, run the admin server stack from the sibling repo and
point this agent at it. Do not use `server_ingest` from this repo for that flow; the compose
`ingest` service in `sentinel-admin-server` is the one serving the local backend pipeline.

### 1. Start the local admin stack

In `/Users/kaszperek/repos/sentinel-admin-server/server`:

```bash
docker compose up -d postgres admin-server ingest
```

Expected local endpoints:

- admin API: `http://localhost:8080`
- TLS ingest uplink: `localhost:9000`

### 2. Create an agent and camera in the local admin server

Use the management UI or API. The important values you need from the admin server are:

- `agent token` from `POST /api/management/agents`
- `agents.id`
- canonical camera `camera_id` from the `cameras` table/API

Use those exact values in the local JSON files. Do not substitute:

- a device/camera bearer token for the agent token
- a display label such as `pi-cam-001` for the canonical `camera_id`
- the camera row `id` for the canonical `camera_id`

### 3. Install the ingest CA on the local machine

The agent TLS uplink loads the CA from `/etc/sentinel_rtp_cam/ca.crt` by default.

```bash
sudo mkdir -p /etc/sentinel_rtp_cam
sudo install -m 0644 \
  /Users/kaszperek/repos/sentinel-admin-server/tls_ingest/certs/ca.crt \
  /etc/sentinel_rtp_cam/ca.crt
```

If the ingest certificate was issued for `server.local`, add a local hosts entry:

```bash
echo '127.0.0.1 server.local' | sudo tee -a /etc/hosts
```

Then use `server.local:9000` in `camera.json`.

### 4. Write local JSON config

`/etc/sentinel_rtp_cam/server.json`

```json
{
  "server": {
    "enabled": true,
    "base_url": "http://localhost:8080",
    "bearer_token": "PUT_AGENT_TOKEN_HERE"
  }
}
```

`/etc/sentinel_rtp_cam/camera.json`

```json
{
  "cameras": [
    {
      "id": "PUT_CANONICAL_CAMERA_ID_HERE",
      "agent_id": "PUT_AGENTS_TABLE_ID_HERE",
      "user": "RTSP_USERNAME",
      "pass": "RTSP_PASSWORD",
      "transport": "udp",
      "rtsp": {
        "url": "rtsp://192.168.1.187:554/stream2",
        "stream_id": 1
      },
      "onvif": {
        "url": "http://192.168.1.187:2020/onvif/service"
      }
    }
  ],
  "forward_agent": {
    "server_addr": "server.local:9000",
    "heartbeat_interval_secs": 30,
    "config_pull_interval_secs": 30
  },
  "logging": {
    "rust_log": "info"
  }
}
```

Notes:

- `server.json` is for admin API / heartbeat / config pull.
- `camera.json` is where `forward_agent.server_addr` lives for the TLS ingest uplink.
- `forward_agent.heartbeat_interval_secs` controls heartbeat cadence (default `30`).
- `forward_agent.config_pull_interval_secs` controls remote config pull cadence (default `30`).
- The agent reloads these JSON files periodically, so edits are usually picked up without a
  restart.

### 5. Run the local agent

```bash
cargo run --bin sentinel-agent
```

Expected healthy logs include:

- `Loaded camera configs`
- `Starting agent heartbeat poster`
- `Agent uplink configured`
- `Uplink connected`
- `Uplink HELLO sent`

### 6. Common local failures

- `Config pull failed; server returned error status=404 Not Found`
  - The admin server has no stored `device_configs` row for that camera yet.
  - This does not block local JSON-based forwarding; save camera config once in the admin UI if
    you want the warning gone.
- `Uplink TLS config failed error=No such file or directory`
  - `/etc/sentinel_rtp_cam/ca.crt` is missing or unreadable.
- `Agent in standby; waiting for forward config`
  - `camera.json` is missing a usable camera entry or `forward_agent.server_addr`.
- Heartbeats succeed but live playback does not
  - Check that the camera `id` in `camera.json` exactly matches the admin server camera
    `camera_id`.

### 7. Live playback v2

This repo only forwards the stream. The v2 live playback routes and UI live in
`sentinel-admin-server`. See:

- `../sentinel-admin-server/docs/live-playback-v2.md`
- `docs/live_pipeline_versions.md`

## Service management

```bash
sudo sentinel-manage status
sudo sentinel-manage logs
sudo sentinel-manage start
sudo sentinel-manage stop
sudo sentinel-manage restart
```

## Update

Install the latest release (does not auto-start):

```bash
sudo sentinel-manage update latest
```

If you want it to restart services:

```bash
sudo sentinel-manage update latest --start
```

## Build from source

We do not build on Raspberry Pi. Releases are built in GitHub Actions and deployed via
`sentinel-manage update` from the `sentinel-tooling` repo.

## License

MIT
