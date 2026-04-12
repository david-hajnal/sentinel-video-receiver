# Sentinel RTP Camera

Rust library and binaries for RTSP/RTP ingest, ONVIF motion detection, and forward-agent
streaming.

For device install/manage, use the tooling repo: `david-hajnal/sentinel-tooling`.

## Binaries

- `sentinel-agent` — production agent binary installed on devices.
- `server_ingest` — server-side ingest for forward mode.
- `dummy_server` — local admin API stub for development.
- `onvif_motion_pull` — diagnostics/dev helper.

## Configuration

For deployed agents, configuration is stored in:

- `/etc/sentinel_rtp_cam/server.json`
- `/etc/sentinel_rtp_cam/camera.json`

Use `sentinel-manage init` and `sentinel-manage config` (from `sentinel-tooling`) to manage
these files. Environment variables can still override values for development and testing.

## Logging

Use `RUST_LOG` to control verbosity:

```bash
RUST_LOG=info cargo run --bin sentinel-agent
RUST_LOG=debug cargo run --bin sentinel-agent
RUST_LOG=sentinel_rtp_cam::onvif_motion=debug,info cargo run --bin sentinel-agent
```

## Development

```bash
cargo build
cargo test
```

Run specific binaries:

```bash
cargo run --bin sentinel-agent
cargo run --bin server_ingest
cargo run --bin dummy_server
cargo run --bin onvif_motion_pull
```

## Local admin-server integration

For local end-to-end testing against the sibling `sentinel-admin-server` repo:

1. Start `admin-server` and `ingest` there with Docker Compose.
2. Create an agent and camera in the local admin server.
3. Write `/etc/sentinel_rtp_cam/server.json` and `/etc/sentinel_rtp_cam/camera.json`.
4. Install the ingest CA at `/etc/sentinel_rtp_cam/ca.crt`.
5. Run `cargo run --bin sentinel-agent`.

Important local details:

- `sentinel-agent` reads only `/etc/sentinel_rtp_cam/server.json` and
  `/etc/sentinel_rtp_cam/camera.json`.
- `server.json.example` and `camera.json.example` are templates only.
- `server_ingest` in this repo is not the same local ingest process used by the
  `sentinel-admin-server` live playback stack.
- Use the one-time agent token from `POST /api/management/agents`, not a per-camera device token.
- Use the canonical admin-server `camera_id` in `camera.json`.

See the top-level `README.md` for a full local JSON example.

## License

See LICENSE.
