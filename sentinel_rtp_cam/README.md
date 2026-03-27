# Sentinel RTP Camera

Rust library and binaries for RTSP/RTP ingest, ONVIF motion detection, and forward-agent
streaming.

For device install/manage, use the tooling repo: `david-hajnal/sentinel-tooling`.

## Binaries

- `agent_forward` — forward agent used in production (talks to server ingest).
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
RUST_LOG=info cargo run --bin agent_forward
RUST_LOG=debug cargo run --bin agent_forward
RUST_LOG=sentinel_rtp_cam::onvif_motion=debug,info cargo run --bin agent_forward
```

## Development

```bash
cargo build
cargo test
```

Run specific binaries:

```bash
cargo run --bin agent_forward
cargo run --bin server_ingest
cargo run --bin dummy_server
cargo run --bin onvif_motion_pull
```

## License

See LICENSE.
