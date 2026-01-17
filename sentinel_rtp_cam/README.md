# Sentinel Video Receiver

A Rust library for receiving RTSP/RTP video streams with ONVIF motion detection integration.

## Features

- RTSP client with TCP interleaved and UDP transport modes
- RTP packet parsing and H.264 video depacketization
- ONVIF motion event detection via WS-Security authenticated SOAP
- Motion-triggered video clip recording with FFmpeg
- Event-driven architecture with pub-sub pattern

## Quick Start

### Configuration

Create a `.env` file from `.env.example`

### Logging

The application uses the `tracing` crate for structured logging. Control verbosity using the `RUST_LOG` environment variable:

```bash
# Show only errors and warnings
RUST_LOG=warn cargo run --bin app

# Show info messages (default)
RUST_LOG=info cargo run --bin app

# Show debug messages for troubleshooting
RUST_LOG=debug cargo run --bin app

# Show all trace messages
RUST_LOG=trace cargo run --bin app

# Filter by module (e.g., only RTSP debug logs)
RUST_LOG=sentinel_rtp_cam::rtsp=debug,info cargo run --bin app

# Multiple filters
RUST_LOG=sentinel_rtp_cam::onvif_motion=debug,sentinel_rtp_cam::clip_recorder=trace,info cargo run --bin app
```

Log output includes:
- Thread IDs for async task tracking
- Source file and line numbers
- Structured fields for easy parsing (rule, timestamp, ports, etc.)

Example log output:
```
2026-01-17T10:30:45.123Z  INFO sentinel_rtp_cam::onvif_motion: ONVIF motion poller starting service=http://192.168.1.100:2020/onvif/service
2026-01-17T10:30:46.456Z  INFO sentinel_rtp_cam::rtsp_receiver_udp: Starting RTSP UDP receiver host=192.168.1.100 port=554
2026-01-17T10:30:50.789Z  INFO sentinel_rtp_cam: Motion detected rule="VideoSource/MotionAlarm" timestamp=2026-01-17T10:30:50Z
2026-01-17T10:30:51.012Z  INFO sentinel_rtp_cam::clip_recorder: Recording started rule="VideoSource/MotionAlarm"
```

### Running

Start the receiver with ONVIF motion detection:

```bash
cargo run --bin app
```

Run ONVIF motion detection only:

```bash
cargo run --bin app -- --onvif-only
```

## Architecture

- **RTSP Client**: Handles RTSP protocol communication (OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN)
- **RTP Parser**: Parses RTP packets and extracts H.264 NAL units
- **H.264 Depacketizer**: Reconstructs fragmented NAL units (FU-A) from RTP payloads
- **ONVIF Client**: Polls motion events using SOAP PullPoint subscriptions
- **Event Bus**: Distributes motion events to subscribers
- **Clip Recorder**: Records video clips with pre/post-roll on motion events

## Project Structure

```
src/
├── lib.rs                  # Public API
├── error.rs                # Typed error handling
├── rtp.rs                  # RTP packet parsing
├── rtsp.rs                 # RTSP client
├── sdp.rs                  # SDP parser
├── h264_depacketize.rs     # H.264 RTP depacketization
├── onvif_motion.rs         # ONVIF motion detection
├── event_bus.rs            # Event distribution
├── clip_recorder.rs        # Video recording
├── rtsp_receiver_tcp.rs    # TCP interleaved receiver
├── rtsp_receiver_udp.rs    # UDP receiver
└── bin/
    └── app.rs              # Main application

```

## Requirements

- Rust 2021 edition
- FFmpeg (for video recording)
- ONVIF-compatible IP camera

## Development

### Build

```bash
cargo build
```

### Test

```bash
cargo test
```

### Run specific binary

```bash
cargo run --bin rtsp_pull_tcp_interleaved_to_h264
cargo run --bin rtsp_pull_udp_to_h264
cargo run --bin onvif_motion_pull
```

## License

See LICENSE file for details.