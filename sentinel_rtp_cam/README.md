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