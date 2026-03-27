# Sentinel Video Receiver - Feature Inventory

This document lists implemented features in this repository (current state), not roadmap ideas.

## Runtime Components

- `agent_forward` (`sentinel_rtp_cam/src/bin/agent_forward.rs`)
  - Forwarding agent runtime for sending camera RTP + motion to ingest.
  - Loads config from local JSON, and also polls remote config (`/api/v1/config`).
  - Supports up to 4 camera slots (`CAM1..CAM4`) for forwarding.
  - Sends agent heartbeats including camera target status.
  - Normalizes motion `event_id` values for stable event lifecycle.

- `server_ingest` (`sentinel_rtp_cam/src/bin/server_ingest.rs`)
  - Receives forwarded stream data over custom framed protocol.
  - Handles `HELLO`, `RTP`, `GAP`, and `MOTION` messages.
  - Runs per-stream clip pipeline with ring-buffer pre-roll, post-roll, and hard-stop support.

- `dummy_server` (`sentinel_rtp_cam/src/bin/dummy_server.rs`)
  - Local development/testing server for agent integration.
  - SSE config stream + bearer-auth event endpoints.
  - Includes admin web UI for config editing.

- `onvif_motion_pull` (`sentinel_rtp_cam/src/bin/onvif_motion_pull.rs`)
  - Standalone ONVIF PullPoint motion diagnostics helper.

## Core Media Pipeline Features

- RTSP client implementation:
  - RFC2326-style request/response handling.
  - UDP transport receiver path.
  - TCP interleaved frame parsing support.

- RTP/H.264 processing:
  - RTP packet parsing and sequence continuity handling.
  - H.264 depacketization (single NAL, STAP-A, FU-A).
  - Sync gate requiring SPS/PPS + IDR before output.
  - Annex-B NAL output pipeline.

- Access-unit and loss handling:
  - Marker-driven access-unit assembly.
  - Gap signaling and depacketizer reset on discontinuity.

## Motion and Event Features

- ONVIF PullPoint motion polling:
  - WS-Security SOAP requests (`CreatePullPointSubscription`, `PullMessages`, `Renew`).
  - Topic filtering for ONVIF cell motion events.
  - Automatic resubscribe/recovery on poll errors.
  - Optional ONVIF XML debug dump (`onvif_dump/`).

- Motion event model:
  - Edge event bus (`EventBus`) for motion transitions.
  - State bus (`MotionStateBus`) for current active rules + metadata.
  - Motion events include `camera_id`, `rule`, `active`, timestamp, and `event_id`.

- Event ID behavior:
  - Stable event IDs through active motion periods.
  - Forward-mode normalization/latching to prevent duplicate IDs for one continuous motion.

## Clip Recording Features

- Local recorder (`ClipRecorder`):
  - Arms on motion, starts on keyframe conditions.
  - Pre/post-roll behavior with minimum clip duration.
  - Optional hard-stop by max clip bytes or max clip seconds.
  - Writes `.h264` clips with JSON sidecars.
  - Startup cleanup of stale `.part` files.
  - Retention controls: max files, max age, max total bytes.
  - Optional clip metadata channel for upload pipeline.

- Ingest recorder (`server_pipeline`):
  - Ring-buffer pre-roll and motion-triggered clip start.
  - Clip finalization on post-roll or hard-stop.
  - Handles long-running motion via hard-stop clip splitting.

## Server Integration Features

- Heartbeat:
  - Agent heartbeat includes per-camera status data.
  - Posts to `/api/v1/heartbeat` with `/api/heartbeat` fallback.

- Remote config pull:
  - Polls `/api/v1/config`.
  - Uses `x-camera-id` when a camera hint is available.

- Firmware job execution:
  - Polls `/api/agent/firmware/job` (and `/api/v1/agent/firmware/job` fallback).
  - Executes update/rollback command and reports job status via heartbeat payloads.

## Configuration Features

- JSON config files:
  - `/etc/sentinel_rtp_cam/server.json`
  - `/etc/sentinel_rtp_cam/camera.json`

- Runtime config processing:
  - Merges server + camera JSON.
  - Applies runtime override map used by existing env-based paths.
  - Supports per-camera RTSP/ONVIF/forward settings from JSON.

- Dynamic config:
  - `agent_forward` remote config pull loop.
  - `app` SSE config update listener.

## Protocol/Uplink Features

- Custom framed message protocol (`proto.rs`):
  - Message framing with magic/version/typed payload header.
  - Types include `HELLO`, `RTP`, `GAP`, `MOTION`, `PING`, `PONG`.
  - Payload size limit enforcement.

- Forward uplink (`agent_uplink.rs`):
  - Persistent uplink with reconnect/backoff loop.
  - HELLO handshake payload including stream metadata and ingest hints.
  - Keepalive/ping handling and basic uplink stats counters.

## Operations and Deployment Features

- Release workflow builds and packages `agent_forward`.
- Update/management flow delegated to `sentinel-tooling` (`sentinel-manage`).
