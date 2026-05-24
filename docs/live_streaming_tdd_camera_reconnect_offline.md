# Live Streaming TDD: Camera Reconnect + Offline Lifecycle

## Problem
If a camera cable is unplugged and re-plugged during live streaming, the agent can stop forwarding permanently until the agent process is restarted. Restart works because it re-runs the full RTSP session lifecycle (connect + OPTIONS/DESCRIBE/SETUP/PLAY), but the live task currently does not robustly self-heal.

## Goal
Make streaming self-recovering without agent restart:
1. Detect camera offline reliably.
2. Emit camera offline/online lifecycle events.
3. Reconnect automatically and resume stream forwarding when camera returns.

## Offline Detection Policy (chosen)
A camera is considered offline when either condition is met:
- Continuous outage timeout: no usable RTP for `20s`, or
- Burst outage threshold: at least `4` outage episodes of `>=5s` each within a sliding `120s` window.

Definitions:
- Outage episode starts when RTP read stalls/fails and ends on successful RTP recovery.
- Offline transition is emitted once per offline episode; recovery emits a single online transition.

## Implementation Scope

### Agent (`sentinel-video-receiver`)
- Refactor camera runtime into:
  - `run_camera_session(...)` (single RTSP session)
  - `run_camera_supervisor(...)` (reconnect loop with backoff and cancellation)
- Add ongoing RTP watchdog for both UDP and TCP interleaved paths (not startup-only watchdog).
- On read timeout/disconnect:
  - terminate session cleanly,
  - register outage episode,
  - reconnect with exponential backoff (1s -> 30s max),
  - resume forwarding on recovery.
- Add per-camera offline tracker implementing the selected thresholds.
- Add config knobs under `forward_agent`:
  - `camera_offline_timeout_secs` default `20`
  - `camera_offline_retry_outage_secs` default `5`
  - `camera_offline_retry_count` default `4`
  - `camera_offline_retry_window_secs` default `120`

### Event Reporting
- Agent emits lifecycle activity to `/api/v1/agent/activity`:
  - Offline: `source=rtsp`, `event=timeout|connect_fail`, `level=warn|error`
  - Recovery: `source=rtsp`, `event=reconnect`, `level=info`
- Messages include camera ID and threshold context to aid operations.

### Admin Server (`sentinel-admin-server`)
- In `post_agent_activity`, when a lifecycle device-activity row is newly inserted:
  - also insert an `activity_log` row:
    - `event_type="camera_offline"` for timeout/connect_fail
    - `event_type="camera_online"` for reconnect/connect_ok
  - publish SSE notification via existing `event_tx`.
- Keep dedupe behavior by only mirroring newly inserted activity records.

## Test Plan

### Agent tests
- Offline by continuous timeout (`20s`) is triggered exactly once.
- Offline by burst threshold (`4x >=5s within 120s`) is triggered.
- Sliding-window pruning prevents stale outage counts from triggering offline.
- Recovery clears offline state and emits online transition once.
- Supervisor reconnects after session failure and resumes RTP forwarding.
- Cancel token stops reconnect loop cleanly.

### Admin server tests
- Lifecycle activity mapping creates expected `activity_log.event_type`.
- No extra `activity_log` insert when activity record is deduped.
- Existing motion and clip event flows remain unchanged.

## Acceptance Criteria
- Unplug/replug during active stream no longer requires agent restart.
- Camera transitions to offline according to policy and returns online on recovery.
- Operators can see lifecycle changes in both:
  - `device_activity`
  - `activity_log` / live events stream
- No regressions in motion event ingestion or clip pipeline behavior.

### Assumptions
- Current deployment uses agent-capable bearer token for `/api/v1/agent/activity`.
- New `activity_log.event_type` values (`camera_offline`, `camera_online`) are acceptable for downstream consumers.
- Backoff policy remains existing 1s->30s exponential unless changed later.
