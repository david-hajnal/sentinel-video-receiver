# Sentinel RTP Camera Agent – Testing Plan

Goal: introduce unit + integration tests that verify **forward agent** behavior end-to-end (motion event handling, event_id consistency, RTP forwarding, and ingest-facing clip segmentation), then expand coverage to protocol and IO boundaries.

## Scope and priorities

Priority 0 (critical forward-agent behavior):
- Motion event forwarding with stable event_id while recording is in progress.
- No duplicate/extra event_ids emitted for the same continuous motion.
- RTP forwarding continuity (no silent drop of frames once session is established).
- Ingest-facing clip segmentation: long motion results in multiple clips with same event_id (via ingest behavior).

Priority 1 (core robustness, forward agent):
- RTSP handshake flow (OPTIONS/DESCRIBE/SETUP/PLAY) and recovery.
- ONVIF motion poller stability; no event floods or gaps.
- Backpressure handling when uplink is slow.

Priority 2 (integration coverage):
- Forward agent interacts with mock uplink and mock ingest.
- ONVIF motion polling feeds motion state and events as expected.
- End-to-end ingest clip flow validated (mock ffmpeg or noop remux).

## Current gaps

- No tests exercise forward agent motion events vs. ingest clip output.
- No integration tests for forward agent with synthetic RTP/NAL input.
- No deterministic tests around event_id stability across long motion.

## Proposed test architecture

### 1) Unit tests (fast, deterministic)

Target modules:
- `sentinel_rtp_cam/src/bin/agent_forward.rs` (logic extraction recommended)
- `sentinel_rtp_cam/src/onvif/onvif_motion.rs` (event generation)
- `sentinel_rtp_cam/src/event/event_bus.rs` (state changes)

Add test seams:
- Extract forward-agent orchestration into a testable module (inject RTSP client + uplink).
- Provide a fake RTSP source to emit SPS/PPS/IDR/AU sequences.
- Provide a fake uplink to capture forwarded RTP + motion events.

Suggested test cases (unit):
1. **Stable event_id while recording**
   - Multiple motion events during active recording → forwarded event_id remains the first one.
2. **Motion ends then restarts**
   - Ensure new event_id after full stop; no reuse across separate motions.
3. **RTSP auth + handshake**
   - Basic auth headers applied; OPTIONS/DESCRIBE/SETUP/PLAY are invoked.
4. **RTP forwarding continuity**
   - Frames after PLAY are forwarded without gaps for a steady stream.

### 2) Integration tests (forward agent in-process)

Target: `sentinel_rtp_cam/tests/` (new integration tests)

Build blocks:
- Fake RTSP server (or stub client) that emits NAL sequences.
- Mock uplink to capture forwarded RTP and motion events.
- Mock ingest receiver that records clip metadata requests.

Suggested integration scenarios:
1. **Continuous motion + long stream**
   - Forward agent streams RTP; ingest receives multiple clips with same event_id when `CLIP_MAX_SECS` is enforced.
2. **Burst motion**
   - Motion start/stop pairs close together; forwarded events map to expected event_id behavior.
3. **Motion without video**
   - Motion active but no IDR; expect no RTP forwarding and no ingest clip.

### 3) End-to-end smoke test (optional)

- Run forward agent against real ingest in Docker.
- Validate that ingest produces clips and activity log entries for motion.

## Design decisions to confirm

These are assumptions for tests and should be confirmed (forward agent centric):
- A long continuous motion produces **multiple clips** with the **same event_id** (ingest behavior).
- If motion ends during a clip, ingest respects post-roll and then stops.
- If motion restarts within post-roll, we **merge** into one clip (no split).
- Merge applies while **recording**, not after recording has stopped.

## Implementation steps

1) Extract forward-agent orchestration into a testable module.
2) Create `tests/forward_agent_unit_tests.rs` for event_id + RTP forwarding behavior.
3) Add helper builders for SPS/PPS/IDR NALs and a fake RTSP source.
4) Add integration tests under `tests/` with mock uplink + mock ingest.
5) Document commands for local runs and CI selection.

## Local test commands

- Unit tests only:
  `cargo test -p sentinel_rtp_cam clip_recorder`

- All tests:
  `cargo test -p sentinel_rtp_cam`

## CI integration

- Run unit tests on every push.
- Gate integration tests behind a label or nightly until stable.
- Keep test output dirs under `target/test-output` and clean after run.

## Deliverables

- `tests/forward_agent_unit_tests.rs`
- `tests/integration_forward_agent.rs` (or similar)
- `tests/fixtures/` with synthetic NAL sequences
- README update with testing instructions
