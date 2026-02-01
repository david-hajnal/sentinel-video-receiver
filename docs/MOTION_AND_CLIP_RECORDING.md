# Motion Events and Clip Recording

This document explains how motion events are generated, normalized, forwarded, and how clip recording behaves in the agent and ingest pipeline.

## 1. Motion event lifecycle

### 1.1 Motion detection (ONVIF)
- The ONVIF poller detects motion state changes per rule.
- On **motion start**, a new `event_id` is generated (ULID).
- On **motion end**, the same `event_id` is reused for that rule.

### 1.2 Event channels
- **EventBus (edge events)**: emits `MotionEvent { rule, active, ts, camera_id, event_id }` on state transitions.
- **MotionStateBus (state)**: maintains current active rules and their metadata, used by clip recorders.

### 1.3 Forward agent normalization
The forward agent normalizes motion events before sending them to ingest.

Behavior:
- For a given `(camera_id, rule)`, the first `active=true` event establishes the `event_id`.
- Subsequent `active=true` events **reuse the same `event_id`** until an `active=false` arrives.
- The `active=false` event is forced to the same `event_id` and clears the latch.

This prevents duplicate event_ids for a single continuous motion period.

### 1.4 Forwarding to ingest
- Motion events are sent via the uplink channel with:
  - `stream_id`, `rule`, `active`, `ts`, `camera_id`, `event_id`.
- `stream_id` is resolved by `camera_id` or the first known stream as fallback.

## 2. Clip recording behavior

There are two recording paths:

### 2.1 Local clip recording (agent)
The agent’s local `ClipRecorder` is used in non-forward workflows (or local recording mode). It:
- Arms on motion, starts recording on the first IDR with SPS/PPS.
- Uses post-roll and minimum duration rules.
- Can hard-stop with `CLIP_MAX_SECS` and re-arm if motion is still active.

### 2.2 Ingest clip recording (server)
When the forward agent sends RTP to ingest:
- The ingest pipeline buffers access units in a ring buffer.
- A clip is started when motion becomes active and an IDR arrives.
- Clip stops when:
  - post-roll elapses after motion end, or
  - a hard stop (`CLIP_MAX_SECS`) is reached.

If `CLIP_MAX_SECS` is hit **while motion is still active**, ingest:
- Finalizes the current clip.
- Immediately re-arms a new clip using the **same event_id**.

This produces multiple clips for a long continuous motion period, all linked to the same event_id.

## 3. Event/clip mapping rules

- A single continuous motion period should map to **one event_id**.
- A long motion period may map to **multiple clips** (hard-stop split), all sharing the same event_id.
- Motion events should not emit new event_ids while recording is already active.

## 4. Environment variables (relevant)

Forward agent:
- `SERVER_ADDR`, `AGENT_TOKEN`, `AGENT_ID`
- `CAMx_RTSP_URL`, `CAMx_STREAM_ID`, `CAMx_CAMERA_ID`, `CAMx_TRANSPORT`, `CAMx_RTP_PORT`, `CAMx_RTCP_PORT`
- `CAMx_RTSP_USER`, `CAMx_RTSP_PASS` (or global `RTSP_USER`, `RTSP_PASS`)

Ingest:
- `CLIP_DIR`
- `CLIP_PRE_SECS`, `CLIP_POST_SECS`, `CLIP_RING_SECS`
- `CLIP_STALE_PART_SECS`
- `CLIP_MAX_SECS` (hard stop)

## 5. Operational expectations

- Duplicate motion events from ONVIF will be normalized by the forward agent.
- Activity log entries should show a stable `event_id` across a continuous motion period.
- Clips associated with a long motion period are expected to share the same `event_id`.

## 6. Testing focus

Key behaviors to test:
- Stable event_id forwarding during continuous motion.
- Correct event_id reset after motion ends.
- Hard-stop clip splitting with same event_id at ingest.
- RTP forwarding continuity after RTSP PLAY.

