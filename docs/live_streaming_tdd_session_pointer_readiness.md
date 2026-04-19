# Session Pointer Readiness TDD

## Problem

`initialize_live_session()` writes `current.json` immediately in `starting` state, before the session has a
playable `index.m3u8` or segment. That allows external consumers to observe a session that is not ready.

## Scope

- File: `sentinel_rtp_cam/src/live/v2.rs`
- Primary code paths:
  - `initialize_live_session()`
  - `write_current_session_pointer()`
  - `finalize_segment()` and the first successful publish path

## Success criteria

- A new session is not published to `current.json` until it has a playable manifest.
- `writer_state` progresses in a deterministic way.
- `last_segment_seq` reflects the latest published segment during steady-state, not only shutdown.

## TDD plan

### Step 1: replace startup-pointer assumptions with readiness tests

The current tests encode the behavior we want to change. Replace them with behavior-oriented tests.

Suggested tests:

1. `initialize_live_session_creates_private_session_without_current_pointer`
   - Create a new session root.
   - Call session initialization.
   - Assert the session directory exists.
   - Assert `current.json` does not exist yet.

2. `v2_does_not_publish_current_pointer_before_first_playlist`
   - Start `run_stream_v2()`.
   - Send non-sync RTP only.
   - Assert the session directory may exist, but `current.json` does not point to it yet.

3. `v2_publishes_current_pointer_after_first_segment_and_marks_ready`
   - Start `run_stream_v2()`.
   - Send SPS, PPS, then IDR.
   - Assert `index.m3u8` exists before or at the same observable point as `current.json`.
   - Assert `writer_state == "ready"`.
   - Assert `last_segment_seq == 1`.

4. `write_current_session_pointer_updates_last_segment_seq_on_each_publish`
   - Publish multiple segments.
   - Assert `current.json` moves from `1` to `2` to `3` as segments are finalized.

### Step 2: minimum production change

- Split session creation from pointer publication.
- Keep creating the session directory up front.
- Delay `current.json` creation until after the first successful segment and manifest publish.
- Update pointer writes on every successful segment publish, using `ready` after first publish and
  `stopped` on teardown.

### Step 3: clean up tests and helpers

- Rename tests that currently assert startup pointer creation.
- Keep the public behavior simple; do not add extra states beyond what tests need.

## Detailed test scenarios

### Scenario A: unsynced startup

- Start the pipeline and send only P-frames or malformed sync input.
- Expected result:
  - no `index.m3u8`
  - no published `current.json` for the new session
  - health reporting can show waiting state, but the pointer does not advance

### Scenario B: first playable publish

- Send SPS, PPS, IDR and allow the first segment to finalize.
- Expected result:
  - session directory exists
  - `seg_000001.ts` exists
  - `index.m3u8` exists
  - `current.json` points at this session only after those artifacts are published
  - `writer_state` is `ready`

### Scenario C: later segment publish

- Continue streaming and force another rotation.
- Expected result:
  - `current.json.last_segment_seq` increments with each published segment
  - `generation` remains stable inside the same session

### Scenario D: clean shutdown

- Stop the stream after at least one segment.
- Expected result:
  - `writer_state` becomes `stopped`
  - `last_segment_seq` remains equal to the last published segment

## Verification commands

```bash
cargo test -p sentinel_rtp_cam initialize_live_session
cargo test -p sentinel_rtp_cam v2_publishes_segment_after_sps_pps_and_idr_sync
cargo test -p sentinel_rtp_cam v2_does_not_emit_segments_until_sync
```
