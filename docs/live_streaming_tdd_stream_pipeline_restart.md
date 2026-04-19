# Stream Pipeline Restart TDD

## Problem

`ensure_stream()` stores a sender in the stream map and never removes or replaces it when the underlying
pipeline task exits. A single fatal pipeline error can leave the stream permanently stuck until process
restart.

## Scope

- File: `sentinel_rtp_cam/src/bin/server_ingest.rs`
- Primary code path: `ensure_stream()` and the task lifecycle around it
- Keep the fix local; do not redesign the whole ingest server

## Success criteria

- If a stream pipeline exits, the stale handle is removed or replaced.
- A later RTP packet for the same stream creates a fresh pipeline automatically.
- Normal steady-state behavior for healthy streams is unchanged.

## TDD plan

### Step 1: create a test seam for pipeline execution

Before changing logic, introduce a minimal seam so tests can inject a short-lived or failing pipeline.
Possible approaches:

- wrap the pipeline launch behind a helper function under `#[cfg(test)]`
- inject a function pointer or small trait used only by `ensure_stream()`

Keep the seam narrow. The goal is only to force task exit deterministically.

### Step 2: add failing tests

Suggested tests in `server_ingest.rs` unit tests:

1. `ensure_stream_reuses_existing_sender_while_pipeline_is_alive`
   - Call `ensure_stream()` twice with the same stream id.
   - Assert the same logical stream handle is reused.

2. `ensure_stream_recreates_pipeline_after_task_exit`
   - Use a failing pipeline stub that exits immediately after receiving one message.
   - Send a packet to create the stream.
   - Wait for task exit.
   - Call `ensure_stream()` again or route another packet through the connection handler.
   - Assert a new pipeline launch occurs.

3. `closed_sender_from_handle_conn_triggers_stream_recreation`
   - Arrange for `try_send()` to hit a closed channel.
   - Assert the stale sender is dropped and a new stream is created on the next packet.

### Step 3: minimum production change

- Replace the raw `HashMap<u32, Sender<StreamMsg>>` with a small `StreamHandle` carrying at least:
  - `tx`
  - generation or task token
- On task exit, remove the map entry only if the exiting task still owns the current generation.
- When send fails because the channel is closed, evict the stale handle and recreate.

### Step 4: refactor only after green

If the cleanup logic is duplicated, extract a tiny helper for stale-handle eviction. Avoid adding a full
supervisor loop unless the tests prove the local fix is insufficient.

## Detailed test scenarios

### Scenario A: one-shot failure then recovery

- First packet creates stream `7`.
- Stub pipeline exits with an error after consuming that packet.
- Second packet for stream `7` arrives later.
- Expected result:
  - second packet goes to a newly created pipeline
  - active stream count returns to the correct value
  - stream map contains only the new handle

### Scenario B: stale exit should not remove a newer handle

- Create pipeline generation `1`.
- Before its exit handler runs, create generation `2` for the same stream.
- Let generation `1` finish.
- Expected result:
  - cleanup from generation `1` does not remove generation `2`

### Scenario C: healthy stream reuse

- Pipeline stays alive.
- Multiple packets for the same stream arrive.
- Expected result:
  - only one pipeline launch
  - the sender remains reusable

## Verification commands

```bash
cargo test -p sentinel_rtp_cam ensure_stream
cargo test -p sentinel_rtp_cam parse_motion_msg
```
