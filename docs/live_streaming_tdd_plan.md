# Live Streaming TDD Implementation Plan

This plan breaks the producer-side work from [live_streaming_review.md](./live_streaming_review.md)
into small implementation documents that can be executed independently.

## Assumptions

- Scope is limited to this repository.
- Work should prefer small, verifiable changes over broad refactors.
- Each item starts with failing tests and ends only after the targeted tests pass.
- External backend/UI follow-up remains out of scope for these docs.

## Suggested implementation order

1. `playlist_publish_order`: removes the stale-playlist-to-404 race with the smallest code change.
2. `session_pointer_readiness`: fixes publish ordering so `current.json` only points at playable sessions.
3. `stream_pipeline_restart`: closes the restart gap after pipeline failure.
4. `sdp_sync_seed`: improves startup reliability on cameras that send SPS/PPS only in SDP.

## Documents

- [Playlist Publish Order TDD](./live_streaming_tdd_playlist_publish_order.md)
- [Session Pointer Readiness TDD](./live_streaming_tdd_session_pointer_readiness.md)
- [Stream Pipeline Restart TDD](./live_streaming_tdd_stream_pipeline_restart.md)
- [SDP Sync Seed TDD](./live_streaming_tdd_sdp_sync_seed.md)
- [Server Changes TDD](./live_streaming_tdd_server_changes.md)

## Common TDD rules

- Add the narrowest failing test that reproduces the current behavior.
- Do not change production structure until the failure is demonstrated.
- Make the minimum code change that turns the test green.
- Refactor only when the passing tests make the safety boundary clear.
- Keep new assertions focused on externally observable behavior: files on disk, pointer contents,
  restart behavior, and ingest metadata flow.

## Exit criteria

- Every document has at least one red-green-refactor loop with named tests.
- New tests stay close to the module they verify.
- Existing live pipeline tests still pass after each slice.
