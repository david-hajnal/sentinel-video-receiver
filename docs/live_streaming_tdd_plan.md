# Live Streaming TDD Implementation Plan

This plan broke the producer-side work from [live_streaming_review.md](./live_streaming_review.md)
into small implementation documents that could be executed independently.

## Status

Producer-side work in this repository is now implemented for the planned slices:

- `playlist_publish_order`: implemented
- `session_pointer_readiness`: implemented
- `stream_pipeline_restart`: implemented
- `sdp_sync_seed`: implemented

Remaining follow-up, if needed, is server-side verification or implementation in the backend repo.
Use [Server Changes TDD](./live_streaming_tdd_server_changes.md) for that work.

## Implemented improvements

- Playlist publish now happens before eviction, removing the stale-manifest-to-deleted-segment race.
- `current.json` is published only after the first playable segment and manifest exist, and it moves
  through `ready` and `stopped` with the latest published segment sequence.
- Stream pipelines are recreated after task exit instead of leaving stale senders in the ingest registry.
- SDP `sprop-parameter-sets` now flow from agent DESCRIBE parsing through uplink metadata into v2 sync
  seeding on ingest startup.

## Documents

- [Playlist Publish Order TDD](./live_streaming_tdd_playlist_publish_order.md)
- [Session Pointer Readiness TDD](./live_streaming_tdd_session_pointer_readiness.md)
- [Stream Pipeline Restart TDD](./live_streaming_tdd_stream_pipeline_restart.md)
- [SDP Sync Seed TDD](./live_streaming_tdd_sdp_sync_seed.md)
- [Server Changes TDD](./live_streaming_tdd_server_changes.md)

## Exit criteria

- Producer-side changes are implemented and covered by targeted tests near the affected modules.
- Remaining work, if any, is limited to backend lease/signing/serving behavior.
