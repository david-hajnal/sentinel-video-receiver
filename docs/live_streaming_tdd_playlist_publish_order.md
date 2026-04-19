# Playlist Publish Order TDD

## Problem

`PlaylistState::push_segment()` currently deletes evicted `.ts` files before atomically publishing the
replacement playlist. That creates a race where an older playlist can still be visible while one of its
segment files has already been removed.

## Scope

- File: `sentinel_rtp_cam/src/live/v2.rs`
- Primary code path: `PlaylistState::push_segment()`
- No backend or player changes in this slice

## Success criteria

- The new playlist becomes visible before any evicted segment file is removed.
- If playlist publish fails, previously referenced segment files remain on disk.
- Existing eviction semantics still converge to the configured window after successful publish.

## TDD plan

### Step 1: lock in the failing behavior

Add focused tests next to the existing `PlaylistState` tests in `live/v2.rs`.

Suggested tests:

1. `playlist_push_segment_keeps_evicted_file_until_new_manifest_is_published`
   - Arrange a playlist with `max_segments == 3`.
   - Create segment files `1..=4` on disk.
   - Push segments `1..=3` to establish the initial manifest.
   - On push of segment `4`, assert the old manifest is still readable until publish completes.
   - Assert `seg_000001.ts` still exists until the new `index.m3u8` is visible.

2. `playlist_push_segment_retains_old_files_when_manifest_publish_fails`
   - Introduce a narrow test seam for publish failure.
   - Force manifest publish to fail during the segment `4` push.
   - Assert `seg_000001.ts` is still present after the failed call.
   - Assert the previous `index.m3u8` contents remain unchanged.

### Step 2: minimum production change

- Reorder `push_segment()` to:
  1. append the new entry in memory
  2. compute the evicted entries but do not delete yet
  3. publish the new manifest
  4. delete evicted files only after publish succeeds
- Keep the change local to `PlaylistState`; do not add async background cleanup in the first pass.

### Step 3: refactor if needed

Only if the code becomes awkward, extract a helper that returns the evicted entries after publish.
Avoid introducing new config or retention knobs in this slice.

## Detailed test scenarios

### Scenario A: stale manifest reader

- Start from a manifest referencing `seg_000001.ts`, `seg_000002.ts`, `seg_000003.ts`.
- Push `seg_000004.ts`.
- Expected result:
  - old manifest remains internally consistent until replacement manifest exists
  - replacement manifest references `seg_000002.ts`, `seg_000003.ts`, `seg_000004.ts`
  - `seg_000001.ts` is deleted only after the replacement manifest has landed

### Scenario B: publish failure

- Same initial setup as Scenario A.
- Simulate failure while writing or renaming `index.m3u8`.
- Expected result:
  - previous manifest content is untouched
  - no referenced segment file is deleted
  - the function returns an error

### Scenario C: repeated eviction

- Push enough segments to trigger multiple evictions in sequence.
- Expected result:
  - `#EXT-X-MEDIA-SEQUENCE` remains correct
  - only the oldest file from the previous visible manifest is removed after each successful publish
  - no extra files disappear

## Verification commands

```bash
cargo test -p sentinel_rtp_cam playlist_push_segment
cargo test -p sentinel_rtp_cam run_stream_v2_gap_driven_segments_include_discontinuity_and_evict_old_segments
```
