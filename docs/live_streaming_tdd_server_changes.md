# Live Streaming Server Changes TDD

## Why this document exists

The producer-side documents cover only this repository. The review also identified server-side work that is
likely required in the backend repo that issues leases, signs HLS asset URLs, and serves manifests and
segments.

Create this document only if that backend path is still missing the guarantees below. If those guarantees
already exist in the server repo, use this as a verification checklist instead of an implementation plan.

## Problem

Live playback correctness depends on strict server behavior at the session boundary. If the server selects
stale sessions, signs paths too loosely, rewrites manifests incorrectly, or collapses distinct failure modes
into the same response, clients can replay old data or get stuck retrying URLs that can never succeed.

## Scope

Expected server responsibilities:

- Lease acquisition for a camera live session
- Reading producer state such as `current.json`
- Signed HLS playlist and segment URLs
- Manifest rewrite for signed segment URIs
- Asset serving for playlists and `.ts` segments
- Response semantics for `202`, `403`, and `404`
- Cache policy for live playlists and pointer-like metadata

This document assumes the producer now exposes these producer-side guarantees:

- `current.json` points only to a playable session
- `writer_state` distinguishes `starting`, `ready`, and `stopped`
- Session-relative asset paths are stable within a generation

## Success criteria

- Lease selection always binds to the intended camera, session, and generation.
- Signed URLs are valid only for the exact session-relative asset path they represent.
- Rewritten manifests preserve the session prefix on every segment URI.
- `202`, `403`, and `404` mean different things and are not overloaded.
- Live metadata and playlists are not cached in ways that can replay stale session state.

## Suggested implementation order

1. Lease/session binding
2. Manifest rewrite correctness
3. Asset-serving response semantics
4. Cache headers and observability

## TDD plan

### Step 1: lease acquisition is strict

Write failing tests around the lease endpoint before changing handlers.

Suggested tests:

1. `lease_returns_202_when_current_session_is_starting`
   - Producer state points at a session with `writer_state = starting`.
   - Expected result:
     - response is `202`
     - body clearly indicates waiting for playable output
     - no signed playable asset URL is returned yet

2. `lease_returns_ready_session_and_generation`
   - Producer state points at `writer_state = ready` with generation `N`.
   - Expected result:
     - response selects that session and generation
     - returned lease payload includes session id and generation
     - returned playlist URL is signed for that specific session path

3. `lease_does_not_fall_back_to_root_level_playlist`
   - Root-level stale artifacts exist beside sessionized output.
   - Expected result:
     - lease selection uses only `current.json` and sessionized paths
     - server never serves `hls_v2/<camera>/index.m3u8` as the active live source

### Step 2: signing is bound to the full asset path

Suggested tests:

1. `signed_segment_url_is_invalid_for_same_filename_in_different_session`
   - Sign `session_a/seg_000001.ts`.
   - Replay the token against `session_b/seg_000001.ts`.
   - Expected result: `403`.

2. `signed_playlist_url_is_invalid_for_different_generation`
   - Generate a lease for generation `4`.
   - Attempt to use it after `current.json` advances to generation `5`.
   - Expected result:
     - old playlist or lease is rejected consistently
     - response does not accidentally serve newer session bytes under an older signature

3. `manifest_rewrite_signs_every_segment_uri_with_session_relative_path`
   - Rewrite a manifest for a sessionized playlist.
   - Expected result:
     - every segment line is rewritten
     - each rewritten URI includes or encodes the correct session-relative asset path
     - no bare filenames remain

### Step 3: asset-serving response semantics are distinct

Suggested tests:

1. `playlist_request_returns_202_for_starting_session`
   - Lease points at a session not yet ready.
   - Expected result: `202`, not `404`.

2. `asset_request_returns_403_for_invalid_signature`
   - Tamper with signature, expiry, or path.
   - Expected result: `403`.

3. `asset_request_returns_404_for_missing_asset_under_valid_signature`
   - Use a valid signature for a segment that no longer exists.
   - Expected result: `404`.

4. `asset_request_does_not_map_missing_session_to_403`
   - Session path is missing due to producer GC or wrong session id.
   - Expected result:
     - valid signature plus missing object yields `404`
     - not-found is not disguised as auth failure

### Step 4: caching behavior is safe for live playback

Suggested tests:

1. `current_pointer_and_live_manifest_are_no_store`
   - Request pointer-like metadata and the live playlist.
   - Expected result:
     - `Cache-Control: no-store, max-age=0, must-revalidate`

2. `segment_responses_use_explicit_policy`
   - Request a signed segment.
   - Expected result:
     - segment caching policy matches the token TTL and retry strategy
     - response does not inherit playlist no-store semantics by accident unless intended

3. `manifest_revalidation_after_session_change_observes_new_session`
   - Session generation advances.
   - Client re-requests the playlist.
   - Expected result:
     - server returns the new session path
     - stale manifest is not replayed from cache

## Detailed test scenarios

### Scenario A: fresh camera still starting

- Producer has created a private session directory.
- `current.json` either does not exist yet or reports `starting`.
- Client requests a live lease.
- Expected result:
  - `202`
  - clear machine-readable waiting state
  - no stale previous session is substituted silently

### Scenario B: generation rollover after restart

- Camera restarts and producer advances from generation `10` to `11`.
- Client still holds a lease or playlist signed for generation `10`.
- Expected result:
  - generation `10` URLs cannot fetch generation `11` assets
  - client must reacquire a lease

### Scenario C: same filenames across different sessions

- Both sessions contain `seg_000001.ts`.
- Expected result:
  - signatures remain path-specific
  - no cache key or auth path drops the session prefix

### Scenario D: rewritten manifest integrity

- Source manifest contains relative segment lines and discontinuity markers.
- Expected result:
  - tags such as `#EXTINF` and `#EXT-X-DISCONTINUITY` are preserved
  - only URI lines are rewritten
  - rewritten URI count matches source segment count exactly

### Scenario E: stale or missing objects

- Playlist or segment object is gone because retention or GC removed it.
- Expected result:
  - valid auth plus missing object returns `404`
  - telemetry identifies camera, session, generation, and requested asset path

## Implementation notes

Keep the implementation narrow.

- Lease records should bind at least:
  - camera id
  - session id
  - generation
  - asset prefix or exact asset path
  - expiry
- Manifest rewriting should operate on parsed playlist lines, not regex replacement over the whole file.
- Signature validation should happen before object fetch, but response codes must still preserve the
  `403` vs `404` distinction.
- Do not introduce filename-only shortcuts for lookup, signing, caching, or metrics labels.

## Observability

Add structured logging and metrics for:

- lease acquisition result: `starting`, `ready`, `stopped`, `not_found`
- signature validation failures by reason
- playlist rewrite failures
- asset misses by camera/session/generation
- count of segment URIs rewritten per manifest

## Verification checklist

- Lease endpoint never selects a root-level stale playlist.
- All returned live URLs are session-scoped.
- Every segment in a rewritten manifest is signed.
- `202`, `403`, and `404` are distinguishable in tests and logs.
- Cache headers on live metadata prevent stale session replay.
