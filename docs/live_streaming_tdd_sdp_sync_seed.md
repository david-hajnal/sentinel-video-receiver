# SDP Sync Seed TDD

## Problem

The agent parses SDP `sprop-parameter-sets`, but the live v2 ingest path does not propagate or use that
metadata. Cameras that provide SPS/PPS only out-of-band can remain permanently unsynced even if RTP is
otherwise healthy.

## Scope

- Files:
  - `sentinel_rtp_cam/src/core/sdp.rs`
  - `sentinel_rtp_cam/src/bin/agent_forward.rs`
  - `sentinel_rtp_cam/src/agent_uplink.rs`
  - `sentinel_rtp_cam/src/bin/server_ingest.rs`
  - `sentinel_rtp_cam/src/live/v2.rs`
- Keep the schema extension limited to the metadata needed for H.264 sync seeding.

## Success criteria

- SDP-derived SPS/PPS can travel from the agent to ingest for H.264 streams.
- `run_stream_v2()` seeds `H264SyncGate` before RTP processing begins.
- A stream with IDR-only RTP and SDP SPS/PPS becomes playable.

## TDD plan

### Step 1: add unit coverage at the metadata boundaries

Suggested tests:

1. `parse_sdp_video_track_extracts_sprop_parameter_sets`
   - Lock in the current SDP parsing behavior if not already covered.

2. `hello_stream_round_trips_h264_parameter_sets`
   - Extend the uplink metadata struct with optional SPS/PPS fields.
   - Serialize and deserialize it.
   - Assert exact base64 payload preservation.

3. `run_stream_v2_seeds_sync_gate_from_metadata`
   - Create a config or helper seam that passes metadata into the v2 runner.
   - Feed only an IDR RTP packet.
   - Assert the first segment is published because SPS/PPS arrived from metadata, not from RTP.

### Step 2: add an end-to-end narrow integration test

Suggested scenario:

- Agent side parses SDP with valid `sprop-parameter-sets`.
- Agent announces the stream over uplink metadata.
- Ingest receives metadata, seeds the gate, then processes RTP containing only IDR access units.
- Expected result:
  - `index.m3u8` is published
  - first segment exists
  - startup does not remain stuck waiting for in-band SPS/PPS

### Step 3: minimum production change

- Extend the uplink hello/stream metadata with optional H.264 SPS/PPS fields.
- Populate them from the already parsed SDP track in `agent_forward.rs`.
- Decode and validate them on ingest.
- Call the sync gate seeding method before consuming RTP for the stream.

### Step 4: keep failure handling explicit

Do not silently ignore invalid metadata.

Add tests for:

- invalid base64 SPS/PPS
- missing SPS with present PPS
- non-H.264 streams carrying no parameter sets

Expected result:

- invalid metadata is rejected or logged clearly
- ingest falls back to normal in-band sync behavior when metadata is absent

## Detailed test scenarios

### Scenario A: out-of-band parameter sets only

- SDP contains valid SPS/PPS.
- RTP stream begins with an IDR and no in-band SPS/PPS.
- Expected result:
  - sync gate becomes primed from metadata
  - first segment is published

### Scenario B: malformed metadata

- SDP or uplink metadata contains invalid base64 or invalid NAL bytes.
- Expected result:
  - stream does not panic
  - failure is surfaced clearly
  - pipeline can still wait for in-band SPS/PPS

### Scenario C: mixed path

- Metadata provides SPS only; PPS arrives in-band later.
- Expected result:
  - gate remains unsynced until PPS + IDR requirements are satisfied
  - once satisfied, playback starts normally

## Verification commands

```bash
cargo test -p sentinel_rtp_cam sdp
cargo test -p sentinel_rtp_cam h264_sync
cargo test -p sentinel_rtp_cam v2_publishes_segment_after_sps_pps_and_idr_sync
```
