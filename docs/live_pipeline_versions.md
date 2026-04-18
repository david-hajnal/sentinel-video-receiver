# Live Pipeline Versions

This repo currently supports two ingest artifact pipelines selected by
`LIVE_PIPELINE_VERSION`.

## v1

- Default: `LIVE_PIPELINE_VERSION=v1`
- Uses the existing `server_pipeline` implementation.
- Writes motion-triggered `.h264` clips and JSON sidecars into `CLIP_DIR`.
- Writes the existing `stream_<id>.health.json` health sidecar in `CLIP_DIR`.
- This is the only ingest pipeline that creates motion-triggered `.h264` clips.
- v1 does not produce viewer-facing HLS live output.

This path is the current default and was left in place unchanged.

## v2

- Enable with: `LIVE_PIPELINE_VERSION=v2`
- Uses the new additive live artifact pipeline in `sentinel_rtp_cam/src/live/v2.rs`.
- v2 is HLS-only and does not create motion-triggered `.h264` clips.
- Viewer-facing live output requires v2.
- Keeps the health sidecar filename and base schema stable:
  - `CLIP_DIR/stream_<id>.health.json`
- Adds live HLS artifacts under a parallel directory prefix:
  - `CLIP_DIR/hls_v2/<stream_id>/index.m3u8`
  - `CLIP_DIR/hls_v2/<stream_id>/seg_000001.ts`
  - `CLIP_DIR/hls_v2/<stream_id>/seg_000002.ts`

### v2 behavior

- Waits for SPS/PPS + IDR sync before opening the first published segment.
- Writes segments and playlists to temp files and atomically renames them into place.
- Keeps the playlist window bounded by `LIVE_HLS_WINDOW_SECS`.
- Uses `#EXT-X-DISCONTINUITY` on the next published segment after a gap/resync.
- Extends the health sidecar with:
  - `last_idr_at`
  - `last_segment_write_at`
  - `segment_seq`
  - `discontinuity_count`

### v2 config

- `LIVE_PIPELINE_VERSION`:
  - `v1` or `v2`
  - default: `v1`
- `LIVE_HLS_SEGMENT_SECS`:
  - default: `2`
- `LIVE_HLS_WINDOW_SECS`:
  - default: `12`

These can be provided directly as environment variables or through the existing
`ingest` config mapping in `camera.json`.

## Common Requirement

Both v1 and v2 require ingest to receive valid H.264 RTP from the forward agent.
If RTP never arrives, or arrives in a non-decodable/non-H.264 form, neither clips
nor live HLS artifacts will be produced.

## Rollback

To roll back from v2 to the current default pipeline:

```bash
export LIVE_PIPELINE_VERSION=v1
```

or remove the override entirely and restart `server_ingest`.
