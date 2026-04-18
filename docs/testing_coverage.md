# Coverage Testing (live pipeline v2)

This repository uses `cargo-llvm-cov` for Rust coverage checks.

## Prerequisites

- `cargo-llvm-cov`
- `jq`

Install `cargo-llvm-cov` locally:

```bash
cargo install cargo-llvm-cov
```

## Enforced Gate (v2 module)

Run the live pipeline v2 coverage gate:

```bash
LIVE_V2_COVERAGE_THRESHOLD=80 ./scripts/check_live_v2_coverage.sh
```

The script:
- runs `cargo llvm-cov test` for `live::v2::tests::`
- filters coverage report to `src/live/v2.rs`
- enforces `--fail-under-lines 80`
- exports a JSON summary report
- reads `src/live/v2.rs` line coverage
- fails when `src/live/v2.rs` drops below the threshold

Equivalent direct command:

```bash
cargo llvm-cov test \
  --manifest-path sentinel_rtp_cam/Cargo.toml \
  live::v2::tests:: \
  --ignore-filename-regex 'src/(agent_uplink|bin/|config/|core/|event/|forward_agent|motion_event_latch|onvif/|proto|rtsp/|server/|server_pipeline|utils/)' \
  --fail-under-lines 80
```

## Full Package Coverage Report

Generate a full package summary for `sentinel_rtp_cam`:

```bash
cargo llvm-cov \
  --manifest-path sentinel_rtp_cam/Cargo.toml \
  --tests \
  --json \
  --summary-only \
  --output-path /tmp/sentinel_rtp_cam_cov.json
```

Inspect `src/live/v2.rs` line coverage from that report:

```bash
jq -r '
  .data[0].files[]
  | select(.filename | endswith("/src/live/v2.rs"))
  | .summary.lines.percent
' /tmp/sentinel_rtp_cam_cov.json
```
