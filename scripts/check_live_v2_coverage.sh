#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST_PATH="$ROOT_DIR/sentinel_rtp_cam/Cargo.toml"
REPORT_PATH="${LIVE_V2_COVERAGE_REPORT_PATH:-$ROOT_DIR/target/live_v2_coverage_summary.json}"
THRESHOLD="${LIVE_V2_COVERAGE_THRESHOLD:-80}"
IGNORE_REGEX='src/(agent_uplink|bin/|config/|core/|event/|forward_agent|motion_event_latch|onvif/|proto|rtsp/|server/|server_pipeline|utils/)'

mkdir -p "$(dirname "$REPORT_PATH")"

cargo llvm-cov test \
  --manifest-path "$MANIFEST_PATH" \
  live::v2::tests:: \
  --ignore-filename-regex "$IGNORE_REGEX" \
  --fail-under-lines "$THRESHOLD" \
  --json \
  --summary-only \
  --output-path "$REPORT_PATH"

LIVE_V2_LINES_PERCENT="$(jq -r '
  .data[0].files[]
  | select(.filename | endswith("/src/live/v2.rs"))
  | .summary.lines.percent
' "$REPORT_PATH")"

if [[ -z "$LIVE_V2_LINES_PERCENT" || "$LIVE_V2_LINES_PERCENT" == "null" ]]; then
  echo "Could not find coverage entry for src/live/v2.rs in $REPORT_PATH" >&2
  exit 1
fi

echo "Coverage gate passed: src/live/v2.rs lines=${LIVE_V2_LINES_PERCENT}% (threshold ${THRESHOLD}%)"
