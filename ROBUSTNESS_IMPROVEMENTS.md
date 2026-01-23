# Agent Robustness Improvements

## Overview
Made the agent more robust by ensuring clips are always saved locally, even when server API calls fail, and by implementing retry limits to prevent infinite retries.

## Changes Made

### 1. Added Maximum Retry Configuration
**File:** `sentinel_rtp_cam/src/config/agent_config.rs`

- Added `max_retries: u32` field to `ServerConfig`
- Default value: 5 attempts (configurable via `SERVER_MAX_RETRIES` environment variable)
- Setting `max_retries=0` enables infinite retries (backward compatible)

### 2. Implemented Retry-with-Limit Function
**File:** `sentinel_rtp_cam/src/server/server_client.rs`

- Added new `retry_with_limit()` function that:
  - Retries failed operations up to `max_retries` times
  - Uses exponential backoff (same as `retry_forever`)
  - Returns `Ok(None)` when max retries exceeded
  - Logs remaining attempts and final drop message
  - Returns `Ok(Some(T))` on success

### 3. Updated Motion Event Poster
**File:** `sentinel_rtp_cam/src/server/server_client.rs`

- `run_motion_event_poster()` now uses `retry_with_limit` by default
- Falls back to `retry_forever` if `max_retries=0`
- Events are dropped after max retries to prevent memory/queue buildup
- Spawns retry tasks in background - doesn't block event processing

### 4. Updated Clip Metadata Poster
**File:** `sentinel_rtp_cam/src/server/server_client.rs`

- `run_clip_meta_poster()` now uses `retry_with_limit` by default
- Same retry behavior as motion events
- Clips are **always saved to disk first** before metadata posting
- Metadata posting failures don't affect clip recording

### 5. Exported New Function
**File:** `sentinel_rtp_cam/src/server/mod.rs`

- Exported `retry_with_limit` for potential use in other modules

## Key Behaviors

### Clip Recording is Independent
✅ **Clips are ALWAYS saved to disk**, regardless of API call status:

1. Motion detected → Recorder arms
2. IDR frame received → FFmpeg starts recording to `.part` file
3. Motion ends → Post-roll timer starts
4. FFmpeg finalizes → `.part` renamed to `.mp4`
5. **Clip metadata sent to channel (non-blocking)**
6. Background task attempts to post metadata to server

The clip save happens in step 4, **before** any API call is attempted.

### Retry Behavior

#### With `max_retries=5` (default):
```
Attempt 1: Failed → Wait 2s  → Retry
Attempt 2: Failed → Wait 4s  → Retry
Attempt 3: Failed → Wait 6s  → Retry
Attempt 4: Failed → Wait 8s  → Retry
Attempt 5: Failed → Wait 10s → Retry
Max reached → DROP MESSAGE (log error)
```

#### With `max_retries=0` (infinite):
```
Retries forever with exponential backoff (capped at 10x interval)
```

## Configuration

### Environment Variables

```bash
# Maximum retry attempts before dropping message (default: 5, 0=infinite)
SERVER_MAX_RETRIES=5

# Retry interval in seconds (default: 2)
SERVER_RETRY_INTERVAL_SECS=2
```

### Log Messages

**Success after retries:**
```
INFO Operation succeeded after retries operation="post_motion_event" attempt=3
```

**Retry in progress:**
```
WARN Operation failed, will retry operation="post_motion_event" attempt=3 
     error="HTTP 422" backoff_secs=6 remaining_attempts=2
```

**Message dropped:**
```
ERROR Operation failed after max retries, dropping message 
      operation="post_motion_event" attempt=5 error="HTTP 422"
```

## Benefits

1. **No Data Loss**: Clips are always saved locally regardless of server connectivity
2. **Memory Efficiency**: Messages are dropped after max retries instead of queuing indefinitely
3. **Resource Protection**: Prevents infinite retry loops consuming CPU/network
4. **Observability**: Clear logging of retry progress and message drops
5. **Configurability**: Can tune retry behavior via environment variables
6. **Backward Compatible**: Setting `max_retries=0` restores infinite retry behavior

## Testing

To test the robust behavior:

1. **Start agent with server down:**
   ```bash
   SERVER_MAX_RETRIES=3 cargo run --bin app
   ```

2. **Trigger motion event** → Check logs for retry attempts

3. **Verify clip saved** → Check clips directory for `.mp4` files

4. **Observe message drop** → After 3 attempts, error logged and message dropped

5. **Clip should exist** regardless of API call success

## Migration Notes

Existing deployments will automatically get `max_retries=5` on restart. To maintain infinite retry behavior (not recommended), set `SERVER_MAX_RETRIES=0`.
