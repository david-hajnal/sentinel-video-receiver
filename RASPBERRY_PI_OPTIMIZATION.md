# Raspberry Pi 3 Performance Optimizations for Clip Recording

## Current Bottleneck
The main issue is **audio track encoding**. The code adds a silent audio track for browser compatibility, which requires AAC encoding. This is CPU-intensive on Raspberry Pi 3.

## Immediate Fixes (No Code Changes Required)

### 1. Reduce Recording Buffer Times
Edit your `.env` file:
```bash
CLIP_PRE_ROLL_SECS=1    # Reduced from 2
CLIP_POST_ROLL_SECS=2   # Reduced from 3
CLIP_MAX_SECS=30        # Reduced from 60 (optional)
```

### 2. System Configuration
Add to `/boot/config.txt` (requires reboot):
```
# Allocate more memory to GPU for video processing
gpu_mem=256

# Optional: Mild overclock (if you have cooling)
arm_freq=1350
over_voltage=4
```

### 3. Storage Optimization
```bash
# Remount with noatime to reduce write operations
sudo mount -o remount,noatime /

# Make permanent in /etc/fstab - add 'noatime' option
# Example: /dev/mmcblk0p2  /  ext4  defaults,noatime  0  1
```

### 4. Camera Settings
- Use lower resolution stream (e.g., 720p instead of 1080p)
- Lower bitrate/quality at camera
- Reduce FPS to 15-20 if acceptable for your use case
- Ensure camera outputs H.264 (not MJPEG)

## Code Changes (Recommended)

### Option A: Make Audio Track Optional (Best Solution)

Add environment variable to control audio:
```bash
CLIP_AUDIO_ENABLED=0  # Disable audio track for RPi
```

Then modify `src/core/clip_recorder.rs` to respect this setting.

### Option B: Use Hardware Encoding for Audio (if needed)
If you need audio, use hardware AAC encoder instead of software.

### Option C: Remove Audio Completely
If you don't need browser compatibility, remove the audio track entirely.

## Performance Comparison

**With silent audio track (current):**
- ffmpeg CPU usage: 80-120% on RPi 3
- Encoding time: ~2-3x real-time

**Without audio track:**
- ffmpeg CPU usage: 5-15% on RPi 3 (with stream copy)
- Encoding time: Near real-time

**With hardware encoding (if no stream copy):**
- ffmpeg CPU usage: 30-50% on RPi 3
- Encoding time: ~1.2x real-time

## Testing Performance

```bash
# Monitor ffmpeg CPU usage
htop  # Look for ffmpeg process

# Check clip encoding time vs duration
ls -lh clips/
# Compare file creation time vs clip duration

# Test hardware encoder availability
ffmpeg -codecs | grep h264_omx
```

## Additional Tips

1. **Monitor temperature**: `vcgencmd measure_temp`
2. **Use SSD over SD card**: Huge improvement in I/O
3. **Reduce concurrent operations**: Avoid running multiple clips simultaneously
4. **Consider USB Coral**: Offload motion detection if needed
5. **Upgrade to RPi 4**: 4GB model offers much better performance

## Quick Test

Try this manually to see improvement without audio:
```bash
ffmpeg -hide_banner -loglevel error \
  -fflags +genpts -r 25 -f h264 -i pipe:0 \
  -c:v copy \
  -movflags +faststart \
  -f mp4 test.mp4
```

Compare CPU usage with and without the audio track.

## Environment Variables Summary

Optimized `.env` for Raspberry Pi 3:
```bash
# Performance optimizations
CLIP_STREAM_COPY=1           # Already set - KEEP THIS
CLIP_PRE_ROLL_SECS=1         # Reduced buffer
CLIP_POST_ROLL_SECS=2        # Reduced buffer
CLIP_MAX_SECS=30             # Shorter clips
CLIP_AUDIO_ENABLED=0         # NEW - Disable audio (needs code change)

# Storage limits (adjusted for SD card)
CLIP_MAX_FILES=50
CLIP_MAX_TOTAL_BYTES=5368709120  # 5GB
CLIP_MAX_AGE_SECS=604800         # 7 days

# Other settings
CLIP_FPS=25
CLIP_DEBOUNCE_MS=300
CLIP_MIN_DURATION_SECS=5
```
