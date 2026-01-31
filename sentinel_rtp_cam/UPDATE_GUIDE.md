# How to Update Sentinel Agent on Raspberry Pi

**Important:** We do **not** build on Raspberry Pi. Builds are produced on GitHub Actions and
the Pi only downloads prebuilt releases.

## Option 1: Using the update.sh Script (Recommended)

The easiest way to update:

```bash
# SSH into your Raspberry Pi
ssh dietpi@agentsmith

# Navigate to the installation directory
cd ~/sentinel-video-receiver/sentinel_rtp_cam

# Run the update script
sudo ./update.sh
```

The script will:
- Detect your architecture
- Download the latest release
- Stop the service
- Install the new binary
- Restart the service
- Verify it's running

## Option 2: Update from a GitHub Build (Preferred for new versions)

If you need a new version, trigger the GitHub workflow to build a release, then update the Pi:

```bash
sudo SENTINEL_VERSION=1.2.3 ./update.sh
```

## Option 3: Update Configuration Only

If only .env changed (like adding CLIP_AUDIO_ENABLED):

```bash
# SSH into Raspberry Pi
ssh dietpi@agentsmith

# Pull latest changes
cd ~/sentinel-video-receiver
git pull

# Update the service configuration
sudo nano /etc/sentinel_rtp_cam/env

# Add or modify:
CLIP_AUDIO_ENABLED=0
CLIP_PRE_ROLL_SECS=1
CLIP_POST_ROLL_SECS=2
CLIP_MAX_SECS=30
CLIP_MIN_FREE_BYTES=1000000000

# Restart service to apply changes
sudo systemctl restart sentinel_rtp_cam

# Check logs
sudo journalctl -u sentinel_rtp_cam -f
```

## Using the Management Script

After updating, use `manage.sh` to start/restart and follow logs:

```bash
# Copy manage script to home directory (optional)
cp ~/sentinel-video-receiver/sentinel_rtp_cam/manage.sh ~/manage-sentinel.sh
chmod +x ~/manage-sentinel.sh

# Common commands
~/manage-sentinel.sh config      # Edit configuration
~/manage-sentinel.sh start       # Start service
~/manage-sentinel.sh restart     # Restart service
~/manage-sentinel.sh logs        # Follow live logs
~/manage-sentinel.sh clips       # List clips
~/manage-sentinel.sh status      # Show status
```

## Verify the Update

```bash
# Check service is running
sudo systemctl status sentinel_rtp_cam

# Check recent logs for errors
sudo journalctl -u sentinel_rtp_cam -n 50 --no-pager

# Monitor CPU usage (should be much lower with CLIP_AUDIO_ENABLED=0)
htop

# Check temperature
vcgencmd measure_temp
```

## Rollback if Needed

If something goes wrong:

```bash
# Stop the new version
sudo systemctl stop sentinel_rtp_cam

# Download previous release
cd /tmp
wget https://github.com/USER/REPO/releases/download/v0.X.X/sentinel_rtp_cam-v0.X.X-armv7.tar.gz
tar xzf sentinel_rtp_cam-v0.X.X-armv7.tar.gz

# Install old version
sudo mv app /usr/local/bin/sentinel_rtp_cam

# Start service
sudo systemctl start sentinel_rtp_cam
```

## Expected Improvements After Update

With CLIP_AUDIO_ENABLED=0:
- **CPU usage**: From 80-120% to 5-15% during clip recording
- **Encoding speed**: From 2-3x real-time to near real-time
- **Temperature**: Significantly lower
- **Reliability**: Fewer timeouts and errors

## Troubleshooting

### Service won't start
```bash
# Check detailed error
sudo journalctl -u sentinel_rtp_cam -n 100 --no-pager

# Verify binary permissions
ls -l /usr/local/bin/sentinel_rtp_cam

# Test manually
cd /var/lib/sentinel_rtp_cam
sudo -u sentinel_rtp_cam /usr/local/bin/sentinel_rtp_cam
```

### Configuration not applied
```bash
# Verify config file location
cat /etc/sentinel_rtp_cam/env

# Restart is required for env changes
sudo systemctl restart sentinel_rtp_cam
```

### Still high CPU usage
```bash
# Verify stream copy is enabled
grep CLIP_STREAM_COPY /etc/sentinel_rtp_cam/env

# Verify audio is disabled
grep CLIP_AUDIO_ENABLED /etc/sentinel_rtp_cam/env

# Check ffmpeg process
ps aux | grep ffmpeg
```
