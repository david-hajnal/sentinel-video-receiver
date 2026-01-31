# Sentinel RTP Camera - Deployment Guide

Production deployment guide for Raspberry Pi OS (32-bit and 64-bit).

**Builds are produced on GitHub Actions.** Raspberry Pi devices only download and run prebuilt
artifacts. We do **not** build on the Pi (it is too slow/unreliable).

## Quick Start

```bash
# 1. Install binary + service (manual)
# Download release from GitHub, then:
sudo install -m 755 sentinel_rtp_cam /usr/local/bin/sentinel_rtp_cam
sudo install -m 644 sentinel_rtp_cam.service /etc/systemd/system/sentinel_rtp_cam.service
sudo systemctl daemon-reload

# 2. Configure
sudo install -d -m 755 /etc/sentinel_rtp_cam
sudo nano /etc/sentinel_rtp_cam/env

# 3. Start
sudo systemctl enable --now sentinel_rtp_cam

# 4. Check status
sudo systemctl status sentinel_rtp_cam
```

## Prerequisites

- Raspberry Pi 3 or newer (2GB+ RAM recommended)
- Raspberry Pi OS (32-bit or 64-bit)
- Root/sudo access
- Network connectivity to camera and server
- ONVIF-compatible IP camera

## Installation

### Method 1: Install from Prebuilt Binary (Required)

```bash
# Download the correct release tarball for your architecture
# https://github.com/david-hajnal/sentinel-video-receiver/releases

# Extract and install
tar xzf sentinel_rtp_cam-<version>-<arch>.tar.gz
sudo install -m 755 sentinel_rtp_cam /usr/local/bin/sentinel_rtp_cam
sudo install -m 644 sentinel_rtp_cam.service /etc/systemd/system/sentinel_rtp_cam.service
sudo systemctl daemon-reload
sudo install -d -m 755 /etc/sentinel_rtp_cam
sudo nano /etc/sentinel_rtp_cam/env
```

### Method 2: Build from Source (Not supported on Pi)

We do **not** build on Raspberry Pi. If you need a new version, build it on GitHub
(via workflows) and update the Pi using `update.sh`.

### Custom Version Installation

Download the desired version from GitHub Releases and install the binary/service as above.

## Configuration

### Edit Configuration File

```bash
sudo nano /etc/sentinel_rtp_cam/env
```

### Required Settings

```bash
# Camera RTSP
RTSP_URL=rtsp://192.168.1.100:554/stream1
RTSP_USER=admin
RTSP_PASS=your_camera_password

# ONVIF Motion Detection
ONVIF_HOST=192.168.1.100
ONVIF_PORT=80
ONVIF_USER=admin
ONVIF_PASS=your_camera_password

# Server
SERVER_BASE_URL=http://your-server:8000
SERVER_BEARER_TOKEN=your_token_here
CAMERA_ID=pi-cam-001
```

### Optional Settings

```bash
# Storage limits (recommended for SD cards)
CLIP_MAX_FILES=100
CLIP_MAX_TOTAL_BYTES=5368709120  # 5GB
CLIP_MAX_AGE_SECS=604800          # 7 days

# Performance (use stream copy to save CPU)
CLIP_STREAM_COPY=1
CLIP_FPS=25

# Debug
RUST_LOG=info
# ONVIF_DEBUG=1
```

## Service Management

### Start Service

```bash
sudo systemctl start sentinel_rtp_cam
```

### Stop Service

```bash
sudo systemctl stop sentinel_rtp_cam
```

### Restart Service

```bash
sudo systemctl restart sentinel_rtp_cam
```

### Enable Auto-Start

```bash
sudo systemctl enable sentinel_rtp_cam
```

### Disable Auto-Start

```bash
sudo systemctl disable sentinel_rtp_cam
```

### Check Status

```bash
# Quick status
sudo systemctl status sentinel_rtp_cam

# Detailed status with logs
sudo ./status.sh

# Custom log count
sudo ./status.sh --logs 100
```

## Viewing Logs

### Follow Live Logs

```bash
sudo journalctl -u sentinel_rtp_cam -f
```

### Recent Logs

```bash
# Last 50 lines
sudo journalctl -u sentinel_rtp_cam -n 50

# Last hour
sudo journalctl -u sentinel_rtp_cam --since "1 hour ago"

# Today's logs
sudo journalctl -u sentinel_rtp_cam --since today
```

### Error Logs Only

```bash
sudo journalctl -u sentinel_rtp_cam -p err
```

### Export Logs

```bash
sudo journalctl -u sentinel_rtp_cam --since "2026-01-20" > logs.txt
```

## Updates

### Update to Latest Version

```bash
sudo ./update.sh
```

The update script will:
- ✓ Download new version
- ✓ Verify checksum (if configured)
- ✓ Create backup of current version
- ✓ Install new binary atomically
- ✓ Restart service
- ✓ Verify service health
- ✓ Auto-rollback on failure

### Service Management Helper

Use `manage.sh` to start/stop/restart and view logs:

```bash
sudo ./manage.sh start
sudo ./manage.sh restart
sudo ./manage.sh logs
```

### Update to Specific Version

```bash
sudo SENTINEL_VERSION=1.2.3 ./update.sh
```

### Dry-Run Mode

Test update without making changes:

```bash
sudo ./update.sh --dry-run
```

### Update Configuration

To use a custom artifact server:

```bash
# Edit environment for updates
export SENTINEL_BASE_URL=https://your-releases.com/sentinel_rtp_cam
export SENTINEL_SHA256_URL=https://your-releases.com/checksums

# Run update
sudo -E ./update.sh
```

## Rollback

### Automatic Rollback

The update script automatically rolls back if the service fails health checks.

### Manual Rollback

If you need to manually rollback:

```bash
# Restore previous version
sudo cp /usr/local/bin/sentinel_rtp_cam.prev /usr/local/bin/sentinel_rtp_cam

# Restart service
sudo systemctl restart sentinel_rtp_cam

# Verify
sudo systemctl status sentinel_rtp_cam
```

## Troubleshooting

### Service Won't Start

```bash
# Check detailed logs
sudo journalctl -u sentinel_rtp_cam -n 100

# Common issues:
# 1. Configuration errors
sudo nano /etc/sentinel_rtp_cam/env

# 2. Camera not reachable
ping 192.168.1.100

# 3. Permissions
sudo ls -la /var/lib/sentinel_rtp_cam
```

### High CPU Usage

```bash
# Check process stats
sudo ./status.sh

# Enable stream copy to reduce encoding
sudo nano /etc/sentinel_rtp_cam/env
# Set: CLIP_STREAM_COPY=1

sudo systemctl restart sentinel_rtp_cam
```

### Storage Full

```bash
# Check storage
df -h /var/lib/sentinel_rtp_cam

# Configure limits
sudo nano /etc/sentinel_rtp_cam/env
# Set: CLIP_MAX_FILES=50
# Set: CLIP_MAX_TOTAL_BYTES=2147483648  # 2GB

sudo systemctl restart sentinel_rtp_cam
```

### Connection Issues

```bash
# Test camera RTSP
ffmpeg -rtsp_transport tcp -i rtsp://admin:pass@192.168.1.100:554/stream1 -frames:v 1 test.jpg

# Test ONVIF
curl -v http://192.168.1.100:80/onvif/device_service

# Test server connectivity
curl -v http://your-server:8000/api/heartbeat \
  -H "Authorization: Bearer your_token" \
  -H "Content-Type: application/json" \
  -d '{"camera_id":"pi-cam-001","timestamp":"2026-01-23T12:00:00Z"}'
```

### Service Crashes

```bash
# Check for crashes
sudo journalctl -u sentinel_rtp_cam --since "1 hour ago" -p err

# Check system resources
free -h
df -h

# Verify binary integrity
ls -lh /usr/local/bin/sentinel_rtp_cam
sudo /usr/local/bin/sentinel_rtp_cam --version
```

## File Locations

| Purpose | Path |
|---------|------|
| Binary | `/usr/local/bin/sentinel_rtp_cam` |
| Backup binary | `/usr/local/bin/sentinel_rtp_cam.prev` |
| Configuration | `/etc/sentinel_rtp_cam/env` |
| State/data | `/var/lib/sentinel_rtp_cam/` |
| Clips | `/var/lib/sentinel_rtp_cam/clips/` |
| Service file | `/etc/systemd/system/sentinel_rtp_cam.service` |
| Logs | `journalctl -u sentinel_rtp_cam` |

## Security

### Configuration File Permissions

The configuration file contains sensitive credentials:

```bash
# Verify permissions (should be 640, root:root)
ls -l /etc/sentinel_rtp_cam/env

# Fix if needed
sudo chmod 640 /etc/sentinel_rtp_cam/env
sudo chown root:root /etc/sentinel_rtp_cam/env
```

### Service Hardening

The systemd service runs with security hardening:
- Non-root user (`sentinel`)
- No privilege escalation
- Private tmp directory
- Protected system directories
- Namespace restrictions

### Network Security

- Change default camera passwords
- Use strong bearer tokens
- Consider firewall rules
- Use HTTPS for server communication (update `SERVER_BASE_URL`)

## Uninstallation

```bash
# Stop and disable service
sudo systemctl stop sentinel_rtp_cam
sudo systemctl disable sentinel_rtp_cam

# Remove service file
sudo rm /etc/systemd/system/sentinel_rtp_cam.service
sudo systemctl daemon-reload

# Remove binary
sudo rm /usr/local/bin/sentinel_rtp_cam
sudo rm -f /usr/local/bin/sentinel_rtp_cam.prev
sudo rm -f /usr/local/bin/sentinel_rtp_cam.new

# Remove configuration (warning: this deletes your settings)
sudo rm -rf /etc/sentinel_rtp_cam

# Remove data (warning: this deletes all clips)
sudo rm -rf /var/lib/sentinel_rtp_cam

# Remove user
sudo userdel sentinel
```

## Monitoring

### Health Checks

```bash
# Quick health check
sudo ./status.sh

# Check for recent errors
sudo journalctl -u sentinel_rtp_cam --since "5 minutes ago" -p err

# Monitor service status
watch -n 5 'sudo systemctl status sentinel_rtp_cam'
```

### Resource Monitoring

```bash
# CPU and memory
htop

# Disk usage
df -h
du -sh /var/lib/sentinel_rtp_cam/clips

# Temperature (important for Raspberry Pi)
vcgencmd measure_temp
```

## Advanced Configuration

### Version Pinning

To prevent automatic updates to a specific version:

```bash
sudo nano /etc/sentinel_rtp_cam/env

# Add this line:
SENTINEL_VERSION=1.2.3
```

### Custom Artifact Server

For air-gapped or private deployments:

```bash
# Set in install/update environment
export SENTINEL_BASE_URL=https://internal-releases.company.com/sentinel
export SENTINEL_SHA256_URL=https://internal-releases.company.com/checksums

sudo -E ./update.sh
```

### Multiple Instances

To run multiple instances (e.g., for multiple cameras):

1. Copy and rename service file
2. Modify `WorkingDirectory`, `EnvironmentFile`, etc.
3. Create separate config files with different `CAMERA_ID`

## Performance Tips

### For Raspberry Pi 3

```bash
# Use stream copy (no re-encoding)
CLIP_STREAM_COPY=1

# Reduce FPS
CLIP_FPS=15

# Limit storage
CLIP_MAX_FILES=50
CLIP_MAX_TOTAL_BYTES=2147483648
```

### For Raspberry Pi 4/5

```bash
# Can handle higher quality
CLIP_STREAM_COPY=1
CLIP_FPS=25
CLIP_MAX_FILES=100
```

## Support

- **Logs:** `sudo journalctl -u sentinel_rtp_cam -f`
- **Status:** `sudo ./status.sh`
- **Configuration:** `/etc/sentinel_rtp_cam/env`
- **Documentation:** Check project README

## Changelog

Keep track of your deployments:

```bash
# Check current version
sudo /usr/local/bin/sentinel_rtp_cam --version

# Review update history
sudo journalctl -u sentinel_rtp_cam | grep "Update completed"
```
