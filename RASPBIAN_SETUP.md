# Running Sentinel Agent on Raspbian/Raspberry Pi OS

This guide covers setting up and running the Sentinel video receiver agent on a Raspberry Pi running Raspbian (Raspberry Pi OS).

## Prerequisites

- Raspberry Pi (3B+, 4, or 5 recommended)
- Raspbian/Raspberry Pi OS (64-bit recommended)
- ONVIF-compatible IP camera on the same network
- Network connectivity to the Sentinel admin server

## 1. System Setup

Update your system:
```bash
sudo apt update
sudo apt upgrade -y
```

Install required dependencies:
```bash
sudo apt install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    ffmpeg \
    git \
    curl
```

## 2. Install Rust

**Check available disk space first:**
```bash
df -h
# Ensure you have at least 2GB free space
```

Install Rust using rustup with **minimal profile** (recommended for Raspberry Pi):
```bash
# Minimal installation (saves ~600MB by skipping docs)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y
source $HOME/.cargo/env
```

**Alternative:** If you have plenty of disk space (32GB+ SD card):
```bash
# Standard installation (includes documentation and extra tools)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

Verify installation:
```bash
rustc --version
cargo --version
```

**If installation fails with "No space left on device":**
```bash
# Clean up failed installation
rm -rf ~/.rustup ~/.cargo

# Free up space
sudo apt clean
sudo apt autoremove

# Retry with minimal profile
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y
source $HOME/.cargo/env
```

## 3. Clone and Build

Clone the repository:
```bash
cd ~
git clone https://github.com/yourusername/sentinel-video-receiver.git
cd sentinel-video-receiver/sentinel_rtp_cam
```

Build the application (this may take 10-30 minutes on a Raspberry Pi):
```bash
cargo build --release --bin app
```

The compiled binary will be at: `target/release/app`

## 4. Configuration

Create a `.env` file:
```bash
cp .env.example .env
nano .env
```

Configure the following essential settings:

```bash
# Camera RTSP Configuration
RTSP_URL='rtsp://192.168.1.100:554/stream1'
RTSP_HOST='192.168.1.100'
RTSP_PORT='554'
RTSP_USER='admin'
RTSP_PASS='your_camera_password'

# ONVIF Motion Detection
ONVIF_HOST=192.168.1.100
ONVIF_PORT=80
ONVIF_USER='admin'
ONVIF_PASS='your_camera_password'

# Server Configuration
SERVER_BASE_URL='http://your-server-ip:8000'
SERVER_BEARER_TOKEN='devtoken'
CAMERA_ID='pi-cam-001'

# Clip Recording
CLIP_DIR=clips
CLIP_PRE_ROLL_SECS=2
CLIP_POST_ROLL_SECS=3
CLIP_FPS=25
CLIP_STREAM_COPY=1

# Clip Management (optional - limit storage usage)
CLIP_MAX_FILES=100
CLIP_MAX_AGE_SECS=604800
CLIP_MAX_TOTAL_BYTES=10737418240
CLIP_MAX_BYTES=52428800
CLIP_MAX_SECS=60
```

Create clips directory:
```bash
mkdir -p clips
```

## 5. Running the Agent

### Manual Run (for testing)

```bash
./target/release/app
```

Press Ctrl+C to stop.

### Run as Systemd Service (recommended for production)

Create a systemd service file:
```bash
sudo nano /etc/systemd/system/sentinel-agent.service
```

Add the following content (adjust paths as needed):
```ini
[Unit]
Description=Sentinel Video Receiver Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/sentinel-video-receiver/sentinel_rtp_cam
ExecStart=/home/pi/sentinel-video-receiver/sentinel_rtp_cam/target/release/app
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Environment variables (alternatively, use EnvironmentFile=/path/to/.env)
Environment="RUST_LOG=info"

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable sentinel-agent
sudo systemctl start sentinel-agent
```

Check status:
```bash
sudo systemctl status sentinel-agent
```

View logs:
```bash
sudo journalctl -u sentinel-agent -f
```

## 6. Performance Optimization for Raspberry Pi

### Use Stream Copy Mode
Set `CLIP_STREAM_COPY=1` to avoid re-encoding video, which is CPU-intensive.

### Limit Clip Storage
Configure storage limits to prevent SD card from filling up:
```bash
CLIP_MAX_FILES=50
CLIP_MAX_TOTAL_BYTES=5368709120  # 5GB
```

### Monitor Resource Usage
```bash
# Check CPU/memory usage
htop

# Check disk usage
df -h

# Check temperature
vcgencmd measure_temp
```

### Cooling
For continuous operation, ensure adequate cooling:
- Use a heatsink
- Consider an active cooling fan for Pi 4/5
- Monitor temperature: `watch -n 2 vcgencmd measure_temp`

## 7. Troubleshooting

### Camera Connection Issues
```bash
# Test RTSP connectivity
ffmpeg -rtsp_transport tcp -i rtsp://admin:password@192.168.1.100:554/stream1 -frames:v 1 test.jpg

# Check ONVIF connectivity
curl -v http://192.168.1.100:80/onvif/device_service
```

### High CPU Usage
- Enable `CLIP_STREAM_COPY=1`
- Reduce FPS: `CLIP_FPS=15`
- Use lower resolution stream from camera

### Network Issues
```bash
# Check connectivity to server
curl -v http://your-server-ip:8000/api/heartbeat \
  -H "Authorization: Bearer devtoken" \
  -H "Content-Type: application/json" \
  -d '{"camera_id":"pi-cam-001","timestamp":"2026-01-23T12:00:00Z"}'
```

### Storage Full
```bash
# Check disk usage
df -h

# Clean old clips manually
find ~/sentinel-video-receiver/sentinel_rtp_cam/clips -name "*.mp4" -mtime +7 -delete
```

### Service Not Starting
```bash
# Check logs
sudo journalctl -u sentinel-agent -n 50 --no-pager

# Test manually first
cd ~/sentinel-video-receiver/sentinel_rtp_cam
./target/release/app
```

## 8. Updating the Agent

```bash
cd ~/sentinel-video-receiver
git pull
cd sentinel_rtp_cam
cargo build --release --bin app
sudo systemctl restart sentinel-agent
```

## 9. Security Recommendations

1. **Change default passwords** on your camera
2. **Use strong bearer token** for server authentication
3. **Firewall**: Only allow necessary ports
4. **Disable SSH password auth**: Use SSH keys only
5. **Keep system updated**: `sudo apt update && sudo apt upgrade`

## 10. Hardware Recommendations

### For H.264 Stream Copy (Recommended)
- **Raspberry Pi 3B+**: Works well with 1-2 cameras
- **Raspberry Pi 4 (2GB+)**: Handles 2-4 cameras
- **Raspberry Pi 5**: Best performance for multiple cameras

### Storage
- **Class 10 microSD card** (minimum 32GB recommended)
  - **16GB minimum** if using minimal Rust profile and limiting clip storage
  - **64GB+** recommended for production with multiple cameras
- **USB SSD** for better performance and longevity

### Power Supply
- Use official Raspberry Pi power supply
- Ensure stable 5V supply to prevent crashes

## Support

For issues or questions:
- Check logs: `sudo journalctl -u sentinel-agent -f`
- Review troubleshooting section above
- Check project README and documentation
