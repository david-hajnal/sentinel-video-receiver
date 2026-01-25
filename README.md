# Sentinel Video Receiver

RTSP/RTP video receiver and clip recorder for Raspberry Pi with ONVIF motion detection support.

## Quick Install

**One-line installation** (downloads prebuilt binary):

```bash
curl -fsSL https://raw.githubusercontent.com/david-hajnal/sentinel-video-receiver/main/sentinel_rtp_cam/install.sh | sudo bash
```

This will:
- Download the latest prebuilt ARM binary from GitHub Releases
- Install to `/usr/local/bin/sentinel_rtp_cam`
- Create systemd service for automatic startup
- Set up configuration directory at `/etc/sentinel_rtp_cam/`

## Configuration

After installation, edit the configuration file:

```bash
sudo nano /etc/sentinel_rtp_cam/env
```

Required settings:
```bash
CAMERA_IP=192.168.1.100
CAMERA_USER=admin
CAMERA_PASSWORD=your_password
ADMIN_SERVER_URL=https://your-admin-server.com
```

Then restart the service:

```bash
sudo systemctl restart sentinel_rtp_cam
```

## Usage

Check service status:
```bash
sudo systemctl status sentinel_rtp_cam
```

View logs:
```bash
sudo journalctl -u sentinel_rtp_cam -f
```

Monitor with status script:
```bash
sudo /usr/local/bin/sentinel_rtp_cam_status.sh
```

## Update

Update to latest version:
```bash
curl -fsSL https://raw.githubusercontent.com/david-hajnal/sentinel-video-receiver/main/sentinel_rtp_cam/update.sh | sudo bash
```

## Build from Source

If prebuilt binaries aren't available for your platform:

```bash
curl -fsSL https://raw.githubusercontent.com/david-hajnal/sentinel-video-receiver/main/sentinel_rtp_cam/install.sh | sudo BUILD_FROM_SOURCE=1 bash
```

Or clone the repository:
```bash
git clone https://github.com/david-hajnal/sentinel-video-receiver.git
cd sentinel-video-receiver/sentinel_rtp_cam
sudo BUILD_FROM_SOURCE=1 ./install.sh
```

## Features

- RTSP/RTP video streaming from IP cameras
- H.264 stream depacketization
- ONVIF motion detection integration
- Automatic video clip recording on motion events
- Upload to admin server
- Systemd service with automatic restart
- Support for Raspberry Pi (armv7/aarch64)

## Documentation

- [Raspberry Pi Setup Guide](sentinel_rtp_cam/RASPBIAN_SETUP.md)
- [Deployment Documentation](sentinel_rtp_cam/README_DEPLOY.md)
- [Release Process](RELEASE.md)

## License

MIT
