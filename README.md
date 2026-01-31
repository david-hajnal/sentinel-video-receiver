# Sentinel Video Receiver

RTSP/RTP video receiver and clip recorder for Raspberry Pi with ONVIF motion detection support.

## Quick Install (Manual)

We build on GitHub Actions and ship prebuilt binaries. Raspberry Pi devices only download and run
those artifacts.

1) Download the latest release binary for your architecture from GitHub Releases.
2) Install the binary:
```bash
sudo install -m 755 sentinel_rtp_cam /usr/local/bin/sentinel_rtp_cam
```
3) Install the systemd unit:
```bash
sudo install -m 644 sentinel_rtp_cam/sentinel_rtp_cam.service /etc/systemd/system/sentinel_rtp_cam.service
sudo systemctl daemon-reload
```
4) Create the config directory and config file:
```bash
sudo install -d -m 755 /etc/sentinel_rtp_cam
sudo nano /etc/sentinel_rtp_cam/env
```
5) Start the service:
```bash
sudo systemctl enable --now sentinel_rtp_cam
```

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
curl -fsSL https://raw.githubusercontent.com/david-hajnal/sentinel-tooling/main/scripts/update.sh | sudo bash
```

## Build from Source

We do not build on Raspberry Pi. Use GitHub workflows to produce releases and update the device
with `update.sh` from https://github.com/david-hajnal/sentinel-tooling.

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
