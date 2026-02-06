# Sentinel Video Receiver

RTSP/RTP receiver and forward agent for Raspberry Pi with ONVIF motion detection.

## Quick install (recommended)

Uses the tooling repo: `david-hajnal/sentinel-tooling`.

```bash
curl -fsSL https://raw.githubusercontent.com/david-hajnal/sentinel-tooling/main/init.sh -o /tmp/sentinel-init.sh
sudo bash /tmp/sentinel-init.sh
```

The installer drops `sentinel-manage` and runs `sentinel-manage init`.
When the wizard finishes, start the agent:

```bash
sudo sentinel-manage start
```

## Configuration

The agent reads JSON config files in `/etc/sentinel_rtp_cam`:

- `server.json` (admin server base URL + bearer token)
- `camera.json` (camera + forward config)

Use the manage tool (from `sentinel-tooling`) to edit them:

```bash
sudo sentinel-manage config server
sudo sentinel-manage config camera
```

If a server is configured, the agent will pull camera config from it.

## Service management

```bash
sudo sentinel-manage status
sudo sentinel-manage logs
sudo sentinel-manage start
sudo sentinel-manage stop
sudo sentinel-manage restart
```

## Update

Install the latest release (does not auto-start):

```bash
sudo sentinel-manage update latest
```

If you want it to restart services:

```bash
sudo sentinel-manage update latest --start
```

## Build from source

We do not build on Raspberry Pi. Releases are built in GitHub Actions and deployed via
`sentinel-manage update` from the `sentinel-tooling` repo.

## License

MIT
