use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

use sentinel_rtp_cam::agent_uplink::Uplink;
use sentinel_rtp_cam::core::rtp::RtpPacket;
use sentinel_rtp_cam::core::sdp::parse_sdp_video_track;
use sentinel_rtp_cam::event::{Event, EventBus, MotionStateBus};
use sentinel_rtp_cam::onvif::run_onvif_motion_poller;
use sentinel_rtp_cam::rtsp::interleaved::{read_interleaved_frame, InterleavedFrame};
use sentinel_rtp_cam::rtsp::rtsp::RtspClient;

#[derive(Debug, Clone)]
struct CamConfig {
    name: String,
    rtsp_url: String,
    stream_id: u32,
    transport: String,
    rtp_port: u16,
    rtcp_port: u16,
    camera_id: String,
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn load_cameras() -> Result<Vec<CamConfig>> {
    let mut cams = Vec::new();
    for i in 1..=4 {
        let prefix = format!("CAM{}_", i);
        let rtsp = match env_string(&format!("{prefix}RTSP_URL")) {
            Some(v) => v,
            None => continue,
        };
        let stream_id: u32 = env_string(&format!("{prefix}STREAM_ID"))
            .ok_or_else(|| anyhow!("Missing {prefix}STREAM_ID"))?
            .parse()?;
        let transport = env_string(&format!("{prefix}TRANSPORT")).unwrap_or_else(|| "tcp".to_string());
        let rtp_port: u16 = env_string(&format!("{prefix}RTP_PORT"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(5004 + (i as u16 - 1) * 2);
        let rtcp_port: u16 = env_string(&format!("{prefix}RTCP_PORT"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(rtp_port + 1);
        let camera_id = env_string(&format!("{prefix}CAMERA_ID")).unwrap_or_else(|| format!("cam-{}", i));

        cams.push(CamConfig {
            name: format!("cam{}", i),
            rtsp_url: rtsp,
            stream_id,
            transport: transport.to_lowercase(),
            rtp_port,
            rtcp_port,
            camera_id,
        });
    }
    if cams.is_empty() {
        bail!("No cameras configured (CAM1_RTSP_URL...)");
    }
    Ok(cams)
}

fn parse_rtsp_url(rtsp_url: &str) -> Result<(String, u16, String)> {
    let url = Url::parse(rtsp_url)?;
    let host = url.host_str().ok_or_else(|| anyhow!("RTSP URL missing host"))?;
    let port = url.port().unwrap_or(554);
    Ok((host.to_string(), port, url.path().to_string()))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if dotenvy::dotenv().is_err() {
        dotenvy::from_filename("../.env").ok();
    }

    let server_addr =
        env_string("SERVER_ADDR").ok_or_else(|| anyhow!("Missing SERVER_ADDR"))?;
    let token = env_string("AGENT_TOKEN").ok_or_else(|| anyhow!("Missing AGENT_TOKEN"))?;
    let agent_id = env_string("AGENT_ID").unwrap_or_else(|| "agent-1".to_string());

    let token_prefix: String = token.chars().take(6).collect();
    info!(
        server = %server_addr,
        agent_id = %agent_id,
        token_prefix = %token_prefix,
        "Agent configured"
    );

    let cams = load_cameras()?;
    info!(camera_count = cams.len(), "Loaded camera configs");
    for cam in &cams {
        info!(
            camera_id = %cam.camera_id,
            stream_id = cam.stream_id,
            transport = %cam.transport,
            rtp_port = cam.rtp_port,
            rtcp_port = cam.rtcp_port,
            "Camera configured"
        );
    }

    let mut stream_map = HashMap::new();
    let mut camera_to_stream = HashMap::new();
    for cam in &cams {
        stream_map.insert(cam.stream_id, cam.name.clone());
        camera_to_stream.insert(cam.camera_id.clone(), cam.stream_id);
    }

    let uplink = Uplink::connect_and_run(server_addr, token, agent_id, stream_map);

    let cancel = CancellationToken::new();

    for cam in cams.clone() {
        let uplink = uplink.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = run_camera(cam, uplink, cancel).await {
                warn!(error = %e, "Camera task ended");
            }
        });
    }

    // ONVIF integration (single poller, map by camera_id -> stream_id)
    let bus = EventBus::new(128);
    let (motion_state, _rx) = MotionStateBus::new();

    let uplink_motion = uplink.clone();
    let cam_map = camera_to_stream.clone();
    let bus_sub = bus.clone();
    tokio::spawn(async move {
        let mut rx = bus_sub.subscribe().await;
        while let Some(ev) = rx.recv().await {
            let Event::Motion(m) = ev;
            let stream_id = cam_map
                .get(&m.camera_id)
                .copied()
                .or_else(|| cam_map.values().next().copied())
                .unwrap_or(1);
            info!(
                stream_id,
                camera_id = %m.camera_id,
                event_id = %m.event_id,
                active = m.active,
                rule = %m.rule,
                "Forwarding motion event"
            );
            uplink_motion.send_motion(
                stream_id,
                m.rule.clone(),
                m.active,
                m.ts.to_rfc3339(),
                m.camera_id.clone(),
                m.event_id.clone(),
            );
        }
    });

    tokio::spawn(async move {
        if let Err(e) = run_onvif_motion_poller(bus, motion_state).await {
            warn!(error = %e, "ONVIF motion poller ended");
        }
    });

    tokio::signal::ctrl_c().await?;
    cancel.cancel();
    Ok(())
}

async fn run_camera(cam: CamConfig, uplink: Uplink, cancel: CancellationToken) -> Result<()> {
    let (host, port, path) = parse_rtsp_url(&cam.rtsp_url)?;
    info!(
        stream_id = cam.stream_id,
        transport = %cam.transport,
        host = %host,
        port,
        path = %path,
        "Starting camera forwarder"
    );

    let mut c = RtspClient::connect(&host, port).await?;

    let r = c.request("OPTIONS", &cam.rtsp_url, &[], None).await?;
    if r.status != 200 {
        bail!("OPTIONS failed: {}", r.status);
    }
    info!(stream_id = cam.stream_id, "RTSP OPTIONS ok");
    let r = c
        .request(
            "DESCRIBE",
            &cam.rtsp_url,
            &[("Accept", "application/sdp")],
            None,
        )
        .await?;
    if r.status != 200 {
        bail!("DESCRIBE failed: {}", r.status);
    }
    info!(stream_id = cam.stream_id, "RTSP DESCRIBE ok");
    let sdp = String::from_utf8_lossy(&r.body);
    let track = parse_sdp_video_track(&sdp)?;

    let setup_url = if track.control.starts_with("rtsp://") {
        track.control.clone()
    } else {
        format!(
            "{}/{}",
            cam.rtsp_url.trim_end_matches('/'),
            track.control.trim_start_matches('/')
        )
    };

    if cam.transport == "udp" {
        let transport = format!(
            "RTP/AVP;unicast;client_port={}-{};mode=play",
            cam.rtp_port, cam.rtcp_port
        );
        let r = c.request("SETUP", &setup_url, &[("Transport", &transport)], None).await?;
        if r.status != 200 {
            bail!("SETUP failed: {}", r.status);
        }
        c.set_session_from(&r);
        info!(stream_id = cam.stream_id, "RTSP SETUP ok (UDP)");

        let r = c.request("PLAY", &cam.rtsp_url, &[("Range", "npt=0.000-")], None).await?;
        if r.status != 200 {
            bail!("PLAY failed: {}", r.status);
        }
        info!(stream_id = cam.stream_id, "RTSP PLAY ok");

        let sock = UdpSocket::bind(("0.0.0.0", cam.rtp_port)).await?;
        info!(stream_id = cam.stream_id, port = cam.rtp_port, "UDP RTP socket bound");
        let mut buf = vec![0u8; 8192];
        let mut expected_seq: Option<u16> = None;
        let mut total_pkts: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut last_log = Instant::now();
        let mut last_pkts = 0u64;
        let mut last_bytes = 0u64;
        let mut first_logged = false;
        let mut gap_count: u64 = 0;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                r = sock.recv_from(&mut buf) => {
                    let (n, _) = r?;
                    total_pkts += 1;
                    total_bytes += n as u64;
                    let pkt = match RtpPacket::parse(&buf[..n]) {
                        Ok(p) => p,
                        Err(_) => { expected_seq = None; continue; }
                    };
                    if !first_logged {
                        info!(stream_id = cam.stream_id, seq = pkt.sequence_number, "First RTP packet received");
                        first_logged = true;
                    }
                    if let Some(exp) = expected_seq {
                        if pkt.sequence_number != exp {
                            gap_count += 1;
                            if gap_count <= 3 || gap_count % 100 == 0 {
                                warn!(
                                    stream_id = cam.stream_id,
                                    expected = exp,
                                    got = pkt.sequence_number,
                                    gap_count,
                                    "RTP sequence gap detected"
                                );
                            }
                            uplink.send_gap(cam.stream_id, exp.wrapping_sub(1), pkt.sequence_number);
                            expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                            continue;
                        }
                    }
                    expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                    uplink.send_rtp(cam.stream_id, buf[..n].to_vec());

                    if last_log.elapsed() >= Duration::from_secs(30) {
                        info!(
                            stream_id = cam.stream_id,
                            pkts_total = total_pkts,
                            pkts_delta = total_pkts.saturating_sub(last_pkts),
                            bytes_total = total_bytes,
                            bytes_delta = total_bytes.saturating_sub(last_bytes),
                            "RTP receive stats"
                        );
                        last_log = Instant::now();
                        last_pkts = total_pkts;
                        last_bytes = total_bytes;
                    }
                }
            }
        }
    } else {
        let transport = "RTP/AVP/TCP;unicast;interleaved=0-1;mode=play";
        let r = c.request("SETUP", &setup_url, &[("Transport", transport)], None).await?;
        if r.status != 200 {
            bail!("SETUP failed: {}", r.status);
        }
        c.set_session_from(&r);
        info!(stream_id = cam.stream_id, "RTSP SETUP ok (TCP interleaved)");

        let r = c.request("PLAY", &cam.rtsp_url, &[("Range", "npt=0.000-")], None).await?;
        if r.status != 200 {
            bail!("PLAY failed: {}", r.status);
        }
        info!(stream_id = cam.stream_id, "RTSP PLAY ok");

        let mut total_pkts: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut last_log = Instant::now();
        let mut last_pkts = 0u64;
        let mut last_bytes = 0u64;
        let mut first_logged = false;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                frame = read_interleaved_frame(&mut c, 0, 1) => {
                    match frame? {
                        InterleavedFrame::Rtp(bytes) => {
                            total_pkts += 1;
                            total_bytes += bytes.len() as u64;
                            if !first_logged {
                                info!(stream_id = cam.stream_id, "First RTP frame received (TCP)");
                                first_logged = true;
                            }
                            uplink.send_rtp(cam.stream_id, bytes);

                            if last_log.elapsed() >= Duration::from_secs(30) {
                                info!(
                                    stream_id = cam.stream_id,
                                    pkts_total = total_pkts,
                                    pkts_delta = total_pkts.saturating_sub(last_pkts),
                                    bytes_total = total_bytes,
                                    bytes_delta = total_bytes.saturating_sub(last_bytes),
                                    "RTP receive stats"
                                );
                                last_log = Instant::now();
                                last_pkts = total_pkts;
                                last_bytes = total_bytes;
                            }
                        }
                        InterleavedFrame::Rtcp(_) => {}
                        InterleavedFrame::Unknown(_, _) => {}
                    }
                }
            }
        }
    }

    Ok(())
}
