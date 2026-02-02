use anyhow::{anyhow, bail, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use sentinel_rtp_cam::agent_uplink::Uplink;
use sentinel_rtp_cam::core::rtp::RtpPacket;
use sentinel_rtp_cam::core::sdp::parse_sdp_video_track;
use sentinel_rtp_cam::event::{Event, EventBus, MotionStateBus};
use sentinel_rtp_cam::forward_agent::{
    basic_auth_value, build_stream_maps, forward_motion_event, load_cameras_from_env,
    parse_rtsp_url, CamConfig, MotionEventIdLatch,
};
use sentinel_rtp_cam::onvif::run_onvif_motion_poller;
use sentinel_rtp_cam::rtsp::interleaved::{read_interleaved_frame, InterleavedFrame};
use sentinel_rtp_cam::rtsp::rtsp::RtspClient;
use sentinel_rtp_cam::AgentConfig;

#[tokio::main]
async fn main() -> Result<()> {
    if dotenvy::dotenv().is_err() {
        dotenvy::from_filename("../.env").ok();
    }

    let legacy_config_path = std::env::var("AGENT_CONFIG_JSON")
        .or_else(|_| std::env::var("CONFIG_JSON_PATH"))
        .ok();
    let server_config_path = std::env::var("SERVER_CONFIG_JSON")
        .unwrap_or_else(|_| "/etc/sentinel_rtp_cam/server.json".to_string());
    let server_config_path = PathBuf::from(server_config_path);
    let camera_config_path = std::env::var("CAMERA_CONFIG_JSON")
        .ok()
        .or_else(|| legacy_config_path.clone())
        .unwrap_or_else(|| "/etc/sentinel_rtp_cam/camera.json".to_string());
    let camera_config_path = PathBuf::from(camera_config_path);
    let mut server_error: Option<String> = None;
    let mut camera_error: Option<String> = None;
    let use_legacy_server = std::env::var("SERVER_CONFIG_JSON").is_err()
        && !server_config_path.exists()
        && legacy_config_path.is_some();
    let use_legacy_camera =
        std::env::var("CAMERA_CONFIG_JSON").is_err() && legacy_config_path.is_some();
    let server_source_path = if use_legacy_server {
        PathBuf::from(legacy_config_path.clone().unwrap())
    } else {
        server_config_path.clone()
    };

    let server_value = match AgentConfig::load_server_json(&server_source_path) {
        Ok(value) => value,
        Err(e) => {
            server_error = Some(e.to_string());
            serde_json::Value::Object(Default::default())
        }
    };
    let camera_value = match AgentConfig::load_camera_json(&camera_config_path) {
        Ok(value) => value,
        Err(e) => {
            camera_error = Some(e.to_string());
            serde_json::Value::Object(Default::default())
        }
    };
    let config_value = AgentConfig::merge_server_camera_configs(&camera_value, &server_value);
    AgentConfig::apply_json_env_overrides(&config_value);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Some(error) = server_error {
        warn!(
            error = %error,
            path = %server_source_path.display(),
            "Failed to load server config JSON, using defaults"
        );
    }
    if let Some(error) = camera_error {
        warn!(
            error = %error,
            path = %camera_config_path.display(),
            "Failed to load camera config JSON, using defaults"
        );
    }
    if use_legacy_server {
        warn!(
            path = %server_source_path.display(),
            "SERVER_CONFIG_JSON unset; using legacy config for server settings"
        );
    }
    if use_legacy_camera {
        warn!(
            path = %camera_config_path.display(),
            "CAMERA_CONFIG_JSON unset; using legacy config path for camera settings"
        );
    }

    let server_addr = std::env::var("SERVER_ADDR").map_err(|_| anyhow!("Missing SERVER_ADDR"))?;

    let cams = load_cameras_from_env()?;
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

    let (stream_map, camera_to_stream) = build_stream_maps(&cams);
    let agent_token = cams
        .first()
        .map(|cam| cam.agent_token.clone())
        .ok_or_else(|| anyhow!("No cameras configured"))?;
    let agent_id = cams
        .first()
        .map(|cam| cam.agent_id.clone())
        .unwrap_or_else(|| "agent-1".to_string());
    let token_mismatch = cams.iter().any(|cam| cam.agent_token != agent_token);
    if token_mismatch {
        bail!("Multiple agent tokens found; single uplink requires a shared token");
    }
    let token_prefix: String = agent_token.chars().take(6).collect();
    info!(
        server = %server_addr,
        agent_id = %agent_id,
        token_prefix = %token_prefix,
        stream_count = stream_map.len(),
        "Agent uplink configured"
    );
    let uplink = Uplink::connect_and_run(server_addr, agent_token, agent_id, stream_map);

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

    let cam_map = camera_to_stream.clone();
    let uplink_motion = uplink.clone();
    let motion_merge_secs = std::env::var("MOTION_MERGE_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .or_else(|| {
            std::env::var("CLIP_POST_SECS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
        })
        .unwrap_or(0)
        .max(0);
    let motion_merge_window = chrono::Duration::seconds(motion_merge_secs);
    let bus_sub = bus.clone();
    tokio::spawn(async move {
        let mut rx = bus_sub.subscribe().await;
        let mut latch = MotionEventIdLatch::new_with_grace(motion_merge_window);
        while let Some(ev) = rx.recv().await {
            let Event::Motion(motion_event) = ev;
            let motion_event = latch.normalize(motion_event);
            let stream_id = forward_motion_event(&uplink_motion, &cam_map, &motion_event);
            info!(stream_id, motion_event = %motion_event, "Forwarding motion event");
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

    let authz = match (&cam.rtsp_user, &cam.rtsp_pass) {
        (Some(u), Some(p)) => Some(basic_auth_value(u, p)),
        (Some(_), None) | (None, Some(_)) => {
            warn!("RTSP_USER/RTSP_PASS mismatch; ignoring auth");
            None
        }
        _ => None,
    };
    let mut common_headers = Vec::new();
    if let Some(ref a) = authz {
        common_headers.push(("Authorization", a.as_str()));
    }

    let r = c
        .request("OPTIONS", &cam.rtsp_url, &common_headers, None)
        .await?;
    if r.status != 200 {
        bail!("OPTIONS failed: {}", r.status);
    }
    info!(stream_id = cam.stream_id, "RTSP OPTIONS ok");
    let mut describe_headers = vec![("Accept", "application/sdp")];
    if let Some(ref a) = authz {
        describe_headers.push(("Authorization", a.as_str()));
    }
    let r = c
        .request("DESCRIBE", &cam.rtsp_url, &describe_headers, None)
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
        let mut setup_headers = vec![("Transport", transport.as_str())];
        if let Some(ref a) = authz {
            setup_headers.push(("Authorization", a.as_str()));
        }
        let r = c.request("SETUP", &setup_url, &setup_headers, None).await?;
        if r.status != 200 {
            bail!("SETUP failed: {}", r.status);
        }
        c.set_session_from(&r);
        info!(stream_id = cam.stream_id, "RTSP SETUP ok (UDP)");

        let mut play_headers = vec![("Range", "npt=0.000-")];
        if let Some(ref a) = authz {
            play_headers.push(("Authorization", a.as_str()));
        }
        let r = c
            .request("PLAY", &cam.rtsp_url, &play_headers, None)
            .await?;
        if r.status != 200 {
            bail!("PLAY failed: {}", r.status);
        }
        info!(stream_id = cam.stream_id, "RTSP PLAY ok");

        let sock = UdpSocket::bind(("0.0.0.0", cam.rtp_port)).await?;
        info!(
            stream_id = cam.stream_id,
            port = cam.rtp_port,
            "UDP RTP socket bound"
        );
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
        let mut setup_headers = vec![("Transport", transport)];
        if let Some(ref a) = authz {
            setup_headers.push(("Authorization", a.as_str()));
        }
        let r = c.request("SETUP", &setup_url, &setup_headers, None).await?;
        if r.status != 200 {
            bail!("SETUP failed: {}", r.status);
        }
        c.set_session_from(&r);
        info!(stream_id = cam.stream_id, "RTSP SETUP ok (TCP interleaved)");

        let mut play_headers = vec![("Range", "npt=0.000-")];
        if let Some(ref a) = authz {
            play_headers.push(("Authorization", a.as_str()));
        }
        let r = c
            .request("PLAY", &cam.rtsp_url, &play_headers, None)
            .await?;
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
