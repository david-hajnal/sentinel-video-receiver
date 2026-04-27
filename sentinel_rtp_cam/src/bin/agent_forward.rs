use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use sentinel_rtp_cam::agent_uplink::Uplink;
use sentinel_rtp_cam::core::rtp::RtpPacket;
use sentinel_rtp_cam::core::sdp::parse_sdp_video_track;
use sentinel_rtp_cam::event::{Event, EventBus, MotionStateBus};
use sentinel_rtp_cam::forward_agent::{
    basic_auth_value, build_stream_maps, forward_motion_event, load_cameras_from_env,
    load_uplink_ingest_config_from_env, parse_rtsp_url, CamConfig, MotionEventIdLatch,
};
use sentinel_rtp_cam::onvif::run_onvif_motion_poller;
use sentinel_rtp_cam::rtsp::interleaved::{read_interleaved_frame, InterleavedFrame};
use sentinel_rtp_cam::rtsp::rtsp::RtspClient;
use sentinel_rtp_cam::{
    load_onvif_probe_cameras_from_env, run_agent_heartbeat_poster, AgentConfig,
    CameraHeartbeatTarget, OnvifProbeManager,
};

const DEFAULT_SERVER_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/server.json";
const DEFAULT_CAMERA_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/camera.json";
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;
const DEFAULT_CONFIG_PULL_INTERVAL_SECS: u64 = 30;

fn runtime_var(name: &str) -> Option<String> {
    AgentConfig::runtime_var(name).filter(|v| !v.trim().is_empty())
}

fn runtime_i64(name: &str) -> Option<i64> {
    runtime_var(name).and_then(|v| v.parse::<i64>().ok())
}

fn heartbeat_interval_secs() -> u64 {
    AgentConfig::runtime_u64_nonzero("HEARTBEAT_INTERVAL_SECS", DEFAULT_HEARTBEAT_INTERVAL_SECS)
}

fn config_pull_interval_secs() -> u64 {
    AgentConfig::runtime_u64_nonzero(
        "CONFIG_PULL_INTERVAL_SECS",
        DEFAULT_CONFIG_PULL_INTERVAL_SECS,
    )
}

fn apply_runtime_overrides_and_log_intervals(
    config_value: &Value,
    last_logged_intervals: &mut Option<(u64, u64)>,
) {
    AgentConfig::apply_json_env_overrides(config_value);
    let intervals = (heartbeat_interval_secs(), config_pull_interval_secs());
    if *last_logged_intervals != Some(intervals) {
        info!(
            heartbeat_interval_secs = intervals.0,
            config_pull_interval_secs = intervals.1,
            "Applied runtime interval settings"
        );
        *last_logged_intervals = Some(intervals);
    }
}

fn config_version_value(value: &Value) -> Option<Value> {
    value
        .get("config_version")
        .filter(|v| !v.is_null())
        .cloned()
}

fn config_camera_id(value: &Value) -> Option<&str> {
    value
        .get("camera_id")
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn build_camera_heartbeat_targets(
    cams: &[CamConfig],
    config_value: &Value,
) -> Vec<CameraHeartbeatTarget> {
    let camera_versions: HashMap<_, _> = config_value
        .get("cameras")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|camera| {
            Some((
                config_camera_id(camera)?.to_string(),
                config_version_value(camera),
            ))
        })
        .collect();

    cams.iter()
        .map(|cam| CameraHeartbeatTarget {
            camera_id: cam.camera_id.clone(),
            rtsp_url: cam.rtsp_url.clone(),
            config_version: camera_versions.get(&cam.camera_id).cloned().flatten(),
        })
        .collect()
}

#[derive(Debug, Clone)]
struct DesiredRuntime {
    server_addr: String,
    cams: Vec<CamConfig>,
    config_value: Value,
}

struct AgentRuntime {
    config_value: Value,
    uplink: Uplink,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl AgentRuntime {
    fn matches_config(&self, config_value: &Value) -> bool {
        self.config_value == *config_value
    }

    async fn stop(self) {
        self.cancel.cancel();
        self.uplink.shutdown();
        for task in self.tasks {
            task.abort();
        }
    }
}

fn desired_runtime_from_config(config_value: &Value) -> Result<Option<DesiredRuntime>> {
    let Some(server_addr) = runtime_var("SERVER_ADDR") else {
        return Ok(None);
    };
    let cams = load_cameras_from_env()?;
    if cams.is_empty() {
        return Ok(None);
    }
    Ok(Some(DesiredRuntime {
        server_addr,
        cams,
        config_value: config_value.clone(),
    }))
}

fn spawn_task<F>(future: F) -> JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future)
}

fn rtsp_status_error(cam: &CamConfig, method: &str, status: u16) -> anyhow::Error {
    if status == 401 {
        warn!(
            camera_id = %cam.camera_id,
            stream_id = cam.stream_id,
            method,
            "RTSP authentication failed; check RTSP username/password"
        );
    }
    anyhow!("{method} failed: {status}")
}

fn parse_interleaved_channels(transport: &str) -> Option<(u8, u8)> {
    for part in transport.split(';') {
        let token = part.trim();
        if let Some(value) = token.strip_prefix("interleaved=") {
            let (rtp, rtcp) = value.split_once('-')?;
            return Some((rtp.parse::<u8>().ok()?, rtcp.parse::<u8>().ok()?));
        }
    }
    None
}

fn format_rtsp_target(rtsp_url: &str) -> String {
    parse_rtsp_url(rtsp_url)
        .map(|(host, _port, path)| format!("{host}{path}"))
        .unwrap_or_else(|_| rtsp_url.to_string())
}

fn start_runtime(desired: &DesiredRuntime) -> Result<AgentRuntime> {
    let applied_config_version: Arc<RwLock<Option<Value>>> =
        Arc::new(RwLock::new(config_version_value(&desired.config_value)));
    let camera_targets = Arc::new(RwLock::new(build_camera_heartbeat_targets(
        &desired.cams,
        &desired.config_value,
    )));
    let agent_id = desired
        .cams
        .first()
        .map(|cam| cam.agent_id.clone())
        .unwrap_or_else(|| "agent-1".to_string());
    let onvif_probe_cameras = match load_onvif_probe_cameras_from_env() {
        Ok(cameras) => cameras,
        Err(error) => {
            warn!(
                error = %error,
                "Failed to load ONVIF probe camera config; capability probes disabled"
            );
            Vec::new()
        }
    };
    let onvif_probe = OnvifProbeManager::new(
        desired
            .cams
            .iter()
            .map(|cam| cam.camera_id.clone())
            .collect(),
        onvif_probe_cameras,
        Some(agent_id.clone()),
    );
    let mut tasks = Vec::new();
    if runtime_var("SERVER_BASE_URL").is_some() && runtime_var("SERVER_BEARER_TOKEN").is_some() {
        let server_cfg = AgentConfig::from_env()?.server;
        let applied_config_version = applied_config_version.clone();
        let camera_targets = camera_targets.clone();
        let heartbeat_agent_id = agent_id.clone();
        let onvif_probe = onvif_probe.clone();
        tasks.push(spawn_task(async move {
            if let Err(error) = run_agent_heartbeat_poster(
                server_cfg,
                heartbeat_agent_id,
                applied_config_version,
                camera_targets,
                onvif_probe,
            )
            .await
            {
                warn!(error = %error, "Agent heartbeat task ended");
            }
        }));
    }

    info!(camera_count = desired.cams.len(), "Loaded camera configs");
    for cam in &desired.cams {
        info!(
            camera_id = %cam.camera_id,
            stream_id = cam.stream_id,
            transport = %cam.transport,
            rtp_port = cam.rtp_port,
            rtcp_port = cam.rtcp_port,
            "Camera configured"
        );
    }

    let (stream_map, camera_to_stream) = build_stream_maps(&desired.cams);
    let stream_to_camera: HashMap<u32, String> = camera_to_stream
        .iter()
        .map(|(camera_id, stream_id)| (*stream_id, camera_id.clone()))
        .collect();
    let agent_token = desired
        .cams
        .first()
        .map(|cam| cam.agent_token.clone())
        .ok_or_else(|| anyhow!("No cameras configured"))?;
    let token_mismatch = desired
        .cams
        .iter()
        .any(|cam| cam.agent_token != agent_token);
    if token_mismatch {
        bail!("Multiple agent tokens found; single uplink requires a shared token");
    }
    let token_prefix: String = agent_token.chars().take(6).collect();
    let stream_ingest = load_uplink_ingest_config_from_env();
    info!(
        server = %desired.server_addr,
        agent_id = %agent_id,
        token_prefix = %token_prefix,
        stream_count = stream_map.len(),
        clip_pre_secs = ?stream_ingest.as_ref().and_then(|cfg| cfg.clip_pre_secs),
        clip_post_secs = ?stream_ingest.as_ref().and_then(|cfg| cfg.clip_post_secs),
        clip_ring_secs = ?stream_ingest.as_ref().and_then(|cfg| cfg.clip_ring_secs),
        clip_stale_part_secs = ?stream_ingest.as_ref().and_then(|cfg| cfg.clip_stale_part_secs),
        clip_max_secs = ?stream_ingest.as_ref().and_then(|cfg| cfg.clip_max_secs),
        "Agent uplink configured"
    );
    let uplink = Uplink::connect_and_run(
        desired.server_addr.clone(),
        agent_token,
        agent_id,
        stream_map,
        stream_to_camera,
        stream_ingest,
        HashMap::new(),
    );

    let cancel = CancellationToken::new();
    for cam in desired.cams.clone() {
        let uplink = uplink.clone();
        let cancel = cancel.clone();
        tasks.push(spawn_task(async move {
            if let Err(error) = run_camera(cam, uplink, cancel).await {
                warn!(error = %error, "Camera task ended");
            }
        }));
    }

    let bus = EventBus::new(128);
    let (motion_state, _rx) = MotionStateBus::new();

    let cam_map = camera_to_stream.clone();
    let uplink_motion = uplink.clone();
    let motion_merge_secs = runtime_i64("MOTION_MERGE_SECS")
        .or_else(|| runtime_i64("CLIP_POST_SECS"))
        .unwrap_or(0)
        .max(0);
    let motion_merge_window = chrono::Duration::seconds(motion_merge_secs);
    let bus_sub = bus.clone();
    tasks.push(spawn_task(async move {
        let mut rx = bus_sub.subscribe().await;
        let mut latch = MotionEventIdLatch::new_with_grace(motion_merge_window);
        while let Some(ev) = rx.recv().await {
            let Event::Motion(motion_event) = ev;
            let motion_event = latch.normalize(motion_event);
            let stream_id = forward_motion_event(&uplink_motion, &cam_map, &motion_event);
            info!(stream_id, motion_event = %motion_event, "Forwarding motion event");
        }
    }));

    tasks.push(spawn_task(async move {
        if let Err(error) = run_onvif_motion_poller(bus, motion_state).await {
            warn!(error = %error, "ONVIF motion poller ended");
        }
    }));

    Ok(AgentRuntime {
        config_value: desired.config_value.clone(),
        uplink,
        cancel,
        tasks,
    })
}

async fn try_pull_remote_config(
    client: &reqwest::Client,
    base_url: &str,
    bearer_token: &str,
    camera_hint: Option<String>,
    _server_path: &PathBuf,
    camera_path: &PathBuf,
) -> Result<bool> {
    let url = format!("{}/api/v1/config", base_url.trim_end_matches('/'));
    let mut resp = client.get(&url).bearer_auth(bearer_token).send().await?;
    let should_retry_with_hint =
        camera_hint.is_some() && should_retry_config_pull_with_hint(resp.status());
    if should_retry_with_hint {
        warn!("Agent config pull failed; retrying with camera hint");
        if let Some(hint) = camera_hint.as_deref() {
            resp = client
                .get(&url)
                .bearer_auth(bearer_token)
                .header("x-camera-id", hint)
                .send()
                .await?;
        }
    }
    if !resp.status().is_success() {
        warn!(
            status = %resp.status(),
            "Config pull failed; server returned error"
        );
        return Ok(false);
    }

    let payload: Value = resp.json().await?;
    let Some(config) = payload.get("config") else {
        warn!("Config pull response missing config payload");
        return Ok(false);
    };

    let mut camera_update = config.clone();
    if let Some(obj) = camera_update.as_object_mut() {
        obj.remove("server");
    }
    AgentConfig::merge_remote_camera_json(camera_path, &camera_update)?;

    Ok(true)
}

fn should_retry_config_pull_with_hint(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::NOT_FOUND
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let server_source_path = PathBuf::from(DEFAULT_SERVER_CONFIG_PATH);
    let server_config_path = server_source_path.clone();
    let camera_config_path = PathBuf::from(DEFAULT_CAMERA_CONFIG_PATH);
    let mut server_error: Option<String> = None;
    let mut camera_error: Option<String> = None;

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
    let config_value = match AgentConfig::merge_server_camera_configs(&camera_value, &server_value)
    {
        Ok(value) => value,
        Err(error) => {
            server_error = Some(error.to_string());
            serde_json::Value::Object(Default::default())
        }
    };
    let mut last_logged_intervals = None;
    apply_runtime_overrides_and_log_intervals(&config_value, &mut last_logged_intervals);

    let log_filter = runtime_var("RUST_LOG").unwrap_or_else(|| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
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

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut last_status = Instant::now() - Duration::from_secs(120);
    let mut last_pull = Instant::now() - Duration::from_secs(120);
    let mut runtime: Option<AgentRuntime> = None;

    loop {
        let server_value = match AgentConfig::load_server_json(&server_source_path) {
            Ok(value) => value,
            Err(e) => {
                if last_status.elapsed() > Duration::from_secs(30) {
                    warn!(
                        error = %e,
                        path = %server_source_path.display(),
                        "Failed to load server config JSON; waiting for config"
                    );
                }
                serde_json::Value::Object(Default::default())
            }
        };
        let mut camera_value = match AgentConfig::load_camera_json(&camera_config_path) {
            Ok(value) => value,
            Err(e) => {
                if last_status.elapsed() > Duration::from_secs(30) {
                    warn!(
                        error = %e,
                        path = %camera_config_path.display(),
                        "Failed to load camera config JSON; waiting for config"
                    );
                }
                serde_json::Value::Object(Default::default())
            }
        };
        let mut config_value =
            match AgentConfig::merge_server_camera_configs(&camera_value, &server_value) {
                Ok(value) => value,
                Err(error) => {
                    if last_status.elapsed() > Duration::from_secs(30) {
                        warn!(error = %error, "Invalid server base URL; waiting for config");
                    }
                    camera_value.clone()
                }
            };
        apply_runtime_overrides_and_log_intervals(&config_value, &mut last_logged_intervals);

        let mut desired = match desired_runtime_from_config(&config_value) {
            Ok(desired) => desired,
            Err(e) => {
                if last_status.elapsed() > Duration::from_secs(30) {
                    warn!(error = %e, "Camera config incomplete; waiting for config");
                }
                None
            }
        };

        let server_base_url = runtime_var("SERVER_BASE_URL");
        let server_bearer = runtime_var("SERVER_BEARER_TOKEN");

        if last_pull.elapsed() > Duration::from_secs(config_pull_interval_secs()) {
            if let (Some(base_url), Some(token)) = (&server_base_url, &server_bearer) {
                let camera_hint =
                    runtime_var("CAMERA_ID").or_else(|| runtime_var("CAM1_CAMERA_ID"));
                match try_pull_remote_config(
                    &http_client,
                    base_url,
                    token,
                    camera_hint,
                    &server_config_path,
                    &camera_config_path,
                )
                .await
                {
                    Ok(true) => {
                        info!("Pulled config from server; reloading");
                        match AgentConfig::load_camera_json(&camera_config_path) {
                            Ok(value) => {
                                camera_value = value;
                                config_value = match AgentConfig::merge_server_camera_configs(
                                    &camera_value,
                                    &server_value,
                                ) {
                                    Ok(value) => value,
                                    Err(error) => {
                                        warn!(
                                            error = %error,
                                            "Reloaded server config has invalid base URL"
                                        );
                                        camera_value.clone()
                                    }
                                };
                                apply_runtime_overrides_and_log_intervals(
                                    &config_value,
                                    &mut last_logged_intervals,
                                );
                                desired = match desired_runtime_from_config(&config_value) {
                                    Ok(desired) => desired,
                                    Err(e) => {
                                        warn!(
                                            error = %e,
                                            "Reloaded config is still incomplete after pull"
                                        );
                                        None
                                    }
                                };
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to reload camera config after pull");
                            }
                        }
                    }
                    Ok(false) => {
                        if last_status.elapsed() > Duration::from_secs(30) {
                            warn!("Config pull did not return a usable config");
                        }
                    }
                    Err(e) => {
                        if last_status.elapsed() > Duration::from_secs(30) {
                            warn!(error = %e, "Config pull failed");
                        }
                    }
                }
            }
            last_pull = Instant::now();
        }

        if let Some(ref desired_runtime) = desired {
            let needs_restart = runtime
                .as_ref()
                .map(|running| !running.matches_config(&desired_runtime.config_value))
                .unwrap_or(true);
            if needs_restart {
                match start_runtime(desired_runtime) {
                    Ok(new_runtime) => {
                        if let Some(old_runtime) = runtime.replace(new_runtime) {
                            old_runtime.stop().await;
                        }
                        info!(
                            camera_count = desired_runtime.cams.len(),
                            "Applied updated camera config"
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to apply updated camera config");
                    }
                }
            }
        } else {
            if let Some(old_runtime) = runtime.take() {
                old_runtime.stop().await;
                info!("Config no longer ready; agent entering standby");
            }
            if last_status.elapsed() > Duration::from_secs(30) {
                let server_hint = if runtime_var("SERVER_ADDR").is_some() {
                    "server_addr=ok"
                } else {
                    "server_addr=missing"
                };
                warn!(
                    camera_count = 0,
                    server_hint, "Agent in standby; waiting for forward config"
                );
                last_status = Instant::now();
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = sleep(Duration::from_secs(5)) => {}
        }
    }

    if let Some(runtime) = runtime.take() {
        runtime.stop().await;
    }
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
        return Err(rtsp_status_error(&cam, "OPTIONS", r.status));
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
        return Err(rtsp_status_error(&cam, "DESCRIBE", r.status));
    }
    info!(stream_id = cam.stream_id, "RTSP DESCRIBE ok");
    let sdp = String::from_utf8_lossy(&r.body);
    let track = parse_sdp_video_track(&sdp)?;
    if let (Some(sps), Some(pps)) = (track.sprop_sps.clone(), track.sprop_pps.clone()) {
        uplink.set_stream_h264_parameter_sets(cam.stream_id, sps, pps);
    }
    let has_sprop_parameter_sets = track.sprop_sps.is_some() && track.sprop_pps.is_some();
    info!(
        camera_id = %cam.camera_id,
        stream_id = cam.stream_id,
        payload_type = track.payload_type,
        codec = track.codec_name.as_deref().unwrap_or("unknown"),
        control_url = %track.control,
        has_sprop_parameter_sets,
        "Parsed SDP video track"
    );
    if let Some(codec) = track.codec_name.as_deref() {
        if !codec.eq_ignore_ascii_case("H264") {
            error!(
                camera_id = %cam.camera_id,
                stream_id = cam.stream_id,
                advertised_codec = %codec,
                "Live startup failed: live path is H.264-only"
            );
            bail!("Unsupported codec advertised in SDP: {codec}");
        }
    } else {
        warn!(
            camera_id = %cam.camera_id,
            stream_id = cam.stream_id,
            payload_type = track.payload_type,
            "SDP missing rtpmap codec for selected payload type; assuming H264"
        );
    }

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
        let requested_transport = format!(
            "RTP/AVP;unicast;client_port={}-{};mode=play",
            cam.rtp_port, cam.rtcp_port
        );
        let mut setup_headers = vec![("Transport", requested_transport.as_str())];
        if let Some(ref a) = authz {
            setup_headers.push(("Authorization", a.as_str()));
        }
        let r = c.request("SETUP", &setup_url, &setup_headers, None).await?;
        if r.status != 200 {
            return Err(rtsp_status_error(&cam, "SETUP", r.status));
        }
        let negotiated_transport = r
            .headers
            .iter()
            .find_map(|(k, v)| k.eq_ignore_ascii_case("Transport").then_some(v.as_str()))
            .unwrap_or("");
        info!(
            stream_id = cam.stream_id,
            camera_id = %cam.camera_id,
            requested_transport = %requested_transport,
            negotiated_transport = %negotiated_transport,
            "RTSP transport negotiated (UDP)"
        );
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
            return Err(rtsp_status_error(&cam, "PLAY", r.status));
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
                r = async {
                    if first_logged {
                        sock.recv_from(&mut buf).await.map(Some)
                    } else {
                        match timeout(Duration::from_secs(10), sock.recv_from(&mut buf)).await {
                            Ok(result) => result.map(Some),
                            Err(_) => {
                                error!(
                                    camera_id = %cam.camera_id,
                                    stream_id = cam.stream_id,
                                    transport = %cam.transport,
                                    rtsp_target = %format_rtsp_target(&cam.rtsp_url),
                                    requested_transport = %requested_transport,
                                    negotiated_transport = %negotiated_transport,
                                    "Live startup failed: no RTP received within 10s after PLAY"
                                );
                                Ok(None)
                            }
                        }
                    }
                } => {
                    let Some((n, _)) = r? else {
                        bail!("No RTP received within startup watchdog window");
                    };
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
        let requested_transport = "RTP/AVP/TCP;unicast;interleaved=0-1;mode=play";
        let mut setup_headers = vec![("Transport", requested_transport)];
        if let Some(ref a) = authz {
            setup_headers.push(("Authorization", a.as_str()));
        }
        let r = c.request("SETUP", &setup_url, &setup_headers, None).await?;
        if r.status != 200 {
            return Err(rtsp_status_error(&cam, "SETUP", r.status));
        }
        let negotiated_transport = r
            .headers
            .iter()
            .find_map(|(k, v)| k.eq_ignore_ascii_case("Transport").then_some(v.as_str()))
            .ok_or_else(|| anyhow!("SETUP response missing Transport header"))?;
        let (rtp_channel, rtcp_channel) = parse_interleaved_channels(negotiated_transport)
            .ok_or_else(|| anyhow!("SETUP response missing/invalid interleaved channels"))?;
        info!(
            stream_id = cam.stream_id,
            camera_id = %cam.camera_id,
            requested_transport = %requested_transport,
            negotiated_transport = %negotiated_transport,
            rtp_channel,
            rtcp_channel,
            "RTSP transport negotiated (TCP interleaved)"
        );
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
            return Err(rtsp_status_error(&cam, "PLAY", r.status));
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
                frame = async {
                    if first_logged {
                        read_interleaved_frame(&mut c, rtp_channel, rtcp_channel).await.map(Some)
                    } else {
                        match timeout(
                            Duration::from_secs(10),
                            read_interleaved_frame(&mut c, rtp_channel, rtcp_channel),
                        ).await {
                            Ok(result) => result.map(Some),
                            Err(_) => {
                                error!(
                                    camera_id = %cam.camera_id,
                                    stream_id = cam.stream_id,
                                    transport = %cam.transport,
                                    rtsp_target = %format_rtsp_target(&cam.rtsp_url),
                                    requested_transport = %requested_transport,
                                    negotiated_transport = %negotiated_transport,
                                    "Live startup failed: no RTP received within 10s after PLAY"
                                );
                                Ok(None)
                            }
                        }
                    }
                } => {
                    let Some(frame) = frame? else {
                        bail!("No RTP received within startup watchdog window");
                    };
                    match frame {
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

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_rtp_cam::forward_agent::resolve_stream_id;
    use serde_json::json;
    use std::fs;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
    }

    fn runtime_test_lock() -> &'static Mutex<()> {
        static RUNTIME_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        RUNTIME_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    impl EnvGuard {
        fn new() -> Self {
            let _lock = runtime_test_lock()
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            AgentConfig::clear_runtime_overrides();
            Self { _lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            AgentConfig::clear_runtime_overrides();
        }
    }

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sentinel-agent-forward-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn multi_camera_env_maps_test2_to_stream_two() {
        let _guard = EnvGuard::new();
        AgentConfig::apply_json_env_overrides(&json!({
            "config_version": 3,
            "agent_id": "01KH1V1AMEP4NDDDJ0SRV067TW",
            "server": {
                "bearer_token": "agent-token"
            },
            "cameras": [
                {
                    "id": "ABLAK",
                    "config_version": 11,
                    "rtsp": {
                        "url": "rtsp://192.168.1.187:554/stream2",
                        "stream_id": 1
                    },
                    "transport": "udp"
                },
                {
                    "id": "aff8812b-c6be-4e59-aefd-40b59b425d92",
                    "config_version": 12,
                    "rtsp": {
                        "url": "rtsp://192.168.1.189:554/stream2",
                        "stream_id": 2
                    },
                    "transport": "udp"
                }
            ]
        }));

        let cams = load_cameras_from_env().expect("load cameras");
        assert_eq!(cams.len(), 2);
        assert_eq!(cams[0].camera_id, "ABLAK");
        assert_eq!(cams[0].stream_id, 1);
        assert_eq!(cams[1].camera_id, "aff8812b-c6be-4e59-aefd-40b59b425d92");
        assert_eq!(cams[1].stream_id, 2);

        let (_stream_map, camera_to_stream) = build_stream_maps(&cams);
        assert_eq!(resolve_stream_id("ABLAK", &camera_to_stream), 1);
        assert_eq!(
            resolve_stream_id("aff8812b-c6be-4e59-aefd-40b59b425d92", &camera_to_stream),
            2
        );

        let targets = build_camera_heartbeat_targets(
            &cams,
            &json!({
                "config_version": 3,
                "cameras": [
                    {
                        "id": "ABLAK",
                        "config_version": 11
                    },
                    {
                        "id": "aff8812b-c6be-4e59-aefd-40b59b425d92",
                        "config_version": 12
                    }
                ]
            }),
        );
        assert_eq!(
            config_version_value(&json!({ "config_version": 3 })),
            Some(json!(3))
        );
        assert_eq!(targets[0].config_version, Some(json!(11)));
        assert_eq!(targets[1].config_version, Some(json!(12)));
    }

    #[test]
    fn rtsp_status_error_mentions_auth_for_401() {
        let cam = CamConfig {
            name: "cam1".to_string(),
            rtsp_url: "rtsp://192.168.1.187:554/stream2".to_string(),
            rtsp_user: Some("user".to_string()),
            rtsp_pass: Some("bad-pass".to_string()),
            stream_id: 1,
            transport: "udp".to_string(),
            rtp_port: 5004,
            rtcp_port: 5005,
            camera_id: "ABLAK".to_string(),
            agent_id: "ABLAK".to_string(),
            agent_token: "agent-token".to_string(),
        };

        let err = rtsp_status_error(&cam, "DESCRIBE", 401);

        assert_eq!(err.to_string(), "DESCRIBE failed: 401");
    }

    #[test]
    fn parse_interleaved_channels_reads_negotiated_pair() {
        let transport = "RTP/AVP/TCP;unicast;interleaved=2-3;mode=play";
        assert_eq!(parse_interleaved_channels(transport), Some((2, 3)));
    }

    #[test]
    fn parse_interleaved_channels_rejects_invalid_values() {
        assert_eq!(
            parse_interleaved_channels("RTP/AVP/TCP;unicast;interleaved=abc-def"),
            None
        );
        assert_eq!(
            parse_interleaved_channels("RTP/AVP/TCP;unicast;mode=play"),
            None
        );
    }

    #[test]
    fn desired_runtime_from_config_drops_removed_camera_slots() {
        let _guard = EnvGuard::new();
        let initial = json!({
            "server": {
                "bearer_token": "agent-token"
            },
            "forward_agent": {
                "server_addr": "127.0.0.1:7443"
            },
            "cameras": [
                {
                    "id": "ABLAK",
                    "rtsp": {
                        "url": "rtsp://192.168.1.187:554/stream2",
                        "stream_id": 1
                    },
                    "transport": "udp"
                },
                {
                    "id": "cam-2",
                    "rtsp": {
                        "url": "rtsp://192.168.1.189:554/stream2",
                        "stream_id": 2
                    },
                    "transport": "tcp"
                }
            ]
        });
        AgentConfig::apply_json_env_overrides(&initial);
        let runtime = desired_runtime_from_config(&initial)
            .expect("build desired runtime")
            .expect("runtime should be ready");
        assert_eq!(runtime.cams.len(), 2);
        assert_eq!(runtime.cams[1].camera_id, "cam-2");

        let updated = json!({
            "server": {
                "bearer_token": "agent-token"
            },
            "forward_agent": {
                "server_addr": "127.0.0.1:7443"
            },
            "cameras": [
                {
                    "id": "ABLAK",
                    "rtsp": {
                        "url": "rtsp://192.168.1.187:554/stream2",
                        "stream_id": 1
                    },
                    "transport": "udp"
                }
            ]
        });
        AgentConfig::apply_json_env_overrides(&updated);
        let runtime = desired_runtime_from_config(&updated)
            .expect("build updated desired runtime")
            .expect("runtime should stay ready");
        assert_eq!(runtime.cams.len(), 1);
        assert_eq!(runtime.cams[0].camera_id, "ABLAK");
    }

    #[test]
    fn desired_runtime_from_config_enters_standby_when_cameras_removed() {
        let _guard = EnvGuard::new();
        let config = json!({
            "server": {
                "bearer_token": "agent-token"
            },
            "forward_agent": {
                "server_addr": "127.0.0.1:7443"
            },
            "cameras": []
        });
        AgentConfig::apply_json_env_overrides(&config);

        assert!(desired_runtime_from_config(&config)
            .expect("evaluate desired runtime")
            .is_none());
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let n = stream.read(&mut buf).await.expect("read request");
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buf[..n]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn camera_hint_header(request: &str) -> Option<String> {
        request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("x-camera-id") {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
    }

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    async fn spawn_config_server() -> (String, tokio::task::JoinHandle<Vec<Option<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let base_url = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = tokio::spawn(async move {
            let mut observed = Vec::new();
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept request");
                let request = read_request(&mut stream).await;
                observed.push(camera_hint_header(&request));
                let response = if attempt == 0 {
                    http_response("400 Bad Request", r#"{"error":"camera hint required"}"#)
                } else {
                    let body = json!({
                        "config": {
                            "config_version": 3,
                            "server": {
                                "bearer_token": "remote-token"
                            },
                            "cameras": [
                                {
                                    "id": "ABLAK",
                                    "config_version": 11,
                                    "rtsp": { "url": "rtsp://192.168.1.187:554/stream2", "stream_id": 1 }
                                },
                                {
                                    "id": "aff8812b-c6be-4e59-aefd-40b59b425d92",
                                    "config_version": 12,
                                    "rtsp": { "url": "rtsp://192.168.1.189/stream2", "stream_id": 2 }
                                }
                            ]
                        }
                    })
                    .to_string();
                    http_response("200 OK", &body)
                };
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
            observed
        });
        (base_url, handle)
    }

    #[tokio::test]
    async fn remote_config_pull_tries_agent_config_before_camera_hint() {
        let dir = unique_test_dir("pull-order");
        fs::create_dir_all(&dir).expect("test dir");
        let camera_path = dir.join("camera.json");
        AgentConfig::write_json_file(
            &camera_path,
            &json!({
                "camera_id": "ABLAK",
                "cameras": [{
                    "id": "ABLAK",
                    "rtsp": { "url": "rtsp://192.168.1.187:554/stream2", "stream_id": 1 }
                }]
            }),
        )
        .expect("write starting camera config");
        let (base_url, requests) = spawn_config_server().await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("http client");

        let pulled = try_pull_remote_config(
            &client,
            &base_url,
            "agent-token",
            Some("ABLAK".to_string()),
            &dir.join("server.json"),
            &camera_path,
        )
        .await
        .expect("pull config");

        assert!(pulled);
        assert_eq!(
            requests.await.expect("request log"),
            vec![None, Some("ABLAK".to_string())]
        );
        let written = AgentConfig::load_camera_json(&camera_path).expect("read camera config");
        assert!(written.get("server").is_none());
        assert_eq!(written["config_version"], 3);
        assert_eq!(written["cameras"][0]["config_version"], 11);
        assert_eq!(
            written["cameras"][1]["id"],
            "aff8812b-c6be-4e59-aefd-40b59b425d92"
        );

        fs::remove_dir_all(dir).expect("remove test dir");
    }

    #[tokio::test]
    async fn remote_config_pull_updates_applied_versions_for_next_heartbeat() {
        let _guard = EnvGuard::new();
        let dir = unique_test_dir("pull-config-version");
        fs::create_dir_all(&dir).expect("test dir");
        let camera_path = dir.join("camera.json");
        let server_path = dir.join("server.json");
        AgentConfig::write_json_file(
            &server_path,
            &json!({
                "server": {
                    "bearer_token": "agent-token"
                }
            }),
        )
        .expect("write server config");
        AgentConfig::write_json_file(
            &camera_path,
            &json!({
                "config_version": 2,
                "camera_id": "ABLAK",
                "cameras": [{
                    "id": "ABLAK",
                    "config_version": 10,
                    "rtsp": { "url": "rtsp://192.168.1.187:554/stream2", "stream_id": 1 }
                }]
            }),
        )
        .expect("write starting camera config");
        let (base_url, requests) = spawn_config_server().await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("http client");

        let pulled = try_pull_remote_config(
            &client,
            &base_url,
            "agent-token",
            Some("ABLAK".to_string()),
            &server_path,
            &camera_path,
        )
        .await
        .expect("pull config");

        assert!(pulled);
        assert_eq!(
            requests.await.expect("request log"),
            vec![None, Some("ABLAK".to_string())]
        );

        let server_value = AgentConfig::load_server_json(&server_path).expect("read server config");
        let camera_value = AgentConfig::load_camera_json(&camera_path).expect("read camera config");
        let merged = AgentConfig::merge_server_camera_configs(&camera_value, &server_value)
            .expect("merge server+camera config");
        AgentConfig::apply_json_env_overrides(&merged);
        let cams = load_cameras_from_env().expect("load cameras from merged config");
        let targets = build_camera_heartbeat_targets(&cams, &merged);

        assert_eq!(config_version_value(&merged), Some(json!(3)));
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].camera_id, "ABLAK");
        assert_eq!(targets[0].config_version, Some(json!(11)));
        assert_eq!(targets[1].camera_id, "aff8812b-c6be-4e59-aefd-40b59b425d92");
        assert_eq!(targets[1].config_version, Some(json!(12)));

        fs::remove_dir_all(dir).expect("remove test dir");
    }
}
