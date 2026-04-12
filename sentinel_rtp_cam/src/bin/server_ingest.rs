use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn};

use sentinel_rtp_cam::live::v2::{run_stream_v2, LivePipelineV2Config};
use sentinel_rtp_cam::proto::{decode_gap, read_msg, Msg, GAP, HELLO, MOTION, RTP};
use sentinel_rtp_cam::server_pipeline::{run_stream, StreamConfig, StreamMsg};
use sentinel_rtp_cam::AgentConfig;

const DEFAULT_CAMERA_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/camera.json";

fn runtime_var(name: &str) -> Option<String> {
    AgentConfig::runtime_var(name).filter(|v| !v.trim().is_empty())
}

fn runtime_u64(name: &str, default: u64) -> u64 {
    runtime_var(name)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn runtime_opt_u64(name: &str) -> Option<u64> {
    runtime_var(name).and_then(|v| v.parse::<u64>().ok())
}

#[derive(Debug, Deserialize)]
struct HelloPayload {
    agent_id: String,
    token: String,
    streams: Vec<HelloStream>,
}

#[derive(Debug, Deserialize)]
struct HelloStream {
    stream_id: u32,
    name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LivePipelineVersion {
    V1,
    V2,
}

fn runtime_live_pipeline_version() -> LivePipelineVersion {
    match runtime_var("LIVE_PIPELINE_VERSION")
        .unwrap_or_else(|| "v1".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "v2" => LivePipelineVersion::V2,
        _ => LivePipelineVersion::V1,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let camera_config_path = std::path::PathBuf::from(DEFAULT_CAMERA_CONFIG_PATH);
    let mut config_error: Option<String> = None;
    match AgentConfig::load_camera_json(&camera_config_path) {
        Ok(value) => AgentConfig::apply_json_env_overrides(&value),
        Err(e) => {
            config_error = Some(e.to_string());
        }
    }

    let log_filter = runtime_var("RUST_LOG").unwrap_or_else(|| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
        .init();

    if let Some(error) = config_error {
        warn!(
            error = %error,
            path = %camera_config_path.display(),
            "Failed to load camera config JSON, using defaults"
        );
    }

    let bind = runtime_var("SERVER_BIND").unwrap_or_else(|| "0.0.0.0:9000".to_string());
    let token = runtime_var("SERVER_TOKEN").unwrap_or_else(|| "devtoken".to_string());

    let clip_dir = PathBuf::from(runtime_var("CLIP_DIR").unwrap_or_else(|| "clips".to_string()));
    let pre_secs: u64 = runtime_u64("CLIP_PRE_SECS", 3);
    let post_secs: u64 = runtime_u64("CLIP_POST_SECS", 5);
    let ring_secs: u64 = runtime_u64("CLIP_RING_SECS", pre_secs);
    let stale_part_secs: u64 = runtime_u64("CLIP_STALE_PART_SECS", 24 * 60 * 60);
    let stale_secs: u64 = runtime_u64("STREAM_STALE_SECS", 15);
    let stream_stats_log_secs: u64 = runtime_u64("STREAM_STATS_LOG_SECS", 60);
    let live_pipeline_version = runtime_live_pipeline_version();
    let live_hls_segment_secs: u64 = runtime_u64("LIVE_HLS_SEGMENT_SECS", 2);
    let live_hls_window_secs: u64 = runtime_u64("LIVE_HLS_WINDOW_SECS", 12);
    let _write_batch_bytes: usize = runtime_var("CLIP_WRITE_BATCH_BYTES")
        .and_then(|v| v.parse().ok())
        .unwrap_or(256 * 1024);
    let max_clip_secs: Option<u64> = runtime_opt_u64("CLIP_MAX_SECS");

    let listener = TcpListener::bind(&bind).await?;
    info!(
        bind = %bind,
        pipeline_version = ?live_pipeline_version,
        live_hls_segment_secs,
        live_hls_window_secs,
        "Server ingest listening"
    );

    let streams: Arc<Mutex<HashMap<u32, mpsc::Sender<StreamMsg>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let active_streams = Arc::new(AtomicUsize::new(0));

    loop {
        let (sock, addr) = listener.accept().await?;
        let token = token.clone();
        let streams = streams.clone();
        let clip_dir = clip_dir.clone();
        let active_streams = active_streams.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(
                sock,
                addr,
                token,
                streams,
                active_streams,
                clip_dir,
                ring_secs,
                pre_secs,
                post_secs,
                stale_part_secs,
                max_clip_secs,
                stale_secs,
                stream_stats_log_secs,
                live_pipeline_version,
                live_hls_segment_secs,
                live_hls_window_secs,
            )
            .await
            {
                warn!(error = %e, "Connection ended");
            }
        });
    }
}

async fn handle_conn(
    mut sock: TcpStream,
    addr: SocketAddr,
    token: String,
    streams: Arc<Mutex<HashMap<u32, mpsc::Sender<StreamMsg>>>>,
    active_streams: Arc<AtomicUsize>,
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
    stale_secs: u64,
    stream_stats_log_secs: u64,
    live_pipeline_version: LivePipelineVersion,
    live_hls_segment_secs: u64,
    live_hls_window_secs: u64,
) -> Result<()> {
    let hello = read_msg(&mut sock).await?;
    if hello.msg_type != HELLO {
        return Err(anyhow!("first msg not HELLO"));
    }
    let hello: HelloPayload = serde_json::from_slice(&hello.payload)?;
    if hello.token != token {
        return Err(anyhow!("invalid token"));
    }
    info!(agent = %hello.agent_id, addr = %addr, "Agent connected");

    for s in &hello.streams {
        ensure_stream(
            &streams,
            &active_streams,
            s.stream_id,
            s.name.clone(),
            clip_dir.clone(),
            ring_secs,
            pre_secs,
            post_secs,
            stale_part_secs,
            max_clip_secs,
            stale_secs,
            stream_stats_log_secs,
            live_pipeline_version,
            live_hls_segment_secs,
            live_hls_window_secs,
        );
    }

    loop {
        let Msg {
            msg_type,
            stream_id,
            payload,
        } = read_msg(&mut sock).await?;
        let tx = ensure_stream(
            &streams,
            &active_streams,
            stream_id,
            format!("stream{}", stream_id),
            clip_dir.clone(),
            ring_secs,
            pre_secs,
            post_secs,
            stale_part_secs,
            max_clip_secs,
            stale_secs,
            stream_stats_log_secs,
            live_pipeline_version,
            live_hls_segment_secs,
            live_hls_window_secs,
        );

        match msg_type {
            RTP => {
                if tx.try_send(StreamMsg::Rtp(payload)).is_err() {
                    warn!(stream_id, "Dropped RTP packet because stream queue is full");
                }
            }
            GAP => {
                if let Ok((last, new)) = decode_gap(&payload) {
                    if tx.try_send(StreamMsg::Gap { last, new }).is_err() {
                        warn!(stream_id, "Dropped GAP message because stream queue is full");
                    }
                }
            }
            MOTION => {
                let v: serde_json::Value = serde_json::from_slice(&payload).unwrap_or_default();
                let rule = v
                    .get("rule")
                    .and_then(|v| v.as_str())
                    .unwrap_or("motion")
                    .to_string();
                let active = v.get("active").and_then(|v| v.as_bool()).unwrap_or(false);
                let ts = v
                    .get("ts")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if tx.try_send(StreamMsg::Motion { rule, active, ts }).is_err() {
                    warn!(stream_id, "Dropped MOTION message because stream queue is full");
                }
            }
            _ => {}
        }
    }
}

fn ensure_stream(
    streams: &Arc<Mutex<HashMap<u32, mpsc::Sender<StreamMsg>>>>,
    active_streams: &Arc<AtomicUsize>,
    stream_id: u32,
    _stream_name: String,
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
    stale_secs: u64,
    stream_stats_log_secs: u64,
    live_pipeline_version: LivePipelineVersion,
    live_hls_segment_secs: u64,
    live_hls_window_secs: u64,
) -> mpsc::Sender<StreamMsg> {
    let mut guard = streams.lock().unwrap();
    if let Some(tx) = guard.get(&stream_id) {
        return tx.clone();
    }

    let (tx, rx) = mpsc::channel::<StreamMsg>(4096);
    let cfg = StreamConfig {
        stream_id,
        clip_dir,
        ring_secs,
        pre_secs,
        post_secs,
        stale_part_secs,
        max_clip_secs,
        stale_secs,
        stats_log_secs: stream_stats_log_secs,
    };
    let v2_cfg = LivePipelineV2Config {
        stream_id,
        output_root: cfg.clip_dir.join("hls_v2").join(stream_id.to_string()),
        health_dir: cfg.clip_dir.clone(),
        stale_secs,
        stats_log_secs: stream_stats_log_secs,
        segment_secs: live_hls_segment_secs,
        window_secs: live_hls_window_secs,
    };
    let active_streams = active_streams.clone();

    tokio::spawn(async move {
        let now_active = active_streams.fetch_add(1, Ordering::SeqCst) + 1;
        info!(stream_id, active_streams = now_active, "Stream pipeline started");
        let result = match live_pipeline_version {
            LivePipelineVersion::V1 => run_stream(cfg, rx).await,
            LivePipelineVersion::V2 => run_stream_v2(v2_cfg, rx).await,
        };
        if let Err(e) = result {
            warn!(error = %e, stream_id = stream_id, "Stream pipeline ended");
        }
        let remaining = active_streams.fetch_sub(1, Ordering::SeqCst).saturating_sub(1);
        info!(stream_id, active_streams = remaining, "Stream pipeline stopped");
    });

    guard.insert(stream_id, tx.clone());
    tx
}
