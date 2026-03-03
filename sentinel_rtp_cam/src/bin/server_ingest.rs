use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn};

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
    let _write_batch_bytes: usize = runtime_var("CLIP_WRITE_BATCH_BYTES")
        .and_then(|v| v.parse().ok())
        .unwrap_or(256 * 1024);
    let max_clip_secs: Option<u64> = runtime_opt_u64("CLIP_MAX_SECS");

    let listener = TcpListener::bind(&bind).await?;
    info!(bind = %bind, "Server ingest listening");

    let streams: Arc<Mutex<HashMap<u32, mpsc::Sender<StreamMsg>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (sock, addr) = listener.accept().await?;
        let token = token.clone();
        let streams = streams.clone();
        let clip_dir = clip_dir.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(
                sock,
                addr,
                token,
                streams,
                clip_dir,
                ring_secs,
                pre_secs,
                post_secs,
                stale_part_secs,
                max_clip_secs,
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
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
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
            s.stream_id,
            s.name.clone(),
            clip_dir.clone(),
            ring_secs,
            pre_secs,
            post_secs,
            stale_part_secs,
            max_clip_secs,
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
            stream_id,
            format!("stream{}", stream_id),
            clip_dir.clone(),
            ring_secs,
            pre_secs,
            post_secs,
            stale_part_secs,
            max_clip_secs,
        );

        match msg_type {
            RTP => {
                let _ = tx.try_send(StreamMsg::Rtp(payload));
            }
            GAP => {
                if let Ok((last, new)) = decode_gap(&payload) {
                    let _ = tx.try_send(StreamMsg::Gap { last, new });
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
                let _ = tx.try_send(StreamMsg::Motion { rule, active, ts });
            }
            _ => {}
        }
    }
}

fn ensure_stream(
    streams: &Arc<Mutex<HashMap<u32, mpsc::Sender<StreamMsg>>>>,
    stream_id: u32,
    _stream_name: String,
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
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
    };

    tokio::spawn(async move {
        if let Err(e) = run_stream(cfg, rx).await {
            warn!(error = %e, stream_id = stream_id, "Stream pipeline ended");
        }
    });

    guard.insert(stream_id, tx.clone());
    tx
}
