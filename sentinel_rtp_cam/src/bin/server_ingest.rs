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
    let legacy_config_path = std::env::var("AGENT_CONFIG_JSON")
        .or_else(|_| std::env::var("CONFIG_JSON_PATH"))
        .ok();
    let camera_config_path = std::env::var("CAMERA_CONFIG_JSON")
        .ok()
        .or_else(|| legacy_config_path.clone())
        .unwrap_or_else(|| "/etc/sentinel_rtp_cam/camera.json".to_string());
    let camera_config_path = std::path::PathBuf::from(camera_config_path);
    let mut config_error: Option<String> = None;
    match AgentConfig::load_camera_json(&camera_config_path) {
        Ok(value) => AgentConfig::apply_json_env_overrides(&value),
        Err(e) => {
            config_error = Some(e.to_string());
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Some(error) = config_error {
        warn!(
            error = %error,
            path = %camera_config_path.display(),
            "Failed to load camera config JSON, using defaults"
        );
    }
    if std::env::var("CAMERA_CONFIG_JSON").is_err() && legacy_config_path.is_some() {
        warn!(
            path = %camera_config_path.display(),
            "CAMERA_CONFIG_JSON unset; using legacy config path for camera settings"
        );
    }

    let bind = std::env::var("SERVER_BIND").unwrap_or_else(|_| "0.0.0.0:9000".to_string());
    let token = std::env::var("SERVER_TOKEN").unwrap_or_else(|_| "devtoken".to_string());

    let clip_dir = PathBuf::from(std::env::var("CLIP_DIR").unwrap_or_else(|_| "clips".to_string()));
    let pre_secs: u64 = std::env::var("CLIP_PRE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let post_secs: u64 = std::env::var("CLIP_POST_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let ring_secs: u64 = std::env::var("CLIP_RING_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(pre_secs);
    let stale_part_secs: u64 = std::env::var("CLIP_STALE_PART_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24 * 60 * 60);
    let _write_batch_bytes: usize = std::env::var("CLIP_WRITE_BATCH_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256 * 1024);
    let _max_clip_secs: Option<u64> = std::env::var("CLIP_MAX_SECS")
        .ok()
        .and_then(|v| v.parse().ok());

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
    };

    tokio::spawn(async move {
        if let Err(e) = run_stream(cfg, rx).await {
            warn!(error = %e, stream_id = stream_id, "Stream pipeline ended");
        }
    });

    guard.insert(stream_id, tx.clone());
    tx
}
