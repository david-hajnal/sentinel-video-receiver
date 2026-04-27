use anyhow::{anyhow, Result};
use base64::Engine;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::{info, warn};

use sentinel_rtp_cam::core::video::annexb_from_raw_nal;
use sentinel_rtp_cam::live::v2::{run_stream_v2, H264SyncSeed, LivePipelineV2Config};
use sentinel_rtp_cam::proto::{decode_gap, read_msg, Msg, GAP, HELLO, MOTION, RTP};
use sentinel_rtp_cam::server_pipeline::{run_stream, StreamConfig, StreamMsg};
use sentinel_rtp_cam::AgentConfig;

const DEFAULT_CAMERA_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/camera.json";
const DEFAULT_SERVER_BIND: &str = "127.0.0.1:9000";
const INSECURE_DEFAULT_SERVER_TOKEN: &str = "devtoken";

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

fn resolved_server_bind(bind: Option<String>) -> String {
    bind.unwrap_or_else(|| DEFAULT_SERVER_BIND.to_string())
}

fn runtime_server_bind() -> String {
    resolved_server_bind(runtime_var("SERVER_BIND"))
}

fn validate_server_token(token: Option<String>) -> Result<String> {
    match token {
        None => Err(anyhow!(
            "SERVER_TOKEN is required and must be non-empty; refusing to start"
        )),
        Some(token) if token == INSECURE_DEFAULT_SERVER_TOKEN => Err(anyhow!(
            "SERVER_TOKEN must not be set to '{}' in ingest mode",
            INSECURE_DEFAULT_SERVER_TOKEN
        )),
        Some(token) => Ok(token),
    }
}

fn validated_server_token() -> Result<String> {
    validate_server_token(runtime_var("SERVER_TOKEN"))
}

fn bind_scope(bind: &str) -> &'static str {
    match bind.parse::<SocketAddr>() {
        Ok(addr) if addr.ip().is_loopback() => "loopback-only",
        _ => "network-exposed",
    }
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
    #[serde(default)]
    h264_sps_b64: Option<String>,
    #[serde(default)]
    h264_pps_b64: Option<String>,
}

#[derive(Clone)]
struct IngestH264SyncSeed {
    sps_annexb: Vec<u8>,
    pps_annexb: Vec<u8>,
}

fn decode_h264_sync_seed(stream: &HelloStream) -> Result<Option<IngestH264SyncSeed>> {
    let decode_opt = |label: &str, value: &str| -> Result<Vec<u8>> {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(value.as_bytes())
            .map_err(|error| anyhow!("invalid {label} base64: {error}"))?;
        if raw.is_empty() {
            return Err(anyhow!("{label} is empty"));
        }
        Ok(raw)
    };

    match (
        stream.h264_sps_b64.as_deref(),
        stream.h264_pps_b64.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow!(
            "h264_pps_b64 missing while h264_sps_b64 is present"
        )),
        (None, Some(_)) => Err(anyhow!(
            "h264_sps_b64 missing while h264_pps_b64 is present"
        )),
        (Some(sps_b64), Some(pps_b64)) => {
            let sps_raw = decode_opt("h264_sps_b64", sps_b64)?;
            let pps_raw = decode_opt("h264_pps_b64", pps_b64)?;
            Ok(Some(IngestH264SyncSeed {
                sps_annexb: annexb_from_raw_nal(&sps_raw),
                pps_annexb: annexb_from_raw_nal(&pps_raw),
            }))
        }
    }
}

#[derive(Debug, Deserialize)]
struct MotionPayload {
    active: bool,
    rule: Option<String>,
    ts: Option<String>,
}

fn payload_preview(payload: &[u8], max_chars: usize) -> String {
    let preview = String::from_utf8_lossy(payload);
    preview.chars().take(max_chars).collect()
}

fn parse_motion_msg(stream_id: u32, payload: &[u8]) -> Option<StreamMsg> {
    let value: Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(error) => {
            warn!(
                stream_id,
                error = %error,
                payload_len = payload.len(),
                payload_preview = %payload_preview(payload, 160),
                "Dropping malformed MOTION payload: invalid JSON"
            );
            return None;
        }
    };

    if !value.get("active").is_some_and(|field| field.is_boolean()) {
        warn!(
            stream_id,
            payload_len = payload.len(),
            payload_preview = %payload_preview(payload, 160),
            "Dropping malformed MOTION payload: missing or non-boolean active"
        );
        return None;
    }

    let parsed: MotionPayload = match serde_json::from_value(value) {
        Ok(parsed) => parsed,
        Err(error) => {
            warn!(
                stream_id,
                error = %error,
                payload_len = payload.len(),
                payload_preview = %payload_preview(payload, 160),
                "Dropping malformed MOTION payload: schema decode failed"
            );
            return None;
        }
    };

    let rule = parsed.rule.unwrap_or_else(|| "motion".to_string());
    let ts = parsed.ts.unwrap_or_default();
    Some(StreamMsg::Motion {
        rule,
        active: parsed.active,
        ts,
    })
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

#[derive(Clone)]
struct StreamHandle {
    tx: mpsc::Sender<StreamMsg>,
    generation: u64,
}

#[cfg(test)]
use std::future::Future;
#[cfg(test)]
use std::pin::Pin;
#[cfg(test)]
type PipelineFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

#[cfg(test)]
type PipelineHook =
    Arc<dyn Fn(u32, u64, mpsc::Receiver<StreamMsg>) -> PipelineFuture + Send + Sync + 'static>;

#[cfg(test)]
static TEST_PIPELINE_HOOK: std::sync::OnceLock<Mutex<Option<PipelineHook>>> =
    std::sync::OnceLock::new();

async fn run_stream_pipeline(
    live_pipeline_version: LivePipelineVersion,
    _stream_id: u32,
    _generation: u64,
    cfg: StreamConfig,
    v2_cfg: LivePipelineV2Config,
    rx: mpsc::Receiver<StreamMsg>,
) -> Result<()> {
    #[cfg(test)]
    {
        let hook = TEST_PIPELINE_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap()
            .clone();
        if let Some(hook) = hook {
            return hook(_stream_id, _generation, rx).await;
        }
    }
    match live_pipeline_version {
        LivePipelineVersion::V1 => run_stream(cfg, rx).await,
        LivePipelineVersion::V2 => run_stream_v2(v2_cfg, rx).await,
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

    let bind = runtime_server_bind();
    let bind_scope = bind_scope(&bind);
    let token = validated_server_token()?;

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
    let clip_write_timeout_ms: u64 = runtime_u64("CLIP_WRITE_TIMEOUT_MS", 5_000);
    let clip_write_retry_count: u64 = runtime_u64("CLIP_WRITE_RETRY_COUNT", 2);
    let clip_write_retry_backoff_ms: u64 = runtime_u64("CLIP_WRITE_RETRY_BACKOFF_MS", 250);
    let max_clip_secs: Option<u64> = runtime_opt_u64("CLIP_MAX_SECS");

    let listener = TcpListener::bind(&bind).await?;
    info!(
        bind = %bind,
        bind_scope,
        pipeline_version = ?live_pipeline_version,
        live_hls_segment_secs,
        live_hls_window_secs,
        "Server ingest listening"
    );
    if bind_scope != "loopback-only" {
        warn!(
            bind = %bind,
            "SERVER_BIND is non-loopback; use firewalling or TLS fronting for safe exposure"
        );
    }
    match live_pipeline_version {
        LivePipelineVersion::V1 => {
            info!(
                "LIVE_PIPELINE_VERSION=v1: clip/ingest-only path; does not publish HLS live artifacts"
            );
            info!(
                "Viewer-facing live output requires LIVE_PIPELINE_VERSION=v2; both v1 and v2 require ingest to receive H.264 RTP"
            );
        }
        LivePipelineVersion::V2 => {
            info!("LIVE_PIPELINE_VERSION=v2: live HLS pipeline enabled");
            info!(
                "Viewer-facing live output requires LIVE_PIPELINE_VERSION=v2; both v1 and v2 require ingest to receive H.264 RTP"
            );
        }
    }

    let streams: Arc<Mutex<HashMap<u32, StreamHandle>>> = Arc::new(Mutex::new(HashMap::new()));
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
                clip_write_timeout_ms,
                clip_write_retry_count,
                clip_write_retry_backoff_ms,
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
    streams: Arc<Mutex<HashMap<u32, StreamHandle>>>,
    active_streams: Arc<AtomicUsize>,
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
    clip_write_timeout_ms: u64,
    clip_write_retry_count: u64,
    clip_write_retry_backoff_ms: u64,
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
        let h264_sync_seed = match decode_h264_sync_seed(s) {
            Ok(seed) => seed,
            Err(error) => {
                warn!(
                    stream_id = s.stream_id,
                    stream_name = %s.name,
                    error = %error,
                    "Invalid H.264 sync seed metadata in HELLO stream entry; falling back to in-band sync"
                );
                None
            }
        };
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
            clip_write_timeout_ms,
            clip_write_retry_count,
            clip_write_retry_backoff_ms,
            stale_secs,
            stream_stats_log_secs,
            live_pipeline_version,
            live_hls_segment_secs,
            live_hls_window_secs,
            h264_sync_seed.clone(),
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
            clip_write_timeout_ms,
            clip_write_retry_count,
            clip_write_retry_backoff_ms,
            stale_secs,
            stream_stats_log_secs,
            live_pipeline_version,
            live_hls_segment_secs,
            live_hls_window_secs,
            None,
        );

        match msg_type {
            RTP => {
                let msg = StreamMsg::Rtp(payload);
                send_or_recreate_stream_msg(
                    &streams,
                    &active_streams,
                    &tx,
                    stream_id,
                    msg,
                    clip_dir.clone(),
                    ring_secs,
                    pre_secs,
                    post_secs,
                    stale_part_secs,
                    max_clip_secs,
                    clip_write_timeout_ms,
                    clip_write_retry_count,
                    clip_write_retry_backoff_ms,
                    stale_secs,
                    stream_stats_log_secs,
                    live_pipeline_version,
                    live_hls_segment_secs,
                    live_hls_window_secs,
                );
            }
            GAP => {
                if let Ok((last, new)) = decode_gap(&payload) {
                    let msg = StreamMsg::Gap { last, new };
                    send_or_recreate_stream_msg(
                        &streams,
                        &active_streams,
                        &tx,
                        stream_id,
                        msg,
                        clip_dir.clone(),
                        ring_secs,
                        pre_secs,
                        post_secs,
                        stale_part_secs,
                        max_clip_secs,
                        clip_write_timeout_ms,
                        clip_write_retry_count,
                        clip_write_retry_backoff_ms,
                        stale_secs,
                        stream_stats_log_secs,
                        live_pipeline_version,
                        live_hls_segment_secs,
                        live_hls_window_secs,
                    );
                }
            }
            MOTION => {
                if let Some(msg) = parse_motion_msg(stream_id, &payload) {
                    send_or_recreate_stream_msg(
                        &streams,
                        &active_streams,
                        &tx,
                        stream_id,
                        msg,
                        clip_dir.clone(),
                        ring_secs,
                        pre_secs,
                        post_secs,
                        stale_part_secs,
                        max_clip_secs,
                        clip_write_timeout_ms,
                        clip_write_retry_count,
                        clip_write_retry_backoff_ms,
                        stale_secs,
                        stream_stats_log_secs,
                        live_pipeline_version,
                        live_hls_segment_secs,
                        live_hls_window_secs,
                    );
                }
            }
            _ => {}
        }
    }
}

fn try_send_stream_msg(
    tx: &mpsc::Sender<StreamMsg>,
    stream_id: u32,
    msg: StreamMsg,
) -> Result<(), StreamMsg> {
    match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_msg)) => {
            warn!(
                stream_id,
                "Dropped stream message because stream queue is full"
            );
            Ok(())
        }
        Err(TrySendError::Closed(msg)) => {
            warn!(stream_id, "Stream channel closed; recreating pipeline");
            Err(msg)
        }
    }
}

fn evict_stream_if_same_sender(
    streams: &Arc<Mutex<HashMap<u32, StreamHandle>>>,
    stream_id: u32,
    tx: &mpsc::Sender<StreamMsg>,
) {
    let mut guard = streams.lock().unwrap();
    if guard
        .get(&stream_id)
        .is_some_and(|handle| handle.tx.same_channel(tx))
    {
        guard.remove(&stream_id);
    }
}

#[allow(clippy::too_many_arguments)]
fn send_or_recreate_stream_msg(
    streams: &Arc<Mutex<HashMap<u32, StreamHandle>>>,
    active_streams: &Arc<AtomicUsize>,
    tx: &mpsc::Sender<StreamMsg>,
    stream_id: u32,
    msg: StreamMsg,
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
    clip_write_timeout_ms: u64,
    clip_write_retry_count: u64,
    clip_write_retry_backoff_ms: u64,
    stale_secs: u64,
    stream_stats_log_secs: u64,
    live_pipeline_version: LivePipelineVersion,
    live_hls_segment_secs: u64,
    live_hls_window_secs: u64,
) {
    let msg = match try_send_stream_msg(tx, stream_id, msg) {
        Ok(()) => return,
        Err(msg) => msg,
    };

    evict_stream_if_same_sender(streams, stream_id, tx);
    let fresh_tx = ensure_stream(
        streams,
        active_streams,
        stream_id,
        format!("stream{}", stream_id),
        clip_dir,
        ring_secs,
        pre_secs,
        post_secs,
        stale_part_secs,
        max_clip_secs,
        clip_write_timeout_ms,
        clip_write_retry_count,
        clip_write_retry_backoff_ms,
        stale_secs,
        stream_stats_log_secs,
        live_pipeline_version,
        live_hls_segment_secs,
        live_hls_window_secs,
        None,
    );
    let _ = try_send_stream_msg(&fresh_tx, stream_id, msg);
}

fn ensure_stream(
    streams: &Arc<Mutex<HashMap<u32, StreamHandle>>>,
    active_streams: &Arc<AtomicUsize>,
    stream_id: u32,
    _stream_name: String,
    clip_dir: PathBuf,
    ring_secs: u64,
    pre_secs: u64,
    post_secs: u64,
    stale_part_secs: u64,
    max_clip_secs: Option<u64>,
    clip_write_timeout_ms: u64,
    clip_write_retry_count: u64,
    clip_write_retry_backoff_ms: u64,
    stale_secs: u64,
    stream_stats_log_secs: u64,
    live_pipeline_version: LivePipelineVersion,
    live_hls_segment_secs: u64,
    live_hls_window_secs: u64,
    h264_sync_seed: Option<IngestH264SyncSeed>,
) -> mpsc::Sender<StreamMsg> {
    let mut guard = streams.lock().unwrap();
    if let Some(handle) = guard.get(&stream_id) {
        if !handle.tx.is_closed() {
            return handle.tx.clone();
        }
    }

    let generation = guard
        .get(&stream_id)
        .map_or(1, |handle| handle.generation.saturating_add(1));
    let (tx, rx) = mpsc::channel::<StreamMsg>(4096);
    guard.insert(
        stream_id,
        StreamHandle {
            tx: tx.clone(),
            generation,
        },
    );
    drop(guard);

    let cfg = StreamConfig {
        stream_id,
        clip_dir,
        ring_secs,
        pre_secs,
        post_secs,
        stale_part_secs,
        max_clip_secs,
        clip_write_timeout_ms,
        clip_write_retry_count,
        clip_write_retry_backoff_ms,
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
        h264_sync_seed: h264_sync_seed.map(|seed| H264SyncSeed {
            sps_annexb: seed.sps_annexb,
            pps_annexb: seed.pps_annexb,
        }),
    };
    let active_streams = active_streams.clone();
    let streams_for_cleanup = streams.clone();

    tokio::spawn(async move {
        let now_active = active_streams.fetch_add(1, Ordering::SeqCst) + 1;
        info!(
            stream_id,
            active_streams = now_active,
            "Stream pipeline started"
        );
        let result = run_stream_pipeline(
            live_pipeline_version,
            stream_id,
            generation,
            cfg,
            v2_cfg,
            rx,
        )
        .await;
        if let Err(e) = result {
            warn!(error = %e, stream_id = stream_id, "Stream pipeline ended");
        }
        let mut guard = streams_for_cleanup.lock().unwrap();
        if guard
            .get(&stream_id)
            .is_some_and(|handle| handle.generation == generation)
        {
            guard.remove(&stream_id);
        }
        drop(guard);
        let remaining = active_streams
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        info!(
            stream_id,
            active_streams = remaining,
            "Stream pipeline stopped"
        );
    });

    tx
}

#[cfg(test)]
mod tests {
    use super::{
        decode_h264_sync_seed, ensure_stream, parse_motion_msg, resolved_server_bind,
        send_or_recreate_stream_msg, validate_server_token, HelloStream, LivePipelineVersion,
        StreamHandle, INSECURE_DEFAULT_SERVER_TOKEN, TEST_PIPELINE_HOOK,
    };
    use anyhow::{anyhow, Result};
    use sentinel_rtp_cam::server_pipeline::StreamMsg;
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    };
    use std::time::Duration;
    use tokio::sync::{mpsc, Notify};
    use tokio::task::yield_now;
    use tokio::time::timeout;

    static TEST_PIPELINE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct HookReset;

    impl Drop for HookReset {
        fn drop(&mut self) {
            clear_test_pipeline_hook();
        }
    }

    fn set_test_pipeline_hook(
        hook: impl Fn(u32, u64, mpsc::Receiver<StreamMsg>) -> super::PipelineFuture
            + Send
            + Sync
            + 'static,
    ) {
        let mut guard = TEST_PIPELINE_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap();
        *guard = Some(Arc::new(hook));
    }

    fn clear_test_pipeline_hook() {
        let mut guard = TEST_PIPELINE_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap();
        *guard = None;
    }

    async fn wait_for_launches(launches: &Arc<AtomicUsize>, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while launches.load(Ordering::SeqCst) < expected {
                yield_now().await;
            }
        })
        .await
        .expect("pipeline launch count should reach expected value");
    }

    fn default_ensure_stream(
        streams: &Arc<Mutex<HashMap<u32, StreamHandle>>>,
        active_streams: &Arc<AtomicUsize>,
        stream_id: u32,
    ) -> mpsc::Sender<StreamMsg> {
        ensure_stream(
            streams,
            active_streams,
            stream_id,
            format!("stream{stream_id}"),
            std::env::temp_dir(),
            3,
            3,
            5,
            24 * 60 * 60,
            None,
            5000,
            2,
            250,
            15,
            60,
            LivePipelineVersion::V1,
            2,
            12,
            None,
        )
    }

    #[test]
    fn parse_motion_msg_rejects_invalid_json() {
        let parsed = parse_motion_msg(7, b"{invalid");
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_motion_msg_rejects_missing_active() {
        let parsed = parse_motion_msg(7, br#"{"rule":"zone-a"}"#);
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_motion_msg_rejects_non_boolean_active() {
        let parsed = parse_motion_msg(7, br#"{"active":"true","rule":"zone-a"}"#);
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_motion_msg_keeps_defaults_for_optional_fields() {
        let parsed = parse_motion_msg(7, br#"{"active":true}"#).expect("motion message");
        match parsed {
            StreamMsg::Motion { rule, active, ts } => {
                assert_eq!(rule, "motion");
                assert!(active);
                assert!(ts.is_empty());
            }
            other => panic!("expected motion msg, got {other:?}"),
        }
    }

    #[test]
    fn default_bind_is_loopback() {
        assert_eq!(resolved_server_bind(None), "127.0.0.1:9000");
    }

    #[test]
    fn missing_token_is_rejected() {
        assert!(validate_server_token(None).is_err());
    }

    #[test]
    fn devtoken_is_rejected() {
        assert!(validate_server_token(Some(INSECURE_DEFAULT_SERVER_TOKEN.to_string())).is_err());
    }

    #[test]
    fn explicit_non_default_token_is_accepted() {
        let token = validate_server_token(Some("prod-token-abc123".to_string()))
            .expect("non-default token should be accepted");
        assert_eq!(token, "prod-token-abc123");
    }

    #[test]
    fn decode_h264_sync_seed_accepts_valid_sps_pps_base64() {
        let stream = HelloStream {
            stream_id: 7,
            name: "stream7".to_string(),
            h264_sps_b64: Some("Z2QAHw==".to_string()),
            h264_pps_b64: Some("aO48gA==".to_string()),
        };

        let seed = decode_h264_sync_seed(&stream)
            .expect("decode should succeed")
            .expect("seed should be present");
        assert_eq!(
            seed.sps_annexb,
            vec![0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1F]
        );
        assert_eq!(
            seed.pps_annexb,
            vec![0x00, 0x00, 0x00, 0x01, 0x68, 0xEE, 0x3C, 0x80]
        );
    }

    #[test]
    fn decode_h264_sync_seed_rejects_invalid_base64() {
        let stream = HelloStream {
            stream_id: 7,
            name: "stream7".to_string(),
            h264_sps_b64: Some("###".to_string()),
            h264_pps_b64: Some("aO48gA==".to_string()),
        };
        assert!(decode_h264_sync_seed(&stream).is_err());
    }

    #[test]
    fn decode_h264_sync_seed_rejects_partial_metadata() {
        let stream = HelloStream {
            stream_id: 7,
            name: "stream7".to_string(),
            h264_sps_b64: Some("Z2QAHw==".to_string()),
            h264_pps_b64: None,
        };
        assert!(decode_h264_sync_seed(&stream).is_err());
    }

    #[test]
    fn decode_h264_sync_seed_absent_metadata_is_none() {
        let stream = HelloStream {
            stream_id: 7,
            name: "stream7".to_string(),
            h264_sps_b64: None,
            h264_pps_b64: None,
        };
        assert!(decode_h264_sync_seed(&stream).unwrap().is_none());
    }

    #[tokio::test]
    async fn ensure_stream_reuses_existing_sender_while_pipeline_is_alive() {
        let _test_lock = TEST_PIPELINE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let _hook_reset = HookReset;
        let streams: Arc<Mutex<HashMap<u32, StreamHandle>>> = Arc::new(Mutex::new(HashMap::new()));
        let active_streams = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));
        let hold_open = Arc::new(Notify::new());

        {
            let launches = launches.clone();
            let hold_open = hold_open.clone();
            set_test_pipeline_hook(move |_stream_id, _generation, mut rx| {
                let launches = launches.clone();
                let hold_open = hold_open.clone();
                Box::pin(async move {
                    launches.fetch_add(1, Ordering::SeqCst);
                    let _ = rx.recv().await;
                    hold_open.notified().await;
                    Ok(())
                })
            });
        }

        let tx1 = default_ensure_stream(&streams, &active_streams, 7);
        let tx2 = default_ensure_stream(&streams, &active_streams, 7);
        wait_for_launches(&launches, 1).await;
        assert!(tx1.same_channel(&tx2));

        tx1.send(StreamMsg::Rtp(vec![1, 2, 3])).await.expect("send");
        hold_open.notify_waiters();
    }

    #[tokio::test]
    async fn ensure_stream_recreates_pipeline_after_task_exit() {
        let _test_lock = TEST_PIPELINE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let _hook_reset = HookReset;
        let streams: Arc<Mutex<HashMap<u32, StreamHandle>>> = Arc::new(Mutex::new(HashMap::new()));
        let active_streams = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));

        {
            let launches = launches.clone();
            set_test_pipeline_hook(move |_stream_id, _generation, mut rx| {
                let launches = launches.clone();
                Box::pin(async move {
                    launches.fetch_add(1, Ordering::SeqCst);
                    let _ = rx.recv().await;
                    Err(anyhow!("intentional pipeline failure"))
                })
            });
        }

        let tx1 = default_ensure_stream(&streams, &active_streams, 7);
        wait_for_launches(&launches, 1).await;
        tx1.send(StreamMsg::Rtp(vec![1])).await.expect("send");

        timeout(Duration::from_secs(1), async {
            while streams.lock().unwrap().contains_key(&7) {
                yield_now().await;
            }
        })
        .await
        .expect("first pipeline should exit and clean up stream handle");

        let tx2 = default_ensure_stream(&streams, &active_streams, 7);
        assert!(!tx1.same_channel(&tx2));
        wait_for_launches(&launches, 2).await;
    }

    #[tokio::test]
    async fn closed_sender_from_handle_conn_triggers_stream_recreation() -> Result<()> {
        let _test_lock = TEST_PIPELINE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let _hook_reset = HookReset;
        let streams: Arc<Mutex<HashMap<u32, StreamHandle>>> = Arc::new(Mutex::new(HashMap::new()));
        let active_streams = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));

        {
            let launches = launches.clone();
            set_test_pipeline_hook(move |_stream_id, _generation, mut rx| {
                let launches = launches.clone();
                Box::pin(async move {
                    launches.fetch_add(1, Ordering::SeqCst);
                    let _ = rx.recv().await;
                    Ok(())
                })
            });
        }

        let tx = default_ensure_stream(&streams, &active_streams, 9);
        tx.send(StreamMsg::Rtp(vec![1, 2, 3])).await?;

        timeout(Duration::from_secs(1), async {
            while streams.lock().unwrap().contains_key(&9) {
                yield_now().await;
            }
        })
        .await
        .expect("initial pipeline should stop and remove stream handle");

        send_or_recreate_stream_msg(
            &streams,
            &active_streams,
            &tx,
            9,
            StreamMsg::Rtp(vec![4, 5, 6]),
            std::env::temp_dir(),
            3,
            3,
            5,
            24 * 60 * 60,
            None,
            5000,
            2,
            250,
            15,
            60,
            LivePipelineVersion::V1,
            2,
            12,
        );

        timeout(Duration::from_secs(1), async {
            while launches.load(Ordering::SeqCst) < 2 {
                yield_now().await;
            }
        })
        .await
        .expect("stream should be recreated after closed sender");
        Ok(())
    }
}
