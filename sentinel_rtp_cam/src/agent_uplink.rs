use anyhow::{anyhow, Result};
use base64::Engine;
use rand::RngCore;
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::AgentConfig;
use crate::proto::{Msg, GAP, RTP};

#[derive(Clone)]
pub struct Uplink {
    media_tx: mpsc::Sender<Msg>,
    motion_tx: mpsc::UnboundedSender<MotionEnvelope>,
    stats: Arc<UplinkStats>,
    shutdown: CancellationToken,
    stream_h264_parameter_sets: Arc<RwLock<HashMap<u32, H264ParameterSets>>>,
}

#[derive(Debug, Clone)]
pub struct H264ParameterSets {
    pub sps: Vec<u8>,
    pub pps: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct HelloPayload {
    agent_id: String,
    token: String,
    streams: Vec<String>,
    timestamp_unix: i64,
    nonce: String,
    streams_info: Vec<HelloStream>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HelloIngestConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_pre_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_post_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_ring_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_stale_part_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_max_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_write_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_write_retry_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clip_write_retry_backoff_ms: Option<u64>,
}

impl HelloIngestConfig {
    pub fn is_empty(&self) -> bool {
        self.clip_pre_secs.is_none()
            && self.clip_post_secs.is_none()
            && self.clip_ring_secs.is_none()
            && self.clip_stale_part_secs.is_none()
            && self.clip_max_secs.is_none()
            && self.clip_write_timeout_ms.is_none()
            && self.clip_write_retry_count.is_none()
            && self.clip_write_retry_backoff_ms.is_none()
    }
}

#[derive(Debug, Serialize)]
struct HelloStream {
    stream_id: u32,
    name: String,
    camera_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingest: Option<HelloIngestConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    h264_sps_b64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    h264_pps_b64: Option<String>,
}
#[derive(Debug, Deserialize)]
struct MotionEnvelope {
    stream_id: u32,
    rule: String,
    active: bool,
    ts: String,
    camera_id: String,
    event_id: String,
}

#[derive(Debug, Serialize)]
struct EventPayload {
    stream_id: String,
    event_type: String,
    state: String,
    event_ts_unix_ms: i64,
    confidence: f64,
    rule: Option<String>,
    event_id: Option<String>,
    camera_id: Option<String>,
}

#[derive(Default)]
struct UplinkStats {
    rtp_sent: AtomicU64,
    gap_sent: AtomicU64,
    motion_sent: AtomicU64,
    dropped: AtomicU64,
}

impl Uplink {
    pub fn connect_and_run(
        server_addr: String,
        token: String,
        agent_id: String,
        stream_map: HashMap<u32, String>,
        stream_camera_map: HashMap<u32, String>,
        stream_ingest: Option<HelloIngestConfig>,
        stream_h264_parameter_sets: HashMap<u32, H264ParameterSets>,
    ) -> Self {
        let (media_tx, mut media_rx) = mpsc::channel::<Msg>(4096);
        let (motion_tx, mut motion_rx) = mpsc::unbounded_channel::<MotionEnvelope>();
        let stats = Arc::new(UplinkStats::default());
        let shutdown = CancellationToken::new();
        let stream_h264_parameter_sets = Arc::new(RwLock::new(stream_h264_parameter_sets));

        let stats_task = stats.clone();
        let shutdown_task = shutdown.clone();
        let h264_parameter_sets = stream_h264_parameter_sets.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                if shutdown_task.is_cancelled() {
                    info!("Uplink shutdown requested");
                    break;
                }
                let (connector, server_name) = match build_tls_connector(&server_addr) {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, server = %server_addr, "Uplink TLS config failed");
                        tokio::select! {
                            _ = shutdown_task.cancelled() => break,
                            _ = sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                };

                match TcpStream::connect(&server_addr).await {
                    Ok(stream) => {
                        let stream = match connector.connect(server_name, stream).await {
                            Ok(tls) => tls,
                            Err(e) => {
                                error!(error = %e, server = %server_addr, "Uplink TLS handshake failed");
                                if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                    break;
                                }
                                backoff = (backoff * 2).min(Duration::from_secs(30));
                                continue;
                            }
                        };
                        info!(
                            server = %server_addr,
                            agent_id = %agent_id,
                            stream_count = stream_map.len(),
                            "Uplink connected"
                        );
                        backoff = Duration::from_secs(1);

                        let nonce = generate_nonce();
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        let streams_info = build_streams_info(
                            &stream_map,
                            &stream_camera_map,
                            stream_ingest.as_ref(),
                            &h264_parameter_sets
                                .read()
                                .unwrap_or_else(|poison| poison.into_inner()),
                        );
                        let streams: Vec<String> =
                            streams_info.iter().map(|s| s.name.clone()).collect();
                        let hello = HelloPayload {
                            agent_id: agent_id.clone(),
                            token: token.clone(),
                            streams,
                            timestamp_unix: now,
                            nonce,
                            streams_info,
                        };
                        let payload = match serde_json::to_vec(&hello) {
                            Ok(p) => p,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize HELLO");
                                if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                    break;
                                }
                                continue;
                            }
                        };
                        let (mut read_half, mut write_half) = tokio::io::split(stream);
                        if let Err(e) =
                            write_record(&mut write_half, RECORD_HELLO, 0, &payload).await
                        {
                            error!(error = %e, "Failed to send HELLO");
                            if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                break;
                            }
                            continue;
                        }
                        info!(stream_count = stream_map.len(), "Uplink HELLO sent");

                        let hello_timeout_secs =
                            AgentConfig::runtime_var("INGEST_TLS_HELLO_TIMEOUT_SECS")
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(10);
                        let hello_ok = match tokio::time::timeout(
                            Duration::from_secs(hello_timeout_secs),
                            read_record(&mut read_half),
                        )
                        .await
                        {
                            Ok(Ok(rec)) if rec.record_type == RECORD_HELLO_OK => {
                                match serde_json::from_slice::<HelloOkPayload>(&rec.payload) {
                                    Ok(ok) => ok,
                                    Err(e) => {
                                        error!(error = %e, "Failed to parse HELLO_OK");
                                        if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            }
                            Ok(Ok(rec)) if rec.record_type == RECORD_ERROR => {
                                if let Ok(err) =
                                    serde_json::from_slice::<ErrorPayload>(&rec.payload)
                                {
                                    error!(code = %err.code, message = %err.message, "HELLO rejected");
                                } else {
                                    error!("HELLO rejected");
                                }
                                if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                    break;
                                }
                                continue;
                            }
                            Ok(Ok(rec)) => {
                                error!(
                                    record_type = rec.record_type,
                                    "Unexpected record after HELLO"
                                );
                                if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                    break;
                                }
                                continue;
                            }
                            Ok(Err(e)) => {
                                error!(error = %e, "Failed to read HELLO_OK");
                                if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                    break;
                                }
                                continue;
                            }
                            Err(_) => {
                                error!("Timed out waiting for HELLO_OK");
                                if !sleep_or_shutdown(&shutdown_task, backoff).await {
                                    break;
                                }
                                continue;
                            }
                        };

                        let (rec_tx, mut rec_rx) = mpsc::channel::<Result<Record>>(32);
                        let read_task = tokio::spawn(async move {
                            loop {
                                match read_record(&mut read_half).await {
                                    Ok(rec) => {
                                        if rec_tx.send(Ok(rec)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = rec_tx.send(Err(e)).await;
                                        break;
                                    }
                                }
                            }
                        });

                        let mut ping = interval(Duration::from_secs(hello_ok.ping_interval_sec));
                        let mut sent: u64 = 0;
                        let mut last_log = Instant::now();
                        let mut last_rtp = 0u64;
                        let mut last_gap = 0u64;
                        let mut last_motion = 0u64;
                        let mut last_dropped = 0u64;
                        loop {
                            tokio::select! {
                                biased;
                                _ = shutdown_task.cancelled() => {
                                    info!("Uplink shutdown requested");
                                    break;
                                }
                                rec = rec_rx.recv() => {
                                    match rec {
                                        Some(Ok(rec)) => {
                                            match rec.record_type {
                                                RECORD_PING => {
                                                    let _ = write_record(&mut write_half, RECORD_PING, 0, &[]).await;
                                                }
                                                RECORD_CLOSE => {
                                                    info!("Uplink server requested close");
                                                    break;
                                                }
                                                RECORD_ERROR => {
                                                    if let Ok(err) = serde_json::from_slice::<ErrorPayload>(&rec.payload) {
                                                        error!(code = %err.code, message = %err.message, "Uplink error");
                                                    } else {
                                                        error!("Uplink error");
                                                    }
                                                    break;
                                                }
                                                _ => {}
                                            }
                                        }
                                        Some(Err(e)) => {
                                            error!(error = %e, "Uplink read failed, reconnecting");
                                            break;
                                        }
                                        None => {
                                            error!("Uplink read channel closed");
                                            break;
                                        }
                                    }
                                }
                                Some(msg) = motion_rx.recv() => {
                                    let event = build_event_payload(&msg);
                                    let payload = match serde_json::to_vec(&event) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            error!(
                                                error = %e,
                                                stream_id = msg.stream_id,
                                                camera_id = %msg.camera_id,
                                                event_id = %msg.event_id,
                                                "Failed to serialize MOTION envelope"
                                            );
                                            continue;
                                        }
                                    };
                                    if let Err(e) = write_record(&mut write_half, RECORD_EVENT, 0, &payload).await {
                                        error!(error = %e, "Uplink write failed, reconnecting");
                                        break;
                                    }
                                    sent += 1;
                                    stats_task.motion_sent.fetch_add(1, Ordering::Relaxed);
                                }
                                Some(msg) = media_rx.recv() => {
                                    match msg.msg_type {
                                        RTP => {
                                            let payload = encode_rtp_tls(msg.stream_id, &msg.payload);
                                            if let Err(e) = write_record(&mut write_half, RECORD_RTP, 0, &payload).await {
                                                error!(error = %e, "Uplink write failed, reconnecting");
                                                break;
                                            }
                                            sent += 1;
                                            stats_task.rtp_sent.fetch_add(1, Ordering::Relaxed);
                                        }
                                        GAP => {
                                            if let Err(e) = write_record(&mut write_half, RECORD_GAP, 0, &msg.payload).await {
                                                error!(error = %e, "Uplink write failed, reconnecting");
                                                break;
                                            }
                                            sent += 1;
                                            stats_task.gap_sent.fetch_add(1, Ordering::Relaxed);
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping.tick() => {
                                    if let Err(e) = write_record(&mut write_half, RECORD_PING, 0, &[]).await {
                                        error!(error = %e, "Uplink ping failed, reconnecting");
                                        break;
                                    }
                                    debug!(sent = sent, "Uplink ping");

                                    if last_log.elapsed() >= Duration::from_secs(30) {
                                        let rtp = stats_task.rtp_sent.load(Ordering::Relaxed);
                                        let gap = stats_task.gap_sent.load(Ordering::Relaxed);
                                        let motion = stats_task.motion_sent.load(Ordering::Relaxed);
                                        let dropped = stats_task.dropped.load(Ordering::Relaxed);

                                        info!(
                                            rtp_total = rtp,
                                            rtp_delta = rtp.saturating_sub(last_rtp),
                                            gap_total = gap,
                                            gap_delta = gap.saturating_sub(last_gap),
                                            motion_total = motion,
                                            motion_delta = motion.saturating_sub(last_motion),
                                            dropped_total = dropped,
                                            dropped_delta = dropped.saturating_sub(last_dropped),
                                            "Uplink stats"
                                        );

                                        last_log = Instant::now();
                                        last_rtp = rtp;
                                        last_gap = gap;
                                        last_motion = motion;
                                        last_dropped = dropped;
                                    }
                                }
                                else => break,
                            }
                        }
                        read_task.abort();
                        if shutdown_task.is_cancelled() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, server = %server_addr, "Uplink connect failed");
                    }
                }

                tokio::select! {
                    _ = shutdown_task.cancelled() => break,
                    _ = sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        });

        Self {
            media_tx,
            motion_tx,
            stats,
            shutdown,
            stream_h264_parameter_sets,
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    pub fn send_rtp(&self, stream_id: u32, rtp_bytes: Vec<u8>) {
        let msg = Msg {
            msg_type: RTP,
            stream_id,
            payload: rtp_bytes,
        };
        if let Err(e) = self.media_tx.try_send(msg) {
            let dropped = self.stats.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped <= 3 || dropped % 100 == 0 {
                warn!(
                    stream_id,
                    dropped_total = dropped,
                    error = %e,
                    "Uplink channel full; dropping RTP"
                );
            }
        }
    }

    pub fn send_gap(&self, stream_id: u32, last_seq: u16, new_seq: u16) {
        let msg = Msg {
            msg_type: GAP,
            stream_id,
            payload: encode_gap_tls(stream_id, last_seq, new_seq),
        };
        if let Err(e) = self.media_tx.try_send(msg) {
            let dropped = self.stats.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped <= 3 || dropped % 100 == 0 {
                warn!(
                    stream_id,
                    dropped_total = dropped,
                    error = %e,
                    "Uplink channel full; dropping GAP"
                );
            }
        }
    }

    pub fn send_motion(
        &self,
        stream_id: u32,
        rule: String,
        active: bool,
        ts: String,
        camera_id: String,
        event_id: String,
    ) {
        let msg = MotionEnvelope {
            stream_id,
            rule,
            active,
            ts,
            camera_id,
            event_id,
        };
        if let Err(e) = self.motion_tx.send(msg) {
            warn!(
                stream_id,
                error = %e,
                "Uplink motion channel closed; dropping MOTION"
            );
        }
    }

    pub fn set_stream_h264_parameter_sets(&self, stream_id: u32, sps: Vec<u8>, pps: Vec<u8>) {
        let mut guard = self
            .stream_h264_parameter_sets
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        guard.insert(stream_id, H264ParameterSets { sps, pps });
    }
}

async fn sleep_or_shutdown(shutdown: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = sleep(duration) => true,
    }
}

fn build_tls_connector(server_addr: &str) -> Result<(TlsConnector, ServerName<'static>)> {
    let ca_path = AgentConfig::runtime_var("INGEST_TLS_CA")
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/etc/sentinel_rtp_cam/ca.crt"));
    let server_name = resolve_server_name(server_addr)?;
    let config = load_tls_config(&ca_path)?;
    Ok((TlsConnector::from(Arc::new(config)), server_name))
}

#[derive(Debug, Deserialize)]
struct HelloOkPayload {
    #[allow(dead_code)]
    session_id: String,
    #[allow(dead_code)]
    max_payload: u32,
    ping_interval_sec: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorPayload {
    code: String,
    message: String,
}

const MAX_PAYLOAD: usize = 2 * 1024 * 1024;
const RECORD_HELLO: u8 = 1;
const RECORD_RTP: u8 = 2;
const RECORD_EVENT: u8 = 3;
const RECORD_PING: u8 = 4;
const RECORD_CLOSE: u8 = 5;
const RECORD_HELLO_OK: u8 = 6;
const RECORD_ERROR: u8 = 7;
const RECORD_GAP: u8 = 8;

struct Record {
    record_type: u8,
    #[allow(dead_code)]
    flags: u8,
    payload: Vec<u8>,
}

async fn read_record<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Result<Record> {
    let mut header = [0u8; 6];
    tokio::io::AsyncReadExt::read_exact(reader, &mut header).await?;
    let record_type = header[0];
    let flags = header[1];
    let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    if len > MAX_PAYLOAD {
        return Err(anyhow!("record payload too large: {}", len));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        tokio::io::AsyncReadExt::read_exact(reader, &mut payload).await?;
    }
    Ok(Record {
        record_type,
        flags,
        payload,
    })
}

async fn write_record<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    record_type: u8,
    flags: u8,
    payload: &[u8],
) -> Result<()> {
    if payload.len() > MAX_PAYLOAD {
        return Err(anyhow!("record payload too large: {}", payload.len()));
    }
    let mut header = [0u8; 6];
    header[0] = record_type;
    header[1] = flags;
    header[2..6].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    tokio::io::AsyncWriteExt::write_all(writer, &header).await?;
    if !payload.is_empty() {
        tokio::io::AsyncWriteExt::write_all(writer, payload).await?;
    }
    tokio::io::AsyncWriteExt::flush(writer).await?;
    Ok(())
}

fn generate_nonce() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::STANDARD.encode(&buf)
}

fn encode_rtp_tls(stream_id: u32, rtp_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + rtp_bytes.len());
    buf.extend_from_slice(&stream_id.to_be_bytes());
    buf.extend_from_slice(rtp_bytes);
    buf
}

fn encode_gap_tls(stream_id: u32, last_seq: u16, new_seq: u16) -> Vec<u8> {
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&stream_id.to_be_bytes());
    buf[4..6].copy_from_slice(&last_seq.to_be_bytes());
    buf[6..8].copy_from_slice(&new_seq.to_be_bytes());
    buf.to_vec()
}

#[cfg(test)]
fn decode_rtp_tls(buf: &[u8]) -> Result<(u32, Vec<u8>)> {
    if buf.len() < 4 {
        return Err(anyhow!("rtp tls payload too short: {}", buf.len()));
    }

    let stream_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    Ok((stream_id, buf[4..].to_vec()))
}

fn build_streams_info(
    stream_map: &HashMap<u32, String>,
    stream_camera_map: &HashMap<u32, String>,
    stream_ingest: Option<&HelloIngestConfig>,
    stream_h264_parameter_sets: &HashMap<u32, H264ParameterSets>,
) -> Vec<HelloStream> {
    let mut streams: Vec<HelloStream> = stream_map
        .iter()
        .map(|(stream_id, name)| {
            let (h264_sps_b64, h264_pps_b64) = stream_h264_parameter_sets
                .get(stream_id)
                .map(|sets| {
                    (
                        Some(base64::engine::general_purpose::STANDARD.encode(&sets.sps)),
                        Some(base64::engine::general_purpose::STANDARD.encode(&sets.pps)),
                    )
                })
                .unwrap_or((None, None));
            HelloStream {
                stream_id: *stream_id,
                name: name.clone(),
                camera_id: stream_camera_map
                    .get(stream_id)
                    .cloned()
                    .unwrap_or_else(|| format!("stream-{stream_id}")),
                ingest: stream_ingest.cloned(),
                h264_sps_b64,
                h264_pps_b64,
            }
        })
        .collect();
    streams.sort_by_key(|s| s.stream_id);
    streams
}

#[cfg(test)]
mod tests {
    use super::{
        build_event_payload, build_streams_info, decode_rtp_tls, encode_gap_tls, encode_rtp_tls,
        load_tls_config, read_record, resolve_server_name, write_record, H264ParameterSets,
        HelloIngestConfig, HelloPayload, MotionEnvelope, RECORD_EVENT, RECORD_GAP, RECORD_RTP,
    };
    use crate::config::AgentConfig;
    use crate::proto::{Msg, GAP, RTP};
    use std::collections::HashMap;
    use std::sync::MutexGuard;
    use tokio::io::duplex;
    use ulid::Ulid;

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        saved: HashMap<String, String>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let lock = AgentConfig::runtime_test_lock()
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let saved = AgentConfig::runtime_snapshot();
            AgentConfig::clear_runtime_overrides();
            Self { _lock: lock, saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            AgentConfig::runtime_restore(std::mem::take(&mut self.saved));
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("sentinel-agent-uplink-{name}-{}", Ulid::new()))
    }

    #[test]
    fn build_streams_info_includes_ingest_override_when_present() {
        let mut stream_map = HashMap::new();
        stream_map.insert(2, "cam2".to_string());
        stream_map.insert(1, "cam1".to_string());
        let mut stream_camera_map = HashMap::new();
        stream_camera_map.insert(1, "camera-1".to_string());
        stream_camera_map.insert(2, "camera-2".to_string());
        let ingest = HelloIngestConfig {
            clip_pre_secs: Some(2),
            clip_post_secs: Some(120),
            clip_ring_secs: Some(5),
            clip_stale_part_secs: Some(3600),
            clip_max_secs: Some(30),
            clip_write_timeout_ms: Some(5000),
            clip_write_retry_count: Some(2),
            clip_write_retry_backoff_ms: Some(250),
        };

        let streams = build_streams_info(
            &stream_map,
            &stream_camera_map,
            Some(&ingest),
            &HashMap::new(),
        );
        assert_eq!(streams.len(), 2);
        assert_eq!(streams[0].stream_id, 1);
        assert_eq!(streams[1].stream_id, 2);
        assert_eq!(
            streams[0]
                .ingest
                .as_ref()
                .and_then(|cfg| cfg.clip_post_secs),
            Some(120)
        );
        assert_eq!(
            streams[1]
                .ingest
                .as_ref()
                .and_then(|cfg| cfg.clip_post_secs),
            Some(120)
        );
    }

    #[test]
    fn build_streams_info_uses_canonical_camera_ids_for_multi_camera_hello() {
        let mut stream_map = HashMap::new();
        stream_map.insert(2, "cam2".to_string());
        stream_map.insert(1, "cam1".to_string());
        let mut stream_camera_map = HashMap::new();
        stream_camera_map.insert(1, "ABLAK".to_string());
        stream_camera_map.insert(2, "aff8812b-c6be-4e59-aefd-40b59b425d92".to_string());

        let streams_info =
            build_streams_info(&stream_map, &stream_camera_map, None, &HashMap::new());
        let streams: Vec<String> = streams_info.iter().map(|s| s.name.clone()).collect();
        let hello = HelloPayload {
            agent_id: "01KH1V1AMEP4NDDDJ0SRV067TW".to_string(),
            token: "agent-token".to_string(),
            streams,
            timestamp_unix: 1_775_463_330,
            nonce: "nonce".to_string(),
            streams_info,
        };

        let payload = serde_json::to_value(&hello).expect("serialize hello");
        assert_eq!(
            payload["streams_info"][0]["camera_id"].as_str(),
            Some("ABLAK")
        );
        assert_eq!(
            payload["streams_info"][1]["camera_id"].as_str(),
            Some("aff8812b-c6be-4e59-aefd-40b59b425d92")
        );
        assert_eq!(payload["streams_info"][0]["stream_id"].as_u64(), Some(1));
        assert_eq!(payload["streams_info"][1]["stream_id"].as_u64(), Some(2));
        assert!(!payload.to_string().contains("CAM2"));
    }

    #[test]
    fn build_streams_info_omits_ingest_override_when_absent() {
        let mut stream_map = HashMap::new();
        stream_map.insert(1, "cam1".to_string());
        let mut stream_camera_map = HashMap::new();
        stream_camera_map.insert(1, "camera-1".to_string());

        let streams = build_streams_info(&stream_map, &stream_camera_map, None, &HashMap::new());
        assert_eq!(streams.len(), 1);
        assert!(streams[0].ingest.is_none());
    }

    #[test]
    fn hello_stream_round_trips_h264_parameter_sets() {
        let mut stream_map = HashMap::new();
        stream_map.insert(1, "cam1".to_string());
        let mut stream_camera_map = HashMap::new();
        stream_camera_map.insert(1, "camera-1".to_string());
        let mut stream_h264_parameter_sets = HashMap::new();
        stream_h264_parameter_sets.insert(
            1,
            H264ParameterSets {
                sps: vec![0x67, 0x64, 0x00, 0x1F],
                pps: vec![0x68, 0xEE, 0x3C, 0x80],
            },
        );

        let streams_info = build_streams_info(
            &stream_map,
            &stream_camera_map,
            None,
            &stream_h264_parameter_sets,
        );
        let streams: Vec<String> = streams_info.iter().map(|s| s.name.clone()).collect();
        let hello = HelloPayload {
            agent_id: "agent-a".to_string(),
            token: "agent-token".to_string(),
            streams,
            timestamp_unix: 1_775_463_330,
            nonce: "nonce".to_string(),
            streams_info,
        };

        let payload = serde_json::to_vec(&hello).expect("serialize hello");
        let value: serde_json::Value = serde_json::from_slice(&payload).expect("parse serialized");
        assert_eq!(
            value["streams_info"][0]["h264_sps_b64"].as_str(),
            Some("Z2QAHw==")
        );
        assert_eq!(
            value["streams_info"][0]["h264_pps_b64"].as_str(),
            Some("aO48gA==")
        );
    }

    #[test]
    fn hello_ingest_config_is_empty_detects_non_empty_values() {
        let empty = HelloIngestConfig {
            clip_pre_secs: None,
            clip_post_secs: None,
            clip_ring_secs: None,
            clip_stale_part_secs: None,
            clip_max_secs: None,
            clip_write_timeout_ms: None,
            clip_write_retry_count: None,
            clip_write_retry_backoff_ms: None,
        };
        assert!(empty.is_empty());

        let non_empty = HelloIngestConfig {
            clip_pre_secs: Some(2),
            clip_post_secs: None,
            clip_ring_secs: None,
            clip_stale_part_secs: None,
            clip_max_secs: None,
            clip_write_timeout_ms: None,
            clip_write_retry_count: None,
            clip_write_retry_backoff_ms: None,
        };
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn build_event_payload_preserves_legacy_motion_fields() {
        let msg = MotionEnvelope {
            stream_id: 7,
            rule: "zone-a".to_string(),
            active: true,
            ts: "2026-04-06T08:15:30Z".to_string(),
            camera_id: "cam-7".to_string(),
            event_id: "evt-123".to_string(),
        };

        let event = build_event_payload(&msg);

        assert_eq!(event.stream_id, "7");
        assert_eq!(event.event_type, "motion");
        assert_eq!(event.state, "start");
        assert_eq!(event.rule.as_deref(), Some("zone-a"));
        assert_eq!(event.camera_id.as_deref(), Some("cam-7"));
        assert_eq!(event.event_id.as_deref(), Some("evt-123"));
        assert_eq!(event.event_ts_unix_ms, 1_775_463_330_000);
    }

    #[test]
    fn build_event_payload_generates_fallback_values_for_invalid_payload() {
        let msg = MotionEnvelope {
            stream_id: 42,
            rule: "motion".to_string(),
            active: false,
            ts: "not-a-timestamp".to_string(),
            camera_id: "".to_string(),
            event_id: "".to_string(),
        };

        let event = build_event_payload(&msg);

        assert_eq!(event.stream_id, "42");
        assert_eq!(event.state, "stop");
        assert_eq!(event.rule.as_deref(), Some("motion"));
        assert_eq!(event.camera_id.as_deref(), Some(""));
        assert!(event.event_ts_unix_ms > 0);
        assert!(event
            .event_id
            .as_deref()
            .is_some_and(|id| id.starts_with("stream42-")));
    }

    #[test]
    fn encode_gap_tls_uses_big_endian_layout() {
        let encoded = encode_gap_tls(9, 0x1234, 0xABCD);
        assert_eq!(encoded, vec![0, 0, 0, 9, 0x12, 0x34, 0xAB, 0xCD]);
    }

    #[test]
    fn encode_rtp_tls_prefixes_big_endian_stream_id() {
        let encoded = encode_rtp_tls(9, &[0x80, 0x60, 0x12, 0x34]);
        assert_eq!(encoded, vec![0, 0, 0, 9, 0x80, 0x60, 0x12, 0x34]);
    }

    #[test]
    fn encode_rtp_tls_prefixes_stream_id_two_as_big_endian() {
        let encoded = encode_rtp_tls(2, &[0x80, 0x60, 0x12, 0x34]);
        assert_eq!(&encoded[..4], &[0x00, 0x00, 0x00, 0x02]);
        assert_eq!(&encoded[4..], &[0x80, 0x60, 0x12, 0x34]);
    }

    #[tokio::test]
    async fn read_and_write_record_roundtrip() {
        let (mut a, mut b) = duplex(1024);
        let payload = b"hello".to_vec();

        tokio::spawn(async move {
            write_record(&mut a, RECORD_EVENT, 3, &payload)
                .await
                .unwrap();
        });

        let record = read_record(&mut b).await.unwrap();
        assert_eq!(record.record_type, RECORD_EVENT);
        assert_eq!(record.flags, 3);
        assert_eq!(record.payload, b"hello".to_vec());
    }

    #[test]
    fn decode_rtp_tls_recovers_stream_id_and_rtp_bytes() {
        let payload = encode_rtp_tls(42, &[0x80, 0x60, 0x12, 0x34]);

        let (stream_id, rtp) = decode_rtp_tls(&payload).expect("decode framed rtp");

        assert_eq!(stream_id, 42);
        assert_eq!(rtp, vec![0x80, 0x60, 0x12, 0x34]);
    }

    #[tokio::test]
    async fn tls_rtp_record_roundtrip_preserves_stream_id_on_wire() {
        let (mut a, mut b) = duplex(1024);
        let payload = encode_rtp_tls(7, &[0x80, 0x60, 0x12, 0x34]);

        tokio::spawn(async move {
            write_record(&mut a, RECORD_RTP, 0, &payload).await.unwrap();
        });

        let record = read_record(&mut b).await.unwrap();
        assert_eq!(record.record_type, RECORD_RTP);

        let (stream_id, rtp) = decode_rtp_tls(&record.payload).expect("decode framed rtp");
        assert_eq!(stream_id, 7);
        assert_eq!(rtp, vec![0x80, 0x60, 0x12, 0x34]);
    }

    #[tokio::test]
    async fn tls_rtp_records_distinguish_different_stream_ids() {
        let (mut a, mut b) = duplex(1024);
        let first = encode_rtp_tls(1, &[0x80, 0x60, 0xAA, 0xBB]);
        let second = encode_rtp_tls(2, &[0x80, 0x60, 0xAA, 0xBB]);

        tokio::spawn(async move {
            write_record(&mut a, RECORD_RTP, 0, &first).await.unwrap();
            write_record(&mut a, RECORD_RTP, 0, &second).await.unwrap();
        });

        let first_record = read_record(&mut b).await.unwrap();
        let second_record = read_record(&mut b).await.unwrap();

        assert_eq!(first_record.record_type, RECORD_RTP);
        assert_eq!(second_record.record_type, RECORD_RTP);
        assert_ne!(first_record.payload, second_record.payload);

        let (first_stream_id, first_rtp) =
            decode_rtp_tls(&first_record.payload).expect("decode first framed rtp");
        let (second_stream_id, second_rtp) =
            decode_rtp_tls(&second_record.payload).expect("decode second framed rtp");

        assert_eq!(first_stream_id, 1);
        assert_eq!(second_stream_id, 2);
        assert_eq!(first_rtp, second_rtp);
    }

    #[test]
    fn encode_rtp_tls_changes_only_the_stream_prefix_for_identical_rtp_bodies() {
        let first = encode_rtp_tls(1, &[0x80, 0x60, 0xAA, 0xBB]);
        let second = encode_rtp_tls(2, &[0x80, 0x60, 0xAA, 0xBB]);

        assert_ne!(&first[..4], &second[..4]);
        assert_eq!(&first[4..], &second[4..]);
    }

    #[tokio::test]
    async fn tls_rtp_write_path_from_msg_is_server_compatible() {
        let (mut a, mut b) = duplex(1024);
        let msg = Msg {
            msg_type: RTP,
            stream_id: 2,
            payload: vec![0x80, 0x60, 0x12, 0x34],
        };

        tokio::spawn(async move {
            let payload = match msg.msg_type {
                RTP => encode_rtp_tls(msg.stream_id, &msg.payload),
                _ => unreachable!("test only covers RTP"),
            };
            write_record(&mut a, RECORD_RTP, 0, &payload).await.unwrap();
        });

        let record = read_record(&mut b).await.unwrap();
        assert_eq!(record.record_type, RECORD_RTP);
        assert_eq!(
            record.payload,
            vec![0x00, 0x00, 0x00, 0x02, 0x80, 0x60, 0x12, 0x34]
        );

        let (stream_id, rtp) = decode_rtp_tls(&record.payload).expect("decode framed rtp");
        assert_eq!(stream_id, 2);
        assert_eq!(rtp, vec![0x80, 0x60, 0x12, 0x34]);
    }

    #[tokio::test]
    async fn gap_tls_record_roundtrip_still_uses_stream_prefixed_payload() {
        let (mut a, mut b) = duplex(1024);
        let msg = Msg {
            msg_type: GAP,
            stream_id: 9,
            payload: encode_gap_tls(9, 0x1234, 0xABCD),
        };

        tokio::spawn(async move {
            write_record(&mut a, RECORD_GAP, 0, &msg.payload)
                .await
                .unwrap();
        });

        let record = read_record(&mut b).await.unwrap();
        assert_eq!(record.record_type, RECORD_GAP);
        assert_eq!(record.payload, vec![0, 0, 0, 9, 0x12, 0x34, 0xAB, 0xCD]);
    }

    #[test]
    fn resolve_server_name_prefers_runtime_override() {
        let _guard = EnvGuard::new();
        AgentConfig::runtime_restore(HashMap::from([(
            "INGEST_TLS_SERVER_NAME".to_string(),
            "override.example".to_string(),
        )]));

        let server_name = resolve_server_name("10.0.0.5:7443").expect("server name");
        match server_name {
            rustls::pki_types::ServerName::DnsName(name) => {
                assert_eq!(name.as_ref(), "override.example");
            }
            other => panic!("expected DNS server name, got {other:?}"),
        }
    }

    #[test]
    fn resolve_server_name_uses_ip_from_server_addr_when_no_override() {
        let _guard = EnvGuard::new();

        let server_name = resolve_server_name("127.0.0.1:7443").expect("server name");
        assert!(matches!(
            server_name,
            rustls::pki_types::ServerName::IpAddress(_)
        ));
    }

    #[test]
    fn load_tls_config_rejects_empty_ca_bundle() {
        let ca_path = test_path("empty-ca");
        std::fs::write(&ca_path, "").expect("write empty ca bundle");

        let err = load_tls_config(&ca_path).expect_err("empty CA bundle should fail");
        assert!(err.to_string().contains("no CA certs found"));

        let _ = std::fs::remove_file(&ca_path);
    }
}

fn build_event_payload(msg: &MotionEnvelope) -> EventPayload {
    let event_ts = chrono::DateTime::parse_from_rfc3339(&msg.ts)
        .ok()
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });
    let event_id = if msg.event_id.is_empty() {
        if event_ts > 0 {
            Some(format!("stream{}-{}", msg.stream_id, event_ts))
        } else {
            None
        }
    } else {
        Some(msg.event_id.clone())
    };
    EventPayload {
        stream_id: msg.stream_id.to_string(),
        event_type: "motion".to_string(),
        state: if msg.active { "start" } else { "stop" }.to_string(),
        event_ts_unix_ms: event_ts,
        confidence: 0.0,
        rule: Some(msg.rule.clone()),
        event_id,
        camera_id: Some(msg.camera_id.clone()),
    }
}

fn load_tls_config(ca_path: &Path) -> Result<rustls::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    let certs = load_certs(ca_path)?;
    if certs.is_empty() {
        return Err(anyhow!("no CA certs found in {}", ca_path.display()));
    }
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|_| anyhow!("failed to add CA cert from {}", ca_path.display()))?;
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(config)
}

fn load_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(std::fs::File::open(path)?);
    let certs = rustls_pemfile::certs(&mut reader).collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(certs)
}

fn resolve_server_name(server_addr: &str) -> Result<ServerName<'static>> {
    if let Some(name) = AgentConfig::runtime_var("INGEST_TLS_SERVER_NAME") {
        if let Ok(ip) = name.parse::<IpAddr>() {
            return Ok(ServerName::IpAddress(ip.into()));
        }
        return Ok(ServerName::try_from(name)?);
    }

    let url = Url::parse(&format!("tcp://{server_addr}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("server host missing in {}", server_addr))?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    Ok(ServerName::try_from(host.to_string())?)
}
