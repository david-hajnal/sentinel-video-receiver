use anyhow::{anyhow, Result};
use rustls::pki_types::ServerName;
use serde::Serialize;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::proto::{encode_gap, write_msg, Msg, GAP, HELLO, MOTION, PING, RTP};

#[derive(Clone)]
pub struct Uplink {
    tx: mpsc::Sender<Msg>,
    stats: Arc<UplinkStats>,
}

#[derive(Debug, Serialize)]
struct HelloPayload {
    agent_id: String,
    token: String,
    streams: Vec<HelloStream>,
}

#[derive(Debug, Serialize)]
struct HelloStream {
    stream_id: u32,
    name: String,
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
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Msg>(4096);
        let stats = Arc::new(UplinkStats::default());

        let stats_task = stats.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                let (connector, server_name) = match build_tls_connector(&server_addr) {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, server = %server_addr, "Uplink TLS config failed");
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                };

                match TcpStream::connect(&server_addr).await {
                    Ok(stream) => {
                        let mut stream = match connector.connect(server_name, stream).await {
                            Ok(tls) => tls,
                            Err(e) => {
                                error!(error = %e, server = %server_addr, "Uplink TLS handshake failed");
                                sleep(backoff).await;
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

                        let hello = HelloPayload {
                            agent_id: agent_id.clone(),
                            token: token.clone(),
                            streams: stream_map
                                .iter()
                                .map(|(id, name)| HelloStream {
                                    stream_id: *id,
                                    name: name.clone(),
                                })
                                .collect(),
                        };
                        let payload = match serde_json::to_vec(&hello) {
                            Ok(p) => p,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize HELLO");
                                sleep(backoff).await;
                                continue;
                            }
                        };
                        let hello_msg = Msg {
                            msg_type: HELLO,
                            stream_id: 0,
                            payload,
                        };
                        if let Err(e) = write_msg(&mut stream, &hello_msg).await {
                            error!(error = %e, "Failed to send HELLO");
                            sleep(backoff).await;
                            continue;
                        }
                        info!(stream_count = stream_map.len(), "Uplink HELLO sent");

                        let mut ping = interval(Duration::from_secs(10));
                        let mut sent: u64 = 0;
                        let mut last_log = Instant::now();
                        let mut last_rtp = 0u64;
                        let mut last_gap = 0u64;
                        let mut last_motion = 0u64;
                        let mut last_dropped = 0u64;
                        loop {
                            tokio::select! {
                                Some(msg) = rx.recv() => {
                                    if let Err(e) = write_msg(&mut stream, &msg).await {
                                        error!(error = %e, "Uplink write failed, reconnecting");
                                        break;
                                    }
                                    sent += 1;
                                    match msg.msg_type {
                                        RTP => { stats_task.rtp_sent.fetch_add(1, Ordering::Relaxed); }
                                        GAP => { stats_task.gap_sent.fetch_add(1, Ordering::Relaxed); }
                                        MOTION => { stats_task.motion_sent.fetch_add(1, Ordering::Relaxed); }
                                        _ => {}
                                    }
                                }
                                _ = ping.tick() => {
                                    let stats = serde_json::json!({
                                        "sent": sent,
                                    });
                                    let payload = serde_json::to_vec(&stats).unwrap_or_default();
                                    let ping_msg = Msg { msg_type: PING, stream_id: 0, payload };
                                    if let Err(e) = write_msg(&mut stream, &ping_msg).await {
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
                    }
                    Err(e) => {
                        error!(error = %e, server = %server_addr, "Uplink connect failed");
                    }
                }

                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        });

        Self { tx, stats }
    }

    pub fn send_rtp(&self, stream_id: u32, rtp_bytes: Vec<u8>) {
        let msg = Msg {
            msg_type: RTP,
            stream_id,
            payload: rtp_bytes,
        };
        if let Err(e) = self.tx.try_send(msg) {
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
            payload: encode_gap(last_seq, new_seq),
        };
        if let Err(e) = self.tx.try_send(msg) {
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
        let payload = serde_json::json!({
            "rule": rule,
            "active": active,
            "ts": ts,
            "camera_id": camera_id,
            "event_id": event_id,
        });
        let msg = Msg {
            msg_type: MOTION,
            stream_id,
            payload: serde_json::to_vec(&payload).unwrap_or_default(),
        };
        if let Err(e) = self.tx.try_send(msg) {
            let dropped = self.stats.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped <= 3 || dropped % 20 == 0 {
                warn!(
                    stream_id,
                    dropped_total = dropped,
                    error = %e,
                    "Uplink channel full; dropping MOTION"
                );
            }
        }
    }
}

fn build_tls_connector(server_addr: &str) -> Result<(TlsConnector, ServerName<'static>)> {
    let ca_path = std::env::var("INGEST_TLS_CA")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/etc/sentinel_rtp_cam/ca.crt"));
    let server_name = resolve_server_name(server_addr)?;
    let config = load_tls_config(&ca_path)?;
    Ok((TlsConnector::from(Arc::new(config)), server_name))
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
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(certs)
}

fn resolve_server_name(server_addr: &str) -> Result<ServerName<'static>> {
    if let Ok(name) = std::env::var("INGEST_TLS_SERVER_NAME") {
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
