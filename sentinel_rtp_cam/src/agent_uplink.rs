use serde::Serialize;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

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
                match TcpStream::connect(&server_addr).await {
                    Ok(mut stream) => {
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
                                warn!(error = %e, "Failed to serialize HELLO");
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
                            warn!(error = %e, "Failed to send HELLO");
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
                                        warn!(error = %e, "Uplink write failed, reconnecting");
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
                                        warn!(error = %e, "Uplink ping failed, reconnecting");
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
                        warn!(error = %e, server = %server_addr, "Uplink connect failed");
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
