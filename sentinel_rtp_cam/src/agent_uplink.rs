use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

use crate::proto::{encode_gap, write_msg, Msg, GAP, HELLO, MOTION, PING, RTP};

#[derive(Clone)]
pub struct Uplink {
    tx: mpsc::Sender<Msg>,
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

impl Uplink {
    pub fn connect_and_run(
        server_addr: String,
        token: String,
        agent_id: String,
        stream_map: HashMap<u32, String>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Msg>(4096);

        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match TcpStream::connect(&server_addr).await {
                    Ok(mut stream) => {
                        info!(server = %server_addr, "Uplink connected");
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

                        let mut ping = interval(Duration::from_secs(10));
                        let mut sent: u64 = 0;
                        loop {
                            tokio::select! {
                                Some(msg) = rx.recv() => {
                                    if let Err(e) = write_msg(&mut stream, &msg).await {
                                        warn!(error = %e, "Uplink write failed, reconnecting");
                                        break;
                                    }
                                    sent += 1;
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

        Self { tx }
    }

    pub fn send_rtp(&self, stream_id: u32, rtp_bytes: Vec<u8>) {
        let msg = Msg {
            msg_type: RTP,
            stream_id,
            payload: rtp_bytes,
        };
        let _ = self.tx.try_send(msg);
    }

    pub fn send_gap(&self, stream_id: u32, last_seq: u16, new_seq: u16) {
        let msg = Msg {
            msg_type: GAP,
            stream_id,
            payload: encode_gap(last_seq, new_seq),
        };
        let _ = self.tx.try_send(msg);
    }

    pub fn send_motion(&self, stream_id: u32, rule: String, active: bool, ts: String) {
        let payload = serde_json::json!({
            "rule": rule,
            "active": active,
            "ts": ts,
        });
        let msg = Msg {
            msg_type: MOTION,
            stream_id,
            payload: serde_json::to_vec(&payload).unwrap_or_default(),
        };
        let _ = self.tx.try_send(msg);
    }
}
