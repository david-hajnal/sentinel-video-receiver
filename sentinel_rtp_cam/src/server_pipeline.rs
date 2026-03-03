use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::core::clip_writer::ClipWriter;
use crate::core::h264_depacketize::H264Depacketizer;
use crate::core::rtp::RtpPacket;
use crate::core::video::nal_type_from_annexb;

#[derive(Debug)]
pub enum StreamMsg {
    Rtp(Vec<u8>),
    Gap {
        last: u16,
        new: u16,
    },
    Motion {
        rule: String,
        active: bool,
        ts: String,
    },
}

#[derive(Clone)]
pub struct StreamConfig {
    pub stream_id: u32,
    pub clip_dir: PathBuf,
    pub ring_secs: u64,
    pub pre_secs: u64,
    pub post_secs: u64,
    pub stale_part_secs: u64,
    pub max_clip_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct AccessUnit {
    ts: Instant,
    nals: Vec<Vec<u8>>,
    has_idr: bool,
}

struct ClipState {
    writer: ClipWriter,
    started_at: DateTime<Utc>,
    rule: String,
    stop_at: Instant,
    hard_stop_at: Option<Instant>,
}

#[derive(Serialize)]
struct ClipSidecar {
    stream_id: u32,
    rule: String,
    filename: String,
    started_at: String,
    ended_at: String,
    duration_secs: i64,
}

pub async fn run_stream(cfg: StreamConfig, mut rx: mpsc::Receiver<StreamMsg>) -> Result<()> {
    tokio::fs::create_dir_all(&cfg.clip_dir).await?;
    cleanup_stale_parts(&cfg.clip_dir, cfg.stale_part_secs).await?;

    let mut dep = H264Depacketizer::new();
    let mut expected_seq: Option<u16> = None;
    let mut synced = false;
    let mut sps: Option<Vec<u8>> = None;
    let mut pps: Option<Vec<u8>> = None;

    let mut ring: VecDeque<AccessUnit> = VecDeque::new();
    let mut current_au: Vec<Vec<u8>> = Vec::new();

    let mut clip: Option<ClipState> = None;
    let mut clip_pending = false;
    let mut pending_rule = String::new();
    let mut pending_ring_snapshot: Option<VecDeque<AccessUnit>> = None;

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Some(cs) = &clip {
                    if should_finalize_clip(Instant::now(), cs.stop_at, cs.hard_stop_at) {
                        finalize_clip(cfg.stream_id, clip.take()).await?;
                    }
                }
            }
            Some(msg) = rx.recv() => {
                match msg {
                    StreamMsg::Gap { .. } => {
                        dep.reset();
                        synced = false;
                        expected_seq = None;
                        current_au.clear();
                    }
                    StreamMsg::Motion { rule, active, ts: _ } => {
                        if active {
                            if clip.is_none() {
                                clip_pending = true;
                                pending_rule = rule;
                                pending_ring_snapshot = Some(ring.clone());
                            }
                        }
                    }
                    StreamMsg::Rtp(bytes) => {
                        let pkt = match RtpPacket::parse(&bytes) {
                            Ok(p) => p,
                            Err(_) => {
                                dep.reset();
                                synced = false;
                                expected_seq = None;
                                current_au.clear();
                                continue;
                            }
                        };

                        if let Some(exp) = expected_seq {
                            if pkt.sequence_number != exp {
                                dep.reset();
                                synced = false;
                                expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                                current_au.clear();
                                continue;
                            }
                        }
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));

                        let nals = match dep.push_rtp_payload(pkt.payload) {
                            Ok(v) => v,
                            Err(_) => {
                                dep.reset();
                                synced = false;
                                expected_seq = None;
                                current_au.clear();
                                continue;
                            }
                        };

                        for nal in nals {
                            current_au.push(nal);
                        }

                        if pkt.marker {
                            if current_au.is_empty() {
                                continue;
                            }

                            let mut au = build_access_unit(&current_au, &mut sps, &mut pps);
                            current_au.clear();

                            if !synced {
                                if au.has_idr && sps.is_some() && pps.is_some() {
                                    let mut with_ps = Vec::new();
                                    with_ps.push(sps.as_ref().unwrap().clone());
                                    with_ps.push(pps.as_ref().unwrap().clone());
                                    with_ps.extend(au.nals.drain(..));
                                    au.nals = with_ps;
                                    synced = true;
                                } else {
                                    continue;
                                }
                            }

                            ring.push_back(au);
                            prune_ring(&mut ring, cfg.ring_secs);

                            if clip_pending && clip.is_none() {
                                if let Some(start) = try_start_clip(&cfg, &pending_rule, &pending_ring_snapshot, sps.as_ref(), pps.as_ref()).await? {
                                    clip = Some(start);
                                    clip_pending = false;
                                    pending_ring_snapshot = None;
                                }
                            }

                            if let Some(cs) = &mut clip {
                                if let Some(au) = ring.back() {
                                    for nal in &au.nals {
                                        let _ = cs.writer.write_nal(nal).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else => break,
        }
    }

    Ok(())
}

fn build_access_unit(
    nals: &[Vec<u8>],
    sps: &mut Option<Vec<u8>>,
    pps: &mut Option<Vec<u8>>,
) -> AccessUnit {
    let mut has_idr = false;
    let mut out = Vec::with_capacity(nals.len());

    for nal in nals {
        if let Some(nt) = nal_type_from_annexb(nal) {
            if nt == 7 {
                *sps = Some(nal.clone());
            } else if nt == 8 {
                *pps = Some(nal.clone());
            } else if nt == 5 {
                has_idr = true;
            }
        }
        out.push(nal.clone());
    }

    AccessUnit {
        ts: Instant::now(),
        nals: out,
        has_idr,
    }
}

fn prune_ring(ring: &mut VecDeque<AccessUnit>, ring_secs: u64) {
    let now = Instant::now();
    let keep = Duration::from_secs(ring_secs);
    while let Some(front) = ring.front() {
        if now.duration_since(front.ts) > keep {
            ring.pop_front();
        } else {
            break;
        }
    }
}

fn should_finalize_clip(now: Instant, stop_at: Instant, hard_stop_at: Option<Instant>) -> bool {
    let stop_due = now >= stop_at;
    let hard_due = hard_stop_at.map(|t| now >= t).unwrap_or(false);
    stop_due || hard_due
}

async fn try_start_clip(
    cfg: &StreamConfig,
    rule: &str,
    ring_snapshot: &Option<VecDeque<AccessUnit>>,
    sps: Option<&Vec<u8>>,
    pps: Option<&Vec<u8>>,
) -> Result<Option<ClipState>> {
    let ts = Utc::now().format("%Y%m%d_%H%M%S%.3fZ").to_string();
    let safe_rule = rule.replace(['/', '\\', ' '], "_");
    let file_path = cfg
        .clip_dir
        .join(format!("{ts}_{}_{}.h264", cfg.stream_id, safe_rule));
    let part_path = cfg
        .clip_dir
        .join(format!("{ts}_{}_{}.h264.part", cfg.stream_id, safe_rule));

    let mut writer = ClipWriter::create(part_path, file_path.clone(), 256 * 1024).await?;

    let mut wrote_any = false;
    if let Some(snapshot) = ring_snapshot {
        let mut found = false;
        for au in snapshot {
            if !found {
                if au.has_idr {
                    found = true;
                    if let (Some(s), Some(p)) = (sps, pps) {
                        writer.write_nal(s).await?;
                        writer.write_nal(p).await?;
                    }
                } else {
                    continue;
                }
            }
            for nal in &au.nals {
                writer.write_nal(nal).await?;
                wrote_any = true;
            }
        }
        if !found {
            debug!(stream_id = cfg.stream_id, "No IDR in preroll window");
        }
    }

    if !wrote_any {
        // Wait for first IDR AU to arrive before writing anything
        if sps.is_none() || pps.is_none() {
            return Ok(None);
        }
    }

    let started_at = Utc::now();
    let now = Instant::now();
    let stop_at = now + Duration::from_secs(cfg.post_secs);
    let hard_stop_at = cfg
        .max_clip_secs
        .map(|secs| now + Duration::from_secs(secs));

    Ok(Some(ClipState {
        writer,
        started_at,
        rule: rule.to_string(),
        stop_at,
        hard_stop_at,
    }))
}

async fn finalize_clip(stream_id: u32, clip: Option<ClipState>) -> Result<()> {
    let Some(cs) = clip else {
        return Ok(());
    };

    let part_path = cs.writer.part_path().clone();
    let final_path = cs.writer.finalize().await?;

    let ended_at = Utc::now();
    let duration_secs = (ended_at - cs.started_at).num_seconds();

    let sidecar = ClipSidecar {
        stream_id,
        rule: cs.rule.clone(),
        filename: final_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown.h264")
            .to_string(),
        started_at: cs.started_at.to_rfc3339(),
        ended_at: ended_at.to_rfc3339(),
        duration_secs,
    };

    let json_path = final_path.with_extension("json");
    let json = serde_json::to_vec_pretty(&sidecar)?;
    tokio::fs::write(&json_path, json).await?;

    info!(file = ?final_path, part_file = ?part_path, duration_secs = duration_secs, "Clip finalized");
    Ok(())
}

async fn cleanup_stale_parts(dir: &PathBuf, stale_part_secs: u64) -> Result<()> {
    let cutoff = SystemTime::now() - Duration::from_secs(stale_part_secs);
    let mut read_dir = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if !filename.ends_with(".h264.part") {
            continue;
        }
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(_) => continue,
        };
        let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if mtime < cutoff {
            let _ = tokio::fs::remove_file(&path).await;
            warn!(file = ?path, "Deleted stale .part clip");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::should_finalize_clip;
    use std::time::{Duration, Instant};

    #[test]
    fn post_secs_deadline_finalizes_clip() {
        let now = Instant::now();
        let stop_at = now - Duration::from_secs(1);
        assert!(should_finalize_clip(now, stop_at, None));
    }

    #[test]
    fn max_clip_secs_deadline_finalizes_clip() {
        let now = Instant::now();
        let stop_at = now + Duration::from_secs(30);
        let hard_stop_at = Some(now - Duration::from_secs(1));
        assert!(should_finalize_clip(now, stop_at, hard_stop_at));
    }

    #[test]
    fn clip_keeps_recording_before_deadlines() {
        let now = Instant::now();
        let stop_at = now + Duration::from_secs(10);
        let hard_stop_at = Some(now + Duration::from_secs(5));
        assert!(!should_finalize_clip(now, stop_at, hard_stop_at));
    }
}
