use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::VecDeque;
#[cfg(test)]
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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
    pub clip_write_timeout_ms: u64,
    pub clip_write_retry_count: u64,
    pub clip_write_retry_backoff_ms: u64,
    pub stale_secs: u64,
    pub stats_log_secs: u64,
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

#[derive(Clone, Copy)]
struct ClipWriteSettings {
    timeout: Duration,
    retry_count: u64,
    retry_backoff: Duration,
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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StreamHealthState {
    Idle,
    Receiving,
    Stale,
}

#[derive(Debug, Serialize)]
struct StreamHealthSnapshot {
    stream_id: u32,
    state: StreamHealthState,
    updated_at: String,
    stale_after_secs: u64,
    ring_secs: u64,
    clip_active: bool,
    clip_pending: bool,
    total_rtp_packets: u64,
    total_access_units: u64,
    total_gaps: u64,
    total_motion_events: u64,
    total_parse_errors: u64,
    last_rtp_at: Option<String>,
    last_access_unit_at: Option<String>,
    last_gap_at: Option<String>,
    last_motion_at: Option<String>,
    last_error_at: Option<String>,
}

#[derive(Debug, Default)]
struct StreamRuntimeStats {
    total_rtp_packets: u64,
    total_access_units: u64,
    total_gaps: u64,
    total_motion_events: u64,
    total_parse_errors: u64,
    last_rtp_at: Option<DateTime<Utc>>,
    last_access_unit_at: Option<DateTime<Utc>>,
    last_gap_at: Option<DateTime<Utc>>,
    last_motion_at: Option<DateTime<Utc>>,
    last_error_at: Option<DateTime<Utc>>,
    last_idr_at: Option<DateTime<Utc>>,
    last_stats_log_at: Option<Instant>,
    last_health_write_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct NoDecodeEpisode {
    first_rtp_at: Option<Instant>,
    access_units_at_start: u64,
    logged: bool,
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
    let mut stats = StreamRuntimeStats::default();
    let mut no_decode_episode = NoDecodeEpisode::default();

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let clip_write = ClipWriteSettings {
        timeout: Duration::from_millis(cfg.clip_write_timeout_ms.max(1)),
        retry_count: cfg.clip_write_retry_count,
        retry_backoff: Duration::from_millis(cfg.clip_write_retry_backoff_ms),
    };

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Some(cs) = &clip {
                    if should_finalize_clip(Instant::now(), cs.stop_at, cs.hard_stop_at) {
                        finalize_clip(cfg.stream_id, clip.take()).await?;
                    }
                }
                maybe_log_no_decode_watchdog(cfg.stream_id, &stats, &mut no_decode_episode);
                maybe_log_stream_stats(&cfg, &mut stats, clip.is_some(), clip_pending);
                maybe_write_stream_health(&cfg, &mut stats, clip.is_some(), clip_pending).await?;
            }
            Some(msg) = rx.recv() => {
                match msg {
                    StreamMsg::Gap { .. } => {
                        stats.total_gaps = stats.total_gaps.saturating_add(1);
                        stats.last_gap_at = Some(Utc::now());
                        dep.reset();
                        synced = false;
                        expected_seq = None;
                        current_au.clear();
                    }
                    StreamMsg::Motion { rule, active, ts: _ } => {
                        stats.total_motion_events = stats.total_motion_events.saturating_add(1);
                        stats.last_motion_at = Some(Utc::now());
                        if active {
                            if clip.is_none() {
                                clip_pending = true;
                                pending_rule = rule;
                                pending_ring_snapshot = Some(ring.clone());
                            }
                        }
                    }
                    StreamMsg::Rtp(bytes) => {
                        stats.total_rtp_packets = stats.total_rtp_packets.saturating_add(1);
                        stats.last_rtp_at = Some(Utc::now());
                        if no_decode_episode.first_rtp_at.is_none() {
                            no_decode_episode.first_rtp_at = Some(Instant::now());
                            no_decode_episode.access_units_at_start = stats.total_access_units;
                            no_decode_episode.logged = false;
                        }
                        let pkt = match RtpPacket::parse(&bytes) {
                            Ok(p) => p,
                            Err(error) => {
                                mark_stream_error(&mut stats);
                                maybe_log_rate_limited_parse_warn(
                                    cfg.stream_id,
                                    stats.total_parse_errors,
                                    &error.to_string(),
                                    bytes.len(),
                                    bytes.first().copied(),
                                    None,
                                );
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
                            Err(error) => {
                                mark_stream_error(&mut stats);
                                maybe_log_rate_limited_parse_warn(
                                    cfg.stream_id,
                                    stats.total_parse_errors,
                                    &error.to_string(),
                                    pkt.payload.len(),
                                    pkt.payload.first().copied(),
                                    derive_rtp_payload_nal_type(pkt.payload),
                                );
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
                            stats.total_access_units = stats.total_access_units.saturating_add(1);
                            stats.last_access_unit_at = Some(Utc::now());
                            if ring.back().is_some_and(|unit| unit.has_idr) {
                                stats.last_idr_at = Some(Utc::now());
                            }
                            no_decode_episode = NoDecodeEpisode::default();
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
                                        if !write_active_clip_nal_with_retry(
                                            cfg.stream_id,
                                            cs,
                                            nal,
                                            clip_write,
                                        )
                                        .await
                                        {
                                            clip = None;
                                            break;
                                        }
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

    write_stream_health(&cfg, &stats, clip.is_some(), clip_pending).await?;
    Ok(())
}

async fn write_active_clip_nal_with_retry(
    stream_id: u32,
    clip: &mut ClipState,
    nal: &[u8],
    settings: ClipWriteSettings,
) -> bool {
    let part_path = clip.writer.part_path().clone();
    let attempts = settings.retry_count.saturating_add(1);
    for attempt in 1..=attempts {
        let result = tokio::time::timeout(settings.timeout, clip.writer.write_nal(nal)).await;
        match result {
            Ok(Ok(())) => return true,
            Ok(Err(error)) if attempt < attempts => {
                warn!(
                    stream_id,
                    rule = %clip.rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    error = %error,
                    "Clip write failed, retrying"
                );
                if settings.retry_backoff > Duration::ZERO {
                    tokio::time::sleep(settings.retry_backoff).await;
                }
                continue;
            }
            Ok(Err(error)) => {
                error!(
                    stream_id,
                    rule = %clip.rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    error = %error,
                    "Clip write failed, dropping active clip"
                );
            }
            Err(_) if attempt < attempts => {
                warn!(
                    stream_id,
                    rule = %clip.rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    "Clip write timed out, retrying"
                );
                if settings.retry_backoff > Duration::ZERO {
                    tokio::time::sleep(settings.retry_backoff).await;
                }
                continue;
            }
            Err(_) => {
                error!(
                    stream_id,
                    rule = %clip.rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    "Clip write timed out, dropping active clip"
                );
            }
        }

        if let Err(clean_error) = tokio::fs::remove_file(&part_path).await {
            warn!(
                stream_id,
                rule = %clip.rule,
                part_file = ?part_path,
                error = %clean_error,
                "Failed to delete .part file after clip write failure"
            );
        }
        return false;
    }

    false
}

#[cfg(test)]
async fn write_with_retry_and_cleanup<F, Fut>(
    stream_id: u32,
    rule: &str,
    part_path: &PathBuf,
    settings: ClipWriteSettings,
    mut op: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let attempts = settings.retry_count.saturating_add(1);
    for attempt in 1..=attempts {
        let result = tokio::time::timeout(settings.timeout, op()).await;
        match result {
            Ok(Ok(())) => return true,
            Ok(Err(error)) if attempt < attempts => {
                warn!(
                    stream_id,
                    rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    error = %error,
                    "Clip write failed, retrying"
                );
                if settings.retry_backoff > Duration::ZERO {
                    tokio::time::sleep(settings.retry_backoff).await;
                }
                continue;
            }
            Ok(Err(error)) => {
                error!(
                    stream_id,
                    rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    error = %error,
                    "Clip write failed, dropping active clip"
                );
            }
            Err(_) if attempt < attempts => {
                warn!(
                    stream_id,
                    rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    "Clip write timed out, retrying"
                );
                if settings.retry_backoff > Duration::ZERO {
                    tokio::time::sleep(settings.retry_backoff).await;
                }
                continue;
            }
            Err(_) => {
                error!(
                    stream_id,
                    rule,
                    part_file = ?part_path,
                    attempt,
                    max_attempts = attempts,
                    timeout_ms = settings.timeout.as_millis(),
                    retry_backoff_ms = settings.retry_backoff.as_millis(),
                    "Clip write timed out, dropping active clip"
                );
            }
        }

        if let Err(clean_error) = tokio::fs::remove_file(part_path).await {
            warn!(
                stream_id,
                rule,
                part_file = ?part_path,
                error = %clean_error,
                "Failed to delete .part file after clip write failure"
            );
        }
        return false;
    }

    false
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

fn mark_stream_error(stats: &mut StreamRuntimeStats) {
    stats.total_parse_errors = stats.total_parse_errors.saturating_add(1);
    stats.last_error_at = Some(Utc::now());
}

fn derive_rtp_payload_nal_type(payload: &[u8]) -> Option<u8> {
    let first = *payload.first()?;
    let nal_type = first & 0x1F;
    match nal_type {
        1..=23 => Some(nal_type),
        28 => payload.get(1).map(|v| v & 0x1F),
        _ => None,
    }
}

fn should_log_rate_limited(count: u64) -> bool {
    count <= 3 || count % 100 == 0
}

fn maybe_log_rate_limited_parse_warn(
    stream_id: u32,
    parse_error_count: u64,
    error_text: &str,
    payload_size: usize,
    first_payload_byte: Option<u8>,
    derived_nal_type: Option<u8>,
) {
    if !should_log_rate_limited(parse_error_count) {
        return;
    }
    warn!(
        stream_id,
        parse_error_count,
        error = %error_text,
        payload_size,
        first_payload_byte = first_payload_byte.map(|v| format!("0x{v:02X}")),
        derived_nal_type,
        "RTP/H264 parse failure"
    );
}

fn maybe_log_no_decode_watchdog(
    stream_id: u32,
    stats: &StreamRuntimeStats,
    episode: &mut NoDecodeEpisode,
) {
    if episode.first_rtp_at.is_none() {
        return;
    }
    if stats.total_access_units > episode.access_units_at_start {
        *episode = NoDecodeEpisode::default();
        return;
    }
    if episode.logged {
        return;
    }
    let elapsed = episode
        .first_rtp_at
        .map(|started| started.elapsed())
        .unwrap_or(Duration::ZERO);
    if elapsed < Duration::from_secs(30) {
        return;
    }
    error!(
        stream_id,
        total_rtp_packets = stats.total_rtp_packets,
        total_access_units = stats.total_access_units,
        total_parse_errors = stats.total_parse_errors,
        last_idr_at = stats.last_idr_at.map(|ts| ts.to_rfc3339()),
        segment_seq = 0_u64,
        "Startup watchdog: receiving media but not decoding a publishable H.264 stream"
    );
    episode.logged = true;
}

fn derive_stream_health_state(
    last_rtp_at: Option<DateTime<Utc>>,
    stale_secs: u64,
    now: DateTime<Utc>,
) -> StreamHealthState {
    let Some(last_rtp_at) = last_rtp_at else {
        return StreamHealthState::Idle;
    };
    let elapsed_secs = now
        .signed_duration_since(last_rtp_at)
        .num_seconds()
        .max(0) as u64;
    if elapsed_secs <= stale_secs.max(1) {
        StreamHealthState::Receiving
    } else {
        StreamHealthState::Stale
    }
}

fn maybe_log_stream_stats(
    cfg: &StreamConfig,
    stats: &mut StreamRuntimeStats,
    clip_active: bool,
    clip_pending: bool,
) {
    let now = Instant::now();
    let interval = Duration::from_secs(cfg.stats_log_secs.max(5));
    if stats
        .last_stats_log_at
        .is_some_and(|last| now.duration_since(last) < interval)
    {
        return;
    }

    let state = derive_stream_health_state(stats.last_rtp_at, cfg.stale_secs, Utc::now());
    info!(
        stream_id = cfg.stream_id,
        state = ?state,
        clip_active,
        clip_pending,
        total_rtp_packets = stats.total_rtp_packets,
        total_access_units = stats.total_access_units,
        total_gaps = stats.total_gaps,
        total_motion_events = stats.total_motion_events,
        total_parse_errors = stats.total_parse_errors,
        "Stream pipeline stats"
    );
    stats.last_stats_log_at = Some(now);
}

async fn maybe_write_stream_health(
    cfg: &StreamConfig,
    stats: &mut StreamRuntimeStats,
    clip_active: bool,
    clip_pending: bool,
) -> Result<()> {
    let now = Instant::now();
    let interval = Duration::from_secs(1);
    if stats
        .last_health_write_at
        .is_some_and(|last| now.duration_since(last) < interval)
    {
        return Ok(());
    }

    write_stream_health(cfg, stats, clip_active, clip_pending).await?;
    stats.last_health_write_at = Some(now);
    Ok(())
}

async fn write_stream_health(
    cfg: &StreamConfig,
    stats: &StreamRuntimeStats,
    clip_active: bool,
    clip_pending: bool,
) -> Result<()> {
    let now = Utc::now();
    let snapshot = StreamHealthSnapshot {
        stream_id: cfg.stream_id,
        state: derive_stream_health_state(stats.last_rtp_at, cfg.stale_secs, now),
        updated_at: now.to_rfc3339(),
        stale_after_secs: cfg.stale_secs,
        ring_secs: cfg.ring_secs,
        clip_active,
        clip_pending,
        total_rtp_packets: stats.total_rtp_packets,
        total_access_units: stats.total_access_units,
        total_gaps: stats.total_gaps,
        total_motion_events: stats.total_motion_events,
        total_parse_errors: stats.total_parse_errors,
        last_rtp_at: stats.last_rtp_at.map(|ts| ts.to_rfc3339()),
        last_access_unit_at: stats.last_access_unit_at.map(|ts| ts.to_rfc3339()),
        last_gap_at: stats.last_gap_at.map(|ts| ts.to_rfc3339()),
        last_motion_at: stats.last_motion_at.map(|ts| ts.to_rfc3339()),
        last_error_at: stats.last_error_at.map(|ts| ts.to_rfc3339()),
    };

    let final_path = cfg
        .clip_dir
        .join(format!("stream_{}.health.json", cfg.stream_id));
    let temp_path = cfg
        .clip_dir
        .join(format!("stream_{}.health.json.tmp", cfg.stream_id));
    let bytes = serde_json::to_vec_pretty(&snapshot)?;
    tokio::fs::write(&temp_path, bytes).await?;
    tokio::fs::rename(&temp_path, &final_path).await?;
    Ok(())
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
    use super::{
        cleanup_stale_parts, derive_stream_health_state, should_finalize_clip, try_start_clip,
        write_stream_health, write_with_retry_and_cleanup, AccessUnit, ClipWriteSettings,
        NoDecodeEpisode, StreamConfig, StreamHealthState, StreamRuntimeStats,
    };
    use anyhow::anyhow;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use tokio::fs;
    use ulid::Ulid;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sentinel_server_pipeline_{name}_{}", Ulid::new()))
    }

    fn stream_config(clip_dir: PathBuf) -> StreamConfig {
        StreamConfig {
            stream_id: 7,
            clip_dir,
            ring_secs: 10,
            pre_secs: 2,
            post_secs: 3,
            stale_part_secs: 1,
            max_clip_secs: Some(9),
            clip_write_timeout_ms: 5_000,
            clip_write_retry_count: 2,
            clip_write_retry_backoff_ms: 250,
            stale_secs: 15,
            stats_log_secs: 60,
        }
    }

    fn annexb(nal_header: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0, 0, 0, 1, nal_header];
        out.extend_from_slice(payload);
        out
    }

    fn access_unit(nals: Vec<Vec<u8>>, has_idr: bool) -> AccessUnit {
        AccessUnit {
            ts: Instant::now(),
            nals,
            has_idr,
        }
    }

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

    #[test]
    fn stream_health_is_idle_without_recent_rtp() {
        let now = Utc::now();
        assert_eq!(
            derive_stream_health_state(None, 15, now),
            StreamHealthState::Idle
        );
    }

    #[test]
    fn stream_health_is_receiving_within_stale_window() {
        let now = Utc::now();
        assert_eq!(
            derive_stream_health_state(Some(now - ChronoDuration::seconds(3)), 15, now),
            StreamHealthState::Receiving
        );
    }

    #[test]
    fn stream_health_is_stale_after_threshold() {
        let now = Utc::now();
        assert_eq!(
            derive_stream_health_state(Some(now - ChronoDuration::seconds(20)), 15, now),
            StreamHealthState::Stale
        );
    }

    #[test]
    fn derive_rtp_payload_nal_type_handles_single_nal_and_fu_a() {
        assert_eq!(super::derive_rtp_payload_nal_type(&[0x65, 0x01]), Some(5));
        assert_eq!(super::derive_rtp_payload_nal_type(&[0x7C, 0x85]), Some(5));
        assert_eq!(super::derive_rtp_payload_nal_type(&[0x78]), None);
    }

    #[test]
    fn should_log_rate_limited_matches_first_three_then_every_hundredth() {
        assert!(super::should_log_rate_limited(1));
        assert!(super::should_log_rate_limited(2));
        assert!(super::should_log_rate_limited(3));
        assert!(!super::should_log_rate_limited(4));
        assert!(super::should_log_rate_limited(100));
        assert!(!super::should_log_rate_limited(101));
    }

    #[test]
    fn no_decode_watchdog_latches_and_resets_after_recovery() {
        let mut stats = StreamRuntimeStats::default();
        stats.total_rtp_packets = 42;
        stats.total_parse_errors = 7;
        let mut episode = NoDecodeEpisode {
            first_rtp_at: Some(Instant::now() - Duration::from_secs(31)),
            access_units_at_start: 0,
            logged: false,
        };

        super::maybe_log_no_decode_watchdog(7, &stats, &mut episode);
        assert!(episode.logged);

        stats.total_access_units = 1;
        super::maybe_log_no_decode_watchdog(7, &stats, &mut episode);
        assert!(episode.first_rtp_at.is_none());
        assert!(!episode.logged);
    }

    #[tokio::test]
    async fn try_start_clip_returns_none_without_usable_idr_or_parameter_sets() {
        let clip_dir = test_dir("start_none");
        fs::create_dir_all(&clip_dir).await.unwrap();
        let cfg = stream_config(clip_dir.clone());
        let ring_snapshot = Some(VecDeque::from([access_unit(
            vec![annexb(0x41, &[0x01])],
            false,
        )]));

        let clip = try_start_clip(&cfg, "rule-a", &ring_snapshot, None, None)
            .await
            .unwrap();

        assert!(clip.is_none());

        fs::remove_dir_all(&clip_dir).await.unwrap();
    }

    #[tokio::test]
    async fn try_start_clip_writes_from_first_idr_and_prefixes_sps_pps() {
        let clip_dir = test_dir("start_preroll");
        fs::create_dir_all(&clip_dir).await.unwrap();
        let cfg = stream_config(clip_dir.clone());

        let pre_idr = annexb(0x41, &[0x10]);
        let idr = annexb(0x65, &[0x20]);
        let post_idr = annexb(0x41, &[0x30]);
        let sps = annexb(0x67, &[0x01, 0x02]);
        let pps = annexb(0x68, &[0x03, 0x04]);
        let ring_snapshot = Some(VecDeque::from([
            access_unit(vec![pre_idr.clone()], false),
            access_unit(vec![idr.clone()], true),
            access_unit(vec![post_idr.clone()], false),
        ]));

        let mut clip = try_start_clip(&cfg, "rule-a", &ring_snapshot, Some(&sps), Some(&pps))
            .await
            .unwrap()
            .expect("clip should start");
        let part_path = clip.writer.part_path().clone();
        clip.writer.flush().await.unwrap();

        let bytes = fs::read(&part_path).await.unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&sps);
        expected.extend_from_slice(&pps);
        expected.extend_from_slice(&idr);
        expected.extend_from_slice(&post_idr);

        assert_eq!(bytes, expected);
        assert!(!bytes.windows(pre_idr.len()).any(|window| window == pre_idr));

        fs::remove_dir_all(&clip_dir).await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_stale_parts_removes_only_old_part_files() {
        let clip_dir = test_dir("cleanup");
        fs::create_dir_all(&clip_dir).await.unwrap();

        let stale_part = clip_dir.join("old.h264.part");
        let fresh_part = clip_dir.join("fresh.h264.part");
        let non_part = clip_dir.join("keep.h264");

        fs::write(&stale_part, b"old").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        fs::write(&fresh_part, b"fresh").await.unwrap();
        fs::write(&non_part, b"keep").await.unwrap();

        cleanup_stale_parts(&clip_dir, 1).await.unwrap();

        assert!(fs::metadata(&stale_part).await.is_err());
        assert!(fs::metadata(&fresh_part).await.is_ok());
        assert!(fs::metadata(&non_part).await.is_ok());

        fs::remove_dir_all(&clip_dir).await.unwrap();
    }

    #[tokio::test]
    async fn write_stream_health_publishes_atomic_snapshot() {
        let clip_dir = test_dir("health");
        fs::create_dir_all(&clip_dir).await.unwrap();
        let cfg = stream_config(clip_dir.clone());
        let stats = StreamRuntimeStats {
            total_rtp_packets: 11,
            total_access_units: 4,
            total_gaps: 1,
            total_motion_events: 2,
            total_parse_errors: 0,
            last_rtp_at: Some(Utc::now()),
            last_access_unit_at: Some(Utc::now()),
            last_gap_at: Some(Utc::now()),
            last_motion_at: Some(Utc::now()),
            last_error_at: None,
            last_idr_at: None,
            last_stats_log_at: None,
            last_health_write_at: None,
        };

        write_stream_health(&cfg, &stats, true, false).await.unwrap();

        let health_path = clip_dir.join("stream_7.health.json");
        let raw = fs::read_to_string(&health_path).await.unwrap();
        assert!(raw.contains("\"stream_id\": 7"));
        assert!(raw.contains("\"state\": \"receiving\""));
        assert!(raw.contains("\"clip_active\": true"));
        assert!(raw.contains("\"total_rtp_packets\": 11"));
        assert!(fs::metadata(clip_dir.join("stream_7.health.json.tmp")).await.is_err());

        fs::remove_dir_all(&clip_dir).await.unwrap();
    }

    #[tokio::test]
    async fn clip_write_retry_succeeds_without_retry() {
        let clip_dir = test_dir("write_retry_success");
        fs::create_dir_all(&clip_dir).await.unwrap();
        let part = clip_dir.join("active.h264.part");
        fs::write(&part, b"seed").await.unwrap();
        let mut attempts = 0u64;
        let settings = ClipWriteSettings {
            timeout: Duration::from_millis(50),
            retry_count: 2,
            retry_backoff: Duration::from_millis(1),
        };

        let ok = write_with_retry_and_cleanup(7, "rule-a", &part, settings, || {
            attempts += 1;
            std::future::ready(Ok(()))
        })
        .await;

        assert!(ok);
        assert_eq!(attempts, 1);
        assert!(fs::metadata(&part).await.is_ok());
        fs::remove_dir_all(&clip_dir).await.unwrap();
    }

    #[tokio::test]
    async fn clip_write_retry_recovers_after_timeout_then_error() {
        let clip_dir = test_dir("write_retry_recover");
        fs::create_dir_all(&clip_dir).await.unwrap();
        let part = clip_dir.join("active.h264.part");
        fs::write(&part, b"seed").await.unwrap();
        let mut attempts = 0u64;
        let settings = ClipWriteSettings {
            timeout: Duration::from_millis(5),
            retry_count: 3,
            retry_backoff: Duration::from_millis(1),
        };

        let ok = write_with_retry_and_cleanup(7, "rule-a", &part, settings, || {
            attempts += 1;
            async move {
                match attempts {
                    1 => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(())
                    }
                    2 => Err(anyhow!("transient io error")),
                    _ => Ok(()),
                }
            }
        })
        .await;

        assert!(ok);
        assert_eq!(attempts, 3);
        assert!(fs::metadata(&part).await.is_ok());
        fs::remove_dir_all(&clip_dir).await.unwrap();
    }

    #[tokio::test]
    async fn clip_write_retry_exhaustion_removes_part_file() {
        let clip_dir = test_dir("write_retry_exhaust");
        fs::create_dir_all(&clip_dir).await.unwrap();
        let part = clip_dir.join("active.h264.part");
        fs::write(&part, b"seed").await.unwrap();
        let mut attempts = 0u64;
        let settings = ClipWriteSettings {
            timeout: Duration::from_millis(5),
            retry_count: 2,
            retry_backoff: Duration::from_millis(1),
        };

        let ok = write_with_retry_and_cleanup(7, "rule-a", &part, settings, || {
            attempts += 1;
            std::future::ready(Err(anyhow!("persistent io error")))
        })
        .await;

        assert!(!ok);
        assert_eq!(attempts, 3);
        assert!(fs::metadata(&part).await.is_err());
        fs::remove_dir_all(&clip_dir).await.unwrap();
    }
}
