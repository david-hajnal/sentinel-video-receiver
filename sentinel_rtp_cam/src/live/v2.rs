use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::core::h264_depacketize::H264Depacketizer;
use crate::core::h264_sync::H264SyncGate;
use crate::core::rtp::RtpPacket;
use crate::core::video::nal_type_from_annexb;
use crate::server_pipeline::StreamMsg;

const PAT_PID: u16 = 0x0000;
const PMT_PID: u16 = 0x1000;
const VIDEO_PID: u16 = 0x0100;
const TS_PACKET_SIZE: usize = 188;
const H264_STREAM_TYPE: u8 = 0x1B;
const H264_STREAM_ID: u8 = 0xE0;
const DEFAULT_V2_SESSION_RETENTION_SECS: u64 = 300;
const DEFAULT_V2_SESSION_KEEP_COUNT: usize = 3;

#[derive(Clone)]
pub struct LivePipelineV2Config {
    pub stream_id: u32,
    pub output_root: PathBuf,
    pub health_dir: PathBuf,
    pub stale_secs: u64,
    pub stats_log_secs: u64,
    pub segment_secs: u64,
    pub window_secs: u64,
}

#[derive(Debug, Clone)]
struct AccessUnit {
    rtp_timestamp: u32,
    nals: Vec<Vec<u8>>,
    has_idr: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StreamHealthState {
    Idle,
    Receiving,
    Stale,
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
    last_segment_write_at: Option<DateTime<Utc>>,
    last_stats_log_at: Option<Instant>,
    last_health_write_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct NoPublishEpisode {
    first_rtp_at: Option<Instant>,
    segment_seq_at_start: u64,
    logged: bool,
}

#[derive(Debug, Serialize)]
struct StreamHealthSnapshot {
    stream_id: u32,
    current_session_id: String,
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
    last_idr_at: Option<String>,
    last_segment_write_at: Option<String>,
    last_segment_write_ts: Option<String>,
    segment_seq: u64,
    discontinuity_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct CurrentSessionPointer {
    session_id: String,
    started_at: String,
    generation: u64,
    writer_state: String,
    updated_at: String,
    last_segment_seq: u64,
}

struct LiveSessionContext {
    session_id: String,
    output_root: PathBuf,
    generation: u64,
    started_at: DateTime<Utc>,
}

struct AtomicPublisher {
    final_path: PathBuf,
    temp_path: PathBuf,
    writer: BufWriter<File>,
}

impl AtomicPublisher {
    async fn create(final_path: PathBuf) -> Result<Self> {
        let temp_path = temp_path_for(&final_path);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = File::create(&temp_path)
            .await
            .with_context(|| format!("create temp file: {}", temp_path.display()))?;
        Ok(Self {
            final_path,
            temp_path,
            writer: BufWriter::with_capacity(256 * 1024, file),
        })
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes).await?;
        Ok(())
    }

    async fn publish(mut self) -> Result<PathBuf> {
        self.writer.flush().await?;
        drop(self.writer);
        fs::rename(&self.temp_path, &self.final_path)
            .await
            .with_context(|| {
                format!(
                    "rename {} -> {}",
                    self.temp_path.display(),
                    self.final_path.display()
                )
            })?;
        Ok(self.final_path)
    }

    async fn discard(self) -> Result<()> {
        drop(self.writer);
        if fs::metadata(&self.temp_path).await.is_ok() {
            fs::remove_file(&self.temp_path).await?;
        }
        Ok(())
    }

    async fn publish_bytes(final_path: &Path, bytes: &[u8]) -> Result<()> {
        let mut writer = Self::create(final_path.to_path_buf()).await?;
        writer.write_all(bytes).await?;
        writer.publish().await?;
        Ok(())
    }
}

struct MpegTsSegmentWriter {
    publisher: AtomicPublisher,
    pat_cc: u8,
    pmt_cc: u8,
    video_cc: u8,
}

impl MpegTsSegmentWriter {
    async fn create(path: PathBuf) -> Result<Self> {
        let mut writer = Self {
            publisher: AtomicPublisher::create(path).await?,
            pat_cc: 0,
            pmt_cc: 0,
            video_cc: 0,
        };
        writer
            .publisher
            .write_all(&build_pat_packet(&mut writer.pat_cc))
            .await?;
        writer
            .publisher
            .write_all(&build_pmt_packet(&mut writer.pmt_cc))
            .await?;
        Ok(writer)
    }

    async fn write_access_unit(&mut self, au: &AccessUnit) -> Result<()> {
        let mut payload = Vec::new();
        for nal in &au.nals {
            payload.extend_from_slice(nal);
        }
        let pes = build_pes_video(u64::from(au.rtp_timestamp), &payload);
        let ts = packetize_ts(VIDEO_PID, &pes, &mut self.video_cc, true);
        self.publisher.write_all(&ts).await?;
        Ok(())
    }

    async fn publish(self) -> Result<PathBuf> {
        self.publisher.publish().await
    }

    async fn discard(self) -> Result<()> {
        self.publisher.discard().await
    }
}

struct SegmentEntry {
    seq: u64,
    duration_secs: f64,
    filename: String,
    discontinuity: bool,
}

struct PlaylistState {
    target_duration_secs: u64,
    max_segments: usize,
    next_segment_seq: u64,
    entries: VecDeque<SegmentEntry>,
}

impl PlaylistState {
    fn new(segment_secs: u64, window_secs: u64) -> Self {
        let target_duration_secs = segment_secs.max(1);
        let max_segments = ((window_secs.max(target_duration_secs) + target_duration_secs - 1)
            / target_duration_secs) as usize
            + 1;
        Self {
            target_duration_secs,
            max_segments: max_segments.max(3),
            next_segment_seq: 1,
            entries: VecDeque::new(),
        }
    }

    fn next_segment_path(&mut self, output_root: &Path) -> (u64, PathBuf) {
        let seq = self.next_segment_seq;
        self.next_segment_seq += 1;
        (seq, output_root.join(format!("seg_{seq:06}.ts")))
    }

    async fn push_segment(
        &mut self,
        output_root: &Path,
        seq: u64,
        duration_secs: f64,
        discontinuity: bool,
    ) -> Result<()> {
        self.entries.push_back(SegmentEntry {
            seq,
            duration_secs: duration_secs.max(0.001),
            filename: format!("seg_{seq:06}.ts"),
            discontinuity,
        });

        while self.entries.len() > self.max_segments {
            if let Some(oldest) = self.entries.pop_front() {
                let old_path = output_root.join(oldest.filename);
                if fs::metadata(&old_path).await.is_ok() {
                    fs::remove_file(&old_path).await?;
                }
            }
        }

        self.publish(output_root).await
    }

    async fn publish(&self, output_root: &Path) -> Result<()> {
        let media_sequence = self.entries.front().map(|entry| entry.seq).unwrap_or(0);
        let mut playlist = String::new();
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:3\n");
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");
        playlist.push_str(&format!(
            "#EXT-X-TARGETDURATION:{}\n",
            self.target_duration_secs
        ));
        playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));

        for entry in &self.entries {
            if entry.discontinuity {
                playlist.push_str("#EXT-X-DISCONTINUITY\n");
            }
            playlist.push_str(&format!("#EXTINF:{:.3},\n", entry.duration_secs));
            playlist.push_str(&entry.filename);
            playlist.push('\n');
        }

        AtomicPublisher::publish_bytes(&output_root.join("index.m3u8"), playlist.as_bytes()).await
    }
}

fn session_id_now() -> String {
    let ts = Utc::now().format("%Y%m%d%H%M%S%3f");
    format!(
        "{ts}-{}",
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    )
}

fn load_session_gc_config() -> (u64, usize) {
    let retention_secs = std::env::var("LIVE_V2_SESSION_RETENTION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_V2_SESSION_RETENTION_SECS)
        .max(30);
    let keep_count = std::env::var("LIVE_V2_SESSION_KEEP_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_V2_SESSION_KEEP_COUNT)
        .max(1);
    (retention_secs, keep_count)
}

async fn read_current_generation(camera_root: &Path) -> u64 {
    let pointer_path = camera_root.join("current.json");
    let raw = match fs::read(&pointer_path).await {
        Ok(raw) => raw,
        Err(_) => return 1,
    };
    let pointer = match serde_json::from_slice::<CurrentSessionPointer>(&raw) {
        Ok(pointer) => pointer,
        Err(_) => return 1,
    };
    pointer.generation.saturating_add(1).max(1)
}

async fn write_current_session_pointer(
    camera_root: &Path,
    context: &LiveSessionContext,
    writer_state: &str,
    last_segment_seq: u64,
) -> Result<()> {
    let pointer = CurrentSessionPointer {
        session_id: context.session_id.clone(),
        started_at: context.started_at.to_rfc3339(),
        generation: context.generation,
        writer_state: writer_state.to_string(),
        updated_at: Utc::now().to_rfc3339(),
        last_segment_seq,
    };
    AtomicPublisher::publish_bytes(
        &camera_root.join("current.json"),
        &serde_json::to_vec_pretty(&pointer)?,
    )
    .await
}

async fn initialize_live_session(camera_root: &Path) -> Result<LiveSessionContext> {
    fs::create_dir_all(camera_root).await?;
    let generation = read_current_generation(camera_root).await;
    let session_id = session_id_now();
    let session_output_root = camera_root.join(&session_id);
    fs::create_dir_all(&session_output_root).await?;

    let context = LiveSessionContext {
        session_id: session_id.clone(),
        output_root: session_output_root,
        generation,
        started_at: Utc::now(),
    };
    write_current_session_pointer(camera_root, &context, "starting", 0).await?;

    let (retention_secs, keep_count) = load_session_gc_config();
    let camera_root = camera_root.to_path_buf();
    tokio::spawn(async move {
        if let Err(error) =
            cleanup_old_sessions(camera_root, session_id, retention_secs, keep_count).await
        {
            warn!(error = %error, "Failed to cleanup old v2 session directories");
        }
    });

    Ok(context)
}

async fn cleanup_old_sessions(
    camera_root: PathBuf,
    current_session_id: String,
    retention_secs: u64,
    keep_count: usize,
) -> Result<()> {
    let mut read_dir = fs::read_dir(&camera_root).await?;
    let mut sessions: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        if !metadata.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name == current_session_id {
            continue;
        }
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        sessions.push((path, modified));
    }
    sessions.sort_by(|a, b| b.1.cmp(&a.1));

    let now = std::time::SystemTime::now();
    let keep_old = keep_count.saturating_sub(1);
    for (idx, (path, modified)) in sessions.into_iter().enumerate() {
        if idx < keep_old {
            continue;
        }
        let age_secs = now.duration_since(modified).unwrap_or_default().as_secs();
        if age_secs <= retention_secs {
            continue;
        }
        if let Err(error) = fs::remove_dir_all(&path).await {
            warn!(
                session_path = %path.display(),
                error = %error,
                "Failed to remove old v2 session directory"
            );
        }
    }

    Ok(())
}

struct CurrentSegment {
    seq: u64,
    writer: MpegTsSegmentWriter,
    started_at: Instant,
    access_units_written: u64,
    discontinuity: bool,
}

impl CurrentSegment {
    fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }
}

pub async fn run_stream_v2(
    cfg: LivePipelineV2Config,
    mut rx: mpsc::Receiver<StreamMsg>,
) -> Result<()> {
    let session = initialize_live_session(&cfg.output_root).await?;
    info!(
        stream_id = cfg.stream_id,
        session_id = %session.session_id,
        generation = session.generation,
        output_root = %session.output_root.display(),
        "Live pipeline v2 session started"
    );

    let mut dep = H264Depacketizer::new();
    let mut sync_gate = H264SyncGate::new(true);
    let mut expected_seq: Option<u16> = None;
    let mut current_au_nals: Vec<Vec<u8>> = Vec::new();
    let mut current_au_rtp_ts: Option<u32> = None;

    let mut stats = StreamRuntimeStats::default();
    let mut playlist = PlaylistState::new(cfg.segment_secs, cfg.window_secs);
    let mut current_segment: Option<CurrentSegment> = None;
    let mut pending_discontinuity = false;
    let mut discontinuity_count = 0u64;
    let mut latest_segment_seq = 0u64;
    let mut no_publish_episode = NoPublishEpisode::default();

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                maybe_log_no_publish_watchdog(
                    cfg.stream_id,
                    &stats,
                    latest_segment_seq,
                    &mut no_publish_episode,
                );
                maybe_log_stream_stats(&cfg, &mut stats, latest_segment_seq, discontinuity_count);
                maybe_write_stream_health(
                    &cfg,
                    &mut stats,
                    latest_segment_seq,
                    discontinuity_count,
                    &session.session_id,
                ).await?;
            }
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    break;
                };
                match msg {
                    StreamMsg::Gap { .. } => {
                        stats.total_gaps = stats.total_gaps.saturating_add(1);
                        stats.last_gap_at = Some(Utc::now());
                        handle_discontinuity(
                            &session.output_root,
                            &mut playlist,
                            &mut current_segment,
                            &mut pending_discontinuity,
                            &mut discontinuity_count,
                            &mut latest_segment_seq,
                        )
                        .await?;
                        dep.reset();
                        sync_gate.reset();
                        expected_seq = None;
                        current_au_nals.clear();
                        current_au_rtp_ts = None;
                    }
                    StreamMsg::Motion { .. } => {
                        stats.total_motion_events = stats.total_motion_events.saturating_add(1);
                        stats.last_motion_at = Some(Utc::now());
                        warn!(
                            stream_id = cfg.stream_id,
                            "Received motion event while LIVE_PIPELINE_VERSION=v2; v2 is HLS-only and does not create .h264 motion clips"
                        );
                    }
                    StreamMsg::Rtp(bytes) => {
                        stats.total_rtp_packets = stats.total_rtp_packets.saturating_add(1);
                        stats.last_rtp_at = Some(Utc::now());
                        if no_publish_episode.first_rtp_at.is_none() {
                            no_publish_episode.first_rtp_at = Some(Instant::now());
                            no_publish_episode.segment_seq_at_start = latest_segment_seq;
                            no_publish_episode.logged = false;
                        }

                        let pkt = match RtpPacket::parse(&bytes) {
                            Ok(pkt) => pkt,
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
                                handle_discontinuity(
                                    &session.output_root,
                                    &mut playlist,
                                    &mut current_segment,
                                    &mut pending_discontinuity,
                                    &mut discontinuity_count,
                                    &mut latest_segment_seq,
                                )
                                .await?;
                                dep.reset();
                                sync_gate.reset();
                                expected_seq = None;
                                current_au_nals.clear();
                                current_au_rtp_ts = None;
                                continue;
                            }
                        };

                        if let Some(expected) = expected_seq {
                            if pkt.sequence_number != expected {
                                stats.total_gaps = stats.total_gaps.saturating_add(1);
                                stats.last_gap_at = Some(Utc::now());
                                handle_discontinuity(
                                    &session.output_root,
                                    &mut playlist,
                                    &mut current_segment,
                                    &mut pending_discontinuity,
                                    &mut discontinuity_count,
                                    &mut latest_segment_seq,
                                )
                                .await?;
                                dep.reset();
                                sync_gate.reset();
                                current_au_nals.clear();
                                current_au_rtp_ts = None;
                            }
                        }
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        current_au_rtp_ts.get_or_insert(pkt.timestamp);

                        let nals = match dep.push_rtp_payload(pkt.payload) {
                            Ok(nals) => nals,
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
                                handle_discontinuity(
                                    &session.output_root,
                                    &mut playlist,
                                    &mut current_segment,
                                    &mut pending_discontinuity,
                                    &mut discontinuity_count,
                                    &mut latest_segment_seq,
                                )
                                .await?;
                                dep.reset();
                                sync_gate.reset();
                                expected_seq = None;
                                current_au_nals.clear();
                                current_au_rtp_ts = None;
                                continue;
                            }
                        };

                        for nal in nals {
                            for gated in sync_gate.push_nal(nal) {
                                current_au_nals.push(gated);
                            }
                        }

                        if pkt.marker {
                            if current_au_nals.is_empty() {
                                current_au_rtp_ts = None;
                                continue;
                            }

                            let au = build_access_unit(
                                std::mem::take(&mut current_au_nals),
                                current_au_rtp_ts.take().unwrap_or(pkt.timestamp),
                            );
                            if au.has_idr {
                                stats.last_idr_at = Some(Utc::now());
                            }
                            stats.total_access_units = stats.total_access_units.saturating_add(1);
                            stats.last_access_unit_at = Some(Utc::now());

                            if should_rotate_segment(current_segment.as_ref(), &au, cfg.segment_secs)
                            {
                                finalize_segment(
                                    &session.output_root,
                                    &mut playlist,
                                    &mut current_segment,
                                    &mut latest_segment_seq,
                                )
                                .await?;
                                no_publish_episode = NoPublishEpisode::default();
                            }

                            if current_segment.is_none() {
                                if !au.has_idr {
                                    continue;
                                }
                                current_segment = Some(open_segment(
                                    &session.output_root,
                                    &mut playlist,
                                    pending_discontinuity,
                                )
                                .await?);
                                pending_discontinuity = false;
                            }

                            if let Some(segment) = &mut current_segment {
                                segment.writer.write_access_unit(&au).await?;
                                segment.access_units_written =
                                    segment.access_units_written.saturating_add(1);
                                stats.last_segment_write_at = Some(Utc::now());
                            }

                            if pending_discontinuity && au.has_idr && latest_segment_seq > 0 {
                                pending_discontinuity = false;
                            }
                        }
                    }
                }
            }
        }
    }

    finalize_segment(
        &session.output_root,
        &mut playlist,
        &mut current_segment,
        &mut latest_segment_seq,
    )
    .await?;
    write_current_session_pointer(&cfg.output_root, &session, "stopped", latest_segment_seq)
        .await?;
    write_stream_health(
        &cfg,
        &stats,
        latest_segment_seq,
        discontinuity_count,
        &session.session_id,
    )
    .await?;
    info!(
        stream_id = cfg.stream_id,
        session_id = %session.session_id,
        latest_segment_seq,
        "Live pipeline v2 session stopped"
    );
    Ok(())
}

fn build_access_unit(nals: Vec<Vec<u8>>, rtp_timestamp: u32) -> AccessUnit {
    let has_idr = nals
        .iter()
        .filter_map(|nal| nal_type_from_annexb(nal))
        .any(|nal_type| nal_type == 5);

    AccessUnit {
        rtp_timestamp,
        nals,
        has_idr,
    }
}

async fn open_segment(
    session_output_root: &Path,
    playlist: &mut PlaylistState,
    discontinuity: bool,
) -> Result<CurrentSegment> {
    let (seq, path) = playlist.next_segment_path(session_output_root);
    Ok(CurrentSegment {
        seq,
        writer: MpegTsSegmentWriter::create(path).await?,
        started_at: Instant::now(),
        access_units_written: 0,
        discontinuity,
    })
}

fn should_rotate_segment(
    current_segment: Option<&CurrentSegment>,
    next_au: &AccessUnit,
    target_segment_secs: u64,
) -> bool {
    let Some(current_segment) = current_segment else {
        return false;
    };
    current_segment.access_units_written > 0
        && current_segment.elapsed_secs() >= target_segment_secs.max(1) as f64
        && next_au.has_idr
}

async fn finalize_segment(
    session_output_root: &Path,
    playlist: &mut PlaylistState,
    current_segment: &mut Option<CurrentSegment>,
    latest_segment_seq: &mut u64,
) -> Result<()> {
    let Some(segment) = current_segment.take() else {
        return Ok(());
    };

    if segment.access_units_written == 0 {
        segment.writer.discard().await?;
        return Ok(());
    }

    let duration_secs = segment.elapsed_secs();
    let seq = segment.seq;
    let discontinuity = segment.discontinuity;
    segment.writer.publish().await?;
    playlist
        .push_segment(session_output_root, seq, duration_secs, discontinuity)
        .await?;
    *latest_segment_seq = seq;
    Ok(())
}

async fn handle_discontinuity(
    session_output_root: &Path,
    playlist: &mut PlaylistState,
    current_segment: &mut Option<CurrentSegment>,
    pending_discontinuity: &mut bool,
    discontinuity_count: &mut u64,
    latest_segment_seq: &mut u64,
) -> Result<()> {
    let saw_published_output = *latest_segment_seq > 0 || current_segment.is_some();
    finalize_segment(
        session_output_root,
        playlist,
        current_segment,
        latest_segment_seq,
    )
    .await?;
    if saw_published_output {
        *pending_discontinuity = true;
        *discontinuity_count = discontinuity_count.saturating_add(1);
    }
    Ok(())
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

fn maybe_log_no_publish_watchdog(
    stream_id: u32,
    stats: &StreamRuntimeStats,
    segment_seq: u64,
    episode: &mut NoPublishEpisode,
) {
    if episode.first_rtp_at.is_none() {
        return;
    }
    if segment_seq > episode.segment_seq_at_start {
        *episode = NoPublishEpisode::default();
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
        segment_seq,
        "Startup watchdog: RTP is arriving but no HLS segment has been published"
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
    let elapsed_secs = now.signed_duration_since(last_rtp_at).num_seconds().max(0) as u64;
    if elapsed_secs <= stale_secs.max(1) {
        StreamHealthState::Receiving
    } else {
        StreamHealthState::Stale
    }
}

fn maybe_log_stream_stats(
    cfg: &LivePipelineV2Config,
    stats: &mut StreamRuntimeStats,
    segment_seq: u64,
    discontinuity_count: u64,
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
        segment_seq,
        discontinuity_count,
        total_rtp_packets = stats.total_rtp_packets,
        total_access_units = stats.total_access_units,
        total_gaps = stats.total_gaps,
        total_motion_events = stats.total_motion_events,
        total_parse_errors = stats.total_parse_errors,
        "Live pipeline v2 stats"
    );
    stats.last_stats_log_at = Some(now);
}

async fn maybe_write_stream_health(
    cfg: &LivePipelineV2Config,
    stats: &mut StreamRuntimeStats,
    segment_seq: u64,
    discontinuity_count: u64,
    current_session_id: &str,
) -> Result<()> {
    let now = Instant::now();
    if stats
        .last_health_write_at
        .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
    {
        return Ok(());
    }
    write_stream_health(
        cfg,
        stats,
        segment_seq,
        discontinuity_count,
        current_session_id,
    )
    .await?;
    stats.last_health_write_at = Some(now);
    Ok(())
}

async fn write_stream_health(
    cfg: &LivePipelineV2Config,
    stats: &StreamRuntimeStats,
    segment_seq: u64,
    discontinuity_count: u64,
    current_session_id: &str,
) -> Result<()> {
    let now = Utc::now();
    let snapshot = StreamHealthSnapshot {
        stream_id: cfg.stream_id,
        current_session_id: current_session_id.to_string(),
        state: derive_stream_health_state(stats.last_rtp_at, cfg.stale_secs, now),
        updated_at: now.to_rfc3339(),
        stale_after_secs: cfg.stale_secs,
        ring_secs: 0,
        clip_active: false,
        clip_pending: false,
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
        last_idr_at: stats.last_idr_at.map(|ts| ts.to_rfc3339()),
        last_segment_write_at: stats.last_segment_write_at.map(|ts| ts.to_rfc3339()),
        last_segment_write_ts: stats.last_segment_write_at.map(|ts| ts.to_rfc3339()),
        segment_seq,
        discontinuity_count,
    };

    AtomicPublisher::publish_bytes(
        &cfg.health_dir
            .join(format!("stream_{}.health.json", cfg.stream_id)),
        &serde_json::to_vec_pretty(&snapshot)?,
    )
    .await
}

fn temp_path_for(final_path: &Path) -> PathBuf {
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("artifact");
    final_path.with_file_name(format!("{file_name}.tmp"))
}

fn build_pat_packet(continuity_counter: &mut u8) -> Vec<u8> {
    let mut section = Vec::new();
    section.push(0x00);
    section.push(0xB0);
    section.push(0x0D);
    section.extend_from_slice(&1u16.to_be_bytes());
    section.push(0xC1);
    section.push(0x00);
    section.push(0x00);
    section.extend_from_slice(&1u16.to_be_bytes());
    section.push(0xE0 | ((PMT_PID >> 8) as u8 & 0x1F));
    section.push((PMT_PID & 0xFF) as u8);
    let crc = mpeg_crc32(&section);
    section.extend_from_slice(&crc.to_be_bytes());

    let mut payload = vec![0x00];
    payload.extend_from_slice(&section);
    packetize_ts(PAT_PID, &payload, continuity_counter, true)
}

fn build_pmt_packet(continuity_counter: &mut u8) -> Vec<u8> {
    let mut section = Vec::new();
    section.push(0x02);
    section.push(0xB0);
    section.push(0x12);
    section.extend_from_slice(&1u16.to_be_bytes());
    section.push(0xC1);
    section.push(0x00);
    section.push(0x00);
    section.push(0xE0 | ((VIDEO_PID >> 8) as u8 & 0x1F));
    section.push((VIDEO_PID & 0xFF) as u8);
    section.push(0xF0);
    section.push(0x00);
    section.push(H264_STREAM_TYPE);
    section.push(0xE0 | ((VIDEO_PID >> 8) as u8 & 0x1F));
    section.push((VIDEO_PID & 0xFF) as u8);
    section.push(0xF0);
    section.push(0x00);
    let crc = mpeg_crc32(&section);
    section.extend_from_slice(&crc.to_be_bytes());

    let mut payload = vec![0x00];
    payload.extend_from_slice(&section);
    packetize_ts(PMT_PID, &payload, continuity_counter, true)
}

fn build_pes_video(pts_90k: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(14 + payload.len());
    out.extend_from_slice(&[0x00, 0x00, 0x01, H264_STREAM_ID]);
    out.extend_from_slice(&0u16.to_be_bytes());
    out.push(0x80);
    out.push(0x80);
    out.push(0x05);
    out.extend_from_slice(&encode_pts(pts_90k));
    out.extend_from_slice(payload);
    out
}

fn encode_pts(pts_90k: u64) -> [u8; 5] {
    let pts = pts_90k & ((1u64 << 33) - 1);
    [
        (0x2 << 4) | ((((pts >> 30) & 0x07) as u8) << 1) | 1,
        ((pts >> 22) & 0xFF) as u8,
        ((((pts >> 15) & 0x7F) as u8) << 1) | 1,
        ((pts >> 7) & 0xFF) as u8,
        (((pts & 0x7F) as u8) << 1) | 1,
    ]
}

fn packetize_ts(
    pid: u16,
    payload: &[u8],
    continuity_counter: &mut u8,
    payload_unit_start: bool,
) -> Vec<u8> {
    let mut packets = Vec::new();
    let mut offset = 0usize;
    let mut first = true;

    while offset < payload.len() {
        let remaining = payload.len() - offset;
        let take = remaining.min(184);
        let use_adaptation = take < 184;
        let adaptation_control = if use_adaptation { 0b11 } else { 0b01 };

        let mut packet = vec![0u8; TS_PACKET_SIZE];
        packet[0] = 0x47;
        packet[1] = (((first && payload_unit_start) as u8) << 6) | ((pid >> 8) as u8 & 0x1F);
        packet[2] = (pid & 0xFF) as u8;
        packet[3] = (adaptation_control << 4) | (*continuity_counter & 0x0F);
        *continuity_counter = continuity_counter.wrapping_add(1) & 0x0F;

        let mut write_offset = 4usize;
        if use_adaptation {
            let adaptation_field_length = 183usize.saturating_sub(take);
            packet[write_offset] = adaptation_field_length as u8;
            write_offset += 1;
            if adaptation_field_length > 0 {
                packet[write_offset] = 0x00;
                write_offset += 1;
                for idx in 1..adaptation_field_length {
                    packet[write_offset + idx - 1] = 0xFF;
                }
                write_offset += adaptation_field_length - 1;
            }
        }

        packet[write_offset..write_offset + take].copy_from_slice(&payload[offset..offset + take]);
        packets.extend_from_slice(&packet);
        offset += take;
        first = false;
    }

    packets
}

fn mpeg_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in bytes {
        crc ^= (byte as u32) << 24;
        for _ in 0..8 {
            if (crc & 0x8000_0000) != 0 {
                crc = (crc << 1) ^ 0x04C1_1DB7;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::{
        build_access_unit, build_pat_packet, build_pmt_packet, cleanup_old_sessions,
        derive_rtp_payload_nal_type, derive_stream_health_state, encode_pts, finalize_segment,
        handle_discontinuity, initialize_live_session, mpeg_crc32, open_segment, packetize_ts,
        read_current_generation, run_stream_v2, should_rotate_segment, temp_path_for,
        write_current_session_pointer, AtomicPublisher, LivePipelineV2Config, NoPublishEpisode,
        PlaylistState, StreamHealthState, StreamRuntimeStats,
    };
    use crate::server_pipeline::StreamMsg;
    use chrono::{Duration as ChronoDuration, Utc};
    use filetime::{set_file_mtime, FileTime};
    use serde_json::Value;
    use std::path::PathBuf;
    use std::time::{Duration, Instant, SystemTime};
    use tokio::fs;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sentinel_live_v2_{name}_{}", Ulid::new()))
    }

    fn config(dir: &PathBuf) -> LivePipelineV2Config {
        LivePipelineV2Config {
            stream_id: 7,
            output_root: dir.join("hls_v2").join("7"),
            health_dir: dir.clone(),
            stale_secs: 15,
            stats_log_secs: 60,
            segment_secs: 1,
            window_secs: 4,
        }
    }

    fn rtp_single_nal(sequence_number: u16, timestamp: u32, marker: bool, nal: &[u8]) -> Vec<u8> {
        let mut packet = Vec::with_capacity(12 + nal.len());
        packet.push(0x80);
        packet.push(if marker { 0xE0 } else { 0x60 });
        packet.extend_from_slice(&sequence_number.to_be_bytes());
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&1u32.to_be_bytes());
        packet.extend_from_slice(nal);
        packet
    }

    async fn send_sync_idr(tx: &mpsc::Sender<StreamMsg>, seq_start: u16, ts_start: u32) {
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            seq_start,
            ts_start,
            true,
            &[0x67, 0x64, 0x00, 0x1F],
        )))
        .await
        .unwrap();
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            seq_start.wrapping_add(1),
            ts_start.wrapping_add(3_600),
            true,
            &[0x68, 0xEE, 0x3C, 0x80],
        )))
        .await
        .unwrap();
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            seq_start.wrapping_add(2),
            ts_start.wrapping_add(7_200),
            true,
            &[0x65, 0x88, 0x99, 0xAA],
        )))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn playlist_state_calculates_max_segments_and_minimum_floor() {
        let tiny = PlaylistState::new(1, 1);
        assert_eq!(tiny.target_duration_secs, 1);
        assert_eq!(tiny.max_segments, 3);

        let wider = PlaylistState::new(2, 9);
        assert_eq!(wider.target_duration_secs, 2);
        assert_eq!(wider.max_segments, 6);
    }

    #[tokio::test]
    async fn playlist_publish_includes_required_tags_and_discontinuity_marker() {
        let dir = test_dir("playlist_publish");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(2, 4);
        fs::write(dir.join("seg_000001.ts"), b"segment-1")
            .await
            .unwrap();
        fs::write(dir.join("seg_000002.ts"), b"segment-2")
            .await
            .unwrap();
        playlist.push_segment(&dir, 1, 1.2, false).await.unwrap();
        playlist.push_segment(&dir, 2, 1.8, true).await.unwrap();

        let manifest = fs::read_to_string(dir.join("index.m3u8")).await.unwrap();
        assert!(manifest.contains("#EXTM3U"));
        assert!(manifest.contains("#EXT-X-VERSION:3"));
        assert!(manifest.contains("#EXT-X-INDEPENDENT-SEGMENTS"));
        assert!(manifest.contains("#EXT-X-TARGETDURATION:2"));
        assert!(manifest.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert!(manifest.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.800,\nseg_000002.ts"));

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn playlist_push_segment_evicts_oldest_files_and_updates_media_sequence() {
        let dir = test_dir("playlist_eviction");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(1, 1);
        assert_eq!(playlist.max_segments, 3);

        for seq in 1..=5 {
            fs::write(
                dir.join(format!("seg_{seq:06}.ts")),
                format!("segment-{seq}"),
            )
            .await
            .unwrap();
            playlist.push_segment(&dir, seq, 1.0, false).await.unwrap();
        }

        let manifest = fs::read_to_string(dir.join("index.m3u8")).await.unwrap();
        assert!(manifest.contains("#EXT-X-MEDIA-SEQUENCE:3"));
        assert!(!manifest.contains("seg_000001.ts"));
        assert!(!manifest.contains("seg_000002.ts"));
        assert!(manifest.contains("seg_000003.ts"));
        assert!(manifest.contains("seg_000004.ts"));
        assert!(manifest.contains("seg_000005.ts"));

        assert!(fs::metadata(dir.join("seg_000001.ts")).await.is_err());
        assert!(fs::metadata(dir.join("seg_000002.ts")).await.is_err());
        assert!(fs::metadata(dir.join("seg_000003.ts")).await.is_ok());
        assert!(fs::metadata(dir.join("seg_000004.ts")).await.is_ok());
        assert!(fs::metadata(dir.join("seg_000005.ts")).await.is_ok());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn should_rotate_segment_requires_data_elapsed_target_and_next_idr() {
        let dir = test_dir("segment_rotate");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(1, 4);
        let mut segment = open_segment(&dir, &mut playlist, false).await.unwrap();
        let idr_au = build_access_unit(vec![vec![0x00, 0x00, 0x00, 0x01, 0x65, 0x88]], 90_000);
        let p_au = build_access_unit(vec![vec![0x00, 0x00, 0x00, 0x01, 0x41, 0x99]], 93_000);

        assert!(!should_rotate_segment(Some(&segment), &idr_au, 1));
        segment.access_units_written = 1;
        segment.started_at = Instant::now() - Duration::from_millis(200);
        assert!(!should_rotate_segment(Some(&segment), &idr_au, 1));
        segment.started_at = Instant::now() - Duration::from_secs(2);
        assert!(!should_rotate_segment(Some(&segment), &p_au, 1));
        assert!(should_rotate_segment(Some(&segment), &idr_au, 1));
        assert!(!should_rotate_segment(None, &idr_au, 1));

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn handle_discontinuity_without_published_output_keeps_pending_false() {
        let dir = test_dir("disco_no_output");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(1, 2);
        let mut current_segment = None;
        let mut pending_discontinuity = false;
        let mut discontinuity_count = 0u64;
        let mut latest_segment_seq = 0u64;

        handle_discontinuity(
            &dir,
            &mut playlist,
            &mut current_segment,
            &mut pending_discontinuity,
            &mut discontinuity_count,
            &mut latest_segment_seq,
        )
        .await
        .unwrap();

        assert!(!pending_discontinuity);
        assert_eq!(discontinuity_count, 0);
        assert_eq!(latest_segment_seq, 0);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn handle_discontinuity_after_published_output_sets_pending_and_increments_count() {
        let dir = test_dir("disco_has_output");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(1, 2);
        let mut current_segment = None;
        let mut pending_discontinuity = false;
        let mut discontinuity_count = 0u64;
        let mut latest_segment_seq = 3u64;

        handle_discontinuity(
            &dir,
            &mut playlist,
            &mut current_segment,
            &mut pending_discontinuity,
            &mut discontinuity_count,
            &mut latest_segment_seq,
        )
        .await
        .unwrap();

        assert!(pending_discontinuity);
        assert_eq!(discontinuity_count, 1);
        assert_eq!(latest_segment_seq, 3);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn finalize_segment_discards_empty_segments_without_writing_playlist_or_ts() {
        let dir = test_dir("finalize_empty");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(1, 2);
        let mut current_segment = Some(open_segment(&dir, &mut playlist, false).await.unwrap());
        let mut latest_segment_seq = 0u64;

        finalize_segment(
            &dir,
            &mut playlist,
            &mut current_segment,
            &mut latest_segment_seq,
        )
        .await
        .unwrap();

        assert!(current_segment.is_none());
        assert_eq!(latest_segment_seq, 0);
        assert!(fs::metadata(dir.join("index.m3u8")).await.is_err());
        assert!(fs::metadata(dir.join("seg_000001.ts")).await.is_err());
        assert!(fs::metadata(dir.join("seg_000001.ts.tmp")).await.is_err());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn finalize_segment_publishes_non_empty_segment_and_updates_playlist_state() {
        let dir = test_dir("finalize_non_empty");
        fs::create_dir_all(&dir).await.unwrap();
        let mut playlist = PlaylistState::new(1, 2);
        let mut current_segment = Some(open_segment(&dir, &mut playlist, false).await.unwrap());
        let mut latest_segment_seq = 0u64;
        let au = build_access_unit(vec![vec![0x00, 0x00, 0x00, 0x01, 0x65, 0x44]], 90_000);

        {
            let segment = current_segment.as_mut().expect("segment");
            segment.writer.write_access_unit(&au).await.unwrap();
            segment.access_units_written = 1;
            segment.started_at = Instant::now() - Duration::from_secs(2);
        }

        finalize_segment(
            &dir,
            &mut playlist,
            &mut current_segment,
            &mut latest_segment_seq,
        )
        .await
        .unwrap();

        assert!(current_segment.is_none());
        assert_eq!(latest_segment_seq, 1);
        assert!(fs::metadata(dir.join("seg_000001.ts")).await.is_ok());
        let seg_size = fs::metadata(dir.join("seg_000001.ts")).await.unwrap().len();
        assert!(seg_size > 0);
        let manifest = fs::read_to_string(dir.join("index.m3u8")).await.unwrap();
        assert!(manifest.contains("seg_000001.ts"));
        assert!(manifest.contains("#EXT-X-MEDIA-SEQUENCE:1"));

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn initialize_live_session_creates_session_directory_and_starting_pointer() {
        let dir = test_dir("init_session");
        fs::create_dir_all(&dir).await.unwrap();

        let session = initialize_live_session(&dir).await.unwrap();
        assert!(fs::metadata(&session.output_root).await.is_ok());

        let pointer_raw = fs::read_to_string(dir.join("current.json")).await.unwrap();
        let pointer: Value = serde_json::from_str(&pointer_raw).unwrap();
        assert_eq!(
            pointer.get("session_id").and_then(Value::as_str),
            Some(session.session_id.as_str())
        );
        assert_eq!(
            pointer.get("writer_state").and_then(Value::as_str),
            Some("starting")
        );
        assert_eq!(
            pointer.get("generation").and_then(Value::as_u64),
            Some(session.generation)
        );

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn write_current_session_pointer_updates_state_and_last_segment_seq() {
        let dir = test_dir("pointer_update");
        fs::create_dir_all(&dir).await.unwrap();
        let session = initialize_live_session(&dir).await.unwrap();

        write_current_session_pointer(&dir, &session, "stopped", 42)
            .await
            .unwrap();
        let pointer_raw = fs::read_to_string(dir.join("current.json")).await.unwrap();
        let pointer: Value = serde_json::from_str(&pointer_raw).unwrap();
        assert_eq!(
            pointer.get("writer_state").and_then(Value::as_str),
            Some("stopped")
        );
        assert_eq!(
            pointer.get("last_segment_seq").and_then(Value::as_u64),
            Some(42)
        );

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn read_current_generation_handles_missing_malformed_and_valid_pointer() {
        let dir = test_dir("read_generation");
        fs::create_dir_all(&dir).await.unwrap();

        assert_eq!(read_current_generation(&dir).await, 1);

        fs::write(dir.join("current.json"), "{not-json")
            .await
            .unwrap();
        assert_eq!(read_current_generation(&dir).await, 1);

        let valid_pointer = serde_json::json!({
            "session_id": "sess_a",
            "started_at": "2026-04-18T12:00:00Z",
            "generation": 8,
            "writer_state": "starting",
            "updated_at": "2026-04-18T12:00:00Z",
            "last_segment_seq": 0
        });
        fs::write(
            dir.join("current.json"),
            serde_json::to_vec_pretty(&valid_pointer).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(read_current_generation(&dir).await, 9);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_old_sessions_keeps_current_and_newest_old_sessions_then_removes_expired_older()
    {
        let dir = test_dir("session_gc");
        fs::create_dir_all(&dir).await.unwrap();
        let current = dir.join("sess_current");
        let keep_newest = dir.join("sess_keep_newest");
        let keep_second = dir.join("sess_keep_second");
        let remove_old = dir.join("sess_remove_old");
        let remove_older = dir.join("sess_remove_older");
        for session_dir in [
            &current,
            &keep_newest,
            &keep_second,
            &remove_old,
            &remove_older,
        ] {
            fs::create_dir_all(session_dir).await.unwrap();
        }

        let now = SystemTime::now();
        set_file_mtime(
            &keep_newest,
            FileTime::from_system_time(now - Duration::from_secs(400)),
        )
        .unwrap();
        set_file_mtime(
            &keep_second,
            FileTime::from_system_time(now - Duration::from_secs(500)),
        )
        .unwrap();
        set_file_mtime(
            &remove_old,
            FileTime::from_system_time(now - Duration::from_secs(600)),
        )
        .unwrap();
        set_file_mtime(
            &remove_older,
            FileTime::from_system_time(now - Duration::from_secs(700)),
        )
        .unwrap();

        cleanup_old_sessions(dir.clone(), "sess_current".to_string(), 300, 3)
            .await
            .unwrap();

        assert!(fs::metadata(&current).await.is_ok());
        assert!(fs::metadata(&keep_newest).await.is_ok());
        assert!(fs::metadata(&keep_second).await.is_ok());
        assert!(fs::metadata(&remove_old).await.is_err());
        assert!(fs::metadata(&remove_older).await.is_err());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[test]
    fn temp_path_for_appends_tmp_suffix_to_filename() {
        let path = PathBuf::from("/tmp/index.m3u8");
        let temp = temp_path_for(&path);
        assert_eq!(temp, PathBuf::from("/tmp/index.m3u8.tmp"));
    }

    #[test]
    fn encode_pts_sets_expected_marker_bits() {
        let encoded = encode_pts(0x1ABCDE);
        assert_eq!(encoded.len(), 5);
        assert_eq!(encoded[0] & 0xF0, 0x20);
        assert_eq!(encoded[0] & 0x01, 1);
        assert_eq!(encoded[2] & 0x01, 1);
        assert_eq!(encoded[4] & 0x01, 1);
    }

    #[test]
    fn packetize_ts_uses_adaptation_padding_and_wraps_continuity_counter() {
        let payload = vec![0xAB; 50];
        let mut cc = 15u8;
        let packets = packetize_ts(0x0100, &payload, &mut cc, true);
        assert_eq!(packets.len() % 188, 0);
        assert_eq!(packets.len(), 188);
        assert_eq!(packets[0], 0x47);
        assert_eq!(packets[3] & 0x0F, 15);
        assert_eq!((packets[3] >> 4) & 0x03, 0b11);
        assert_eq!(cc, 0);

        let adaptation_len = packets[4] as usize;
        assert!(adaptation_len > 0);
        assert_eq!(packets[5], 0x00);
        assert!(packets[6..(5 + adaptation_len)]
            .iter()
            .all(|byte| *byte == 0xFF));
        let payload_offset = 5 + adaptation_len;
        assert_eq!(
            &packets[payload_offset..payload_offset + payload.len()],
            payload.as_slice()
        );

        let mut cc_multi = 14u8;
        let multi_packets = packetize_ts(0x0100, &[0xCD; 400], &mut cc_multi, false);
        assert_eq!(multi_packets.len(), 188 * 3);
        assert_eq!(multi_packets[3] & 0x0F, 14);
        assert_eq!(multi_packets[188 + 3] & 0x0F, 15);
        assert_eq!(multi_packets[(188 * 2) + 3] & 0x0F, 0);
        assert_eq!(cc_multi, 1);
    }

    #[test]
    fn build_pat_and_pmt_packets_use_expected_sync_and_pid_values() {
        let mut cc_pat = 0u8;
        let pat = build_pat_packet(&mut cc_pat);
        assert_eq!(pat.len() % 188, 0);
        assert_eq!(pat[0], 0x47);
        let pat_pid = (((pat[1] & 0x1F) as u16) << 8) | pat[2] as u16;
        assert_eq!(pat_pid, super::PAT_PID);
        assert_eq!(cc_pat, 1);

        let mut cc_pmt = 3u8;
        let pmt = build_pmt_packet(&mut cc_pmt);
        assert_eq!(pmt.len() % 188, 0);
        assert_eq!(pmt[0], 0x47);
        let pmt_pid = (((pmt[1] & 0x1F) as u16) << 8) | pmt[2] as u16;
        assert_eq!(pmt_pid, super::PMT_PID);
        assert_eq!(cc_pmt, 4);
    }

    #[test]
    fn mpeg_crc32_is_stable_for_same_bytes_and_changes_with_input() {
        let bytes = [0x00, 0xB0, 0x0D, 0x00, 0x01, 0xC1];
        let crc_a = mpeg_crc32(&bytes);
        let crc_b = mpeg_crc32(&bytes);
        let mut changed = bytes;
        changed[1] ^= 0x01;
        let crc_changed = mpeg_crc32(&changed);

        assert_eq!(crc_a, crc_b);
        assert_ne!(crc_a, crc_changed);
    }

    #[tokio::test]
    async fn run_stream_v2_gap_driven_segments_include_discontinuity_and_evict_old_segments() {
        let dir = test_dir("gap_discontinuity_eviction");
        fs::create_dir_all(&dir).await.unwrap();
        let mut cfg = config(&dir);
        cfg.segment_secs = 1;
        cfg.window_secs = 1;
        let (tx, rx) = mpsc::channel(256);

        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        let mut seq = 1u16;
        let mut ts = 90_000u32;
        for idx in 0..5 {
            send_sync_idr(&tx, seq, ts).await;
            seq = seq.wrapping_add(3);
            ts = ts.wrapping_add(9_000);
            if idx < 4 {
                tx.send(StreamMsg::Gap {
                    last: seq.wrapping_sub(1),
                    new: seq.wrapping_add(10),
                })
                .await
                .unwrap();
            }
        }
        drop(tx);
        handle.await.unwrap().unwrap();

        let pointer_raw = fs::read_to_string(cfg.output_root.join("current.json"))
            .await
            .expect("current session pointer");
        let pointer: Value = serde_json::from_str(&pointer_raw).expect("valid pointer");
        let session_id = pointer
            .get("session_id")
            .and_then(Value::as_str)
            .expect("session id");
        let session_root = cfg.output_root.join(session_id);
        let manifest = fs::read_to_string(session_root.join("index.m3u8"))
            .await
            .unwrap();

        assert!(manifest.contains("#EXT-X-DISCONTINUITY"));
        assert!(manifest.contains("#EXT-X-MEDIA-SEQUENCE:3"));
        assert!(!manifest.contains("seg_000001.ts"));
        assert!(!manifest.contains("seg_000002.ts"));
        assert!(manifest.contains("seg_000003.ts"));
        assert!(manifest.contains("seg_000004.ts"));
        assert!(manifest.contains("seg_000005.ts"));
        assert!(fs::metadata(session_root.join("seg_000001.ts"))
            .await
            .is_err());
        assert!(fs::metadata(session_root.join("seg_000002.ts"))
            .await
            .is_err());
        for seq in 3..=5 {
            let path = session_root.join(format!("seg_{seq:06}.ts"));
            let size = fs::metadata(path).await.unwrap().len();
            assert!(size > 0);
        }

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[test]
    fn derive_stream_health_state_reports_idle_receiving_and_stale() {
        let now = Utc::now();
        assert_eq!(
            derive_stream_health_state(None, 10, now),
            StreamHealthState::Idle
        );
        assert_eq!(
            derive_stream_health_state(Some(now - ChronoDuration::seconds(5)), 10, now),
            StreamHealthState::Receiving
        );
        assert_eq!(
            derive_stream_health_state(Some(now - ChronoDuration::seconds(11)), 10, now),
            StreamHealthState::Stale
        );
    }

    #[test]
    fn derive_rtp_payload_nal_type_supports_single_nal_fu_a_and_invalid_inputs() {
        assert_eq!(derive_rtp_payload_nal_type(&[0x65, 0xAA]), Some(5));
        assert_eq!(derive_rtp_payload_nal_type(&[0x7C, 0x85, 0xAA]), Some(5));
        assert_eq!(derive_rtp_payload_nal_type(&[0x7C]), None);
        assert_eq!(derive_rtp_payload_nal_type(&[0x00]), None);
        assert_eq!(derive_rtp_payload_nal_type(&[]), None);
    }

    #[tokio::test]
    async fn atomic_publish_keeps_existing_file_visible_until_rename() {
        let dir = test_dir("atomic");
        fs::create_dir_all(&dir).await.unwrap();
        let final_path = dir.join("index.m3u8");
        fs::write(&final_path, b"old").await.unwrap();

        let mut publisher = AtomicPublisher::create(final_path.clone()).await.unwrap();
        publisher.write_all(b"new").await.unwrap();

        assert_eq!(fs::read(&final_path).await.unwrap(), b"old");
        assert!(fs::metadata(temp_path_for(&final_path)).await.is_ok());

        publisher.publish().await.unwrap();

        assert_eq!(fs::read(&final_path).await.unwrap(), b"new");
        assert!(fs::metadata(temp_path_for(&final_path)).await.is_err());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn v2_does_not_emit_segments_until_sync() {
        let dir = test_dir("nosync");
        fs::create_dir_all(&dir).await.unwrap();
        let cfg = config(&dir);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            1,
            90_000,
            true,
            &[0x41, 0x01],
        )))
        .await
        .unwrap();
        drop(tx);

        handle.await.unwrap().unwrap();

        let pointer_raw = fs::read_to_string(cfg.output_root.join("current.json"))
            .await
            .expect("current pointer");
        let pointer: Value = serde_json::from_str(&pointer_raw).expect("valid pointer");
        let session_id = pointer
            .get("session_id")
            .and_then(Value::as_str)
            .expect("session id in pointer");
        let session_root = cfg.output_root.join(session_id);

        assert!(fs::metadata(session_root.join("index.m3u8")).await.is_err());
        assert!(fs::metadata(session_root.join("seg_000001.ts"))
            .await
            .is_err());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn v2_publishes_segment_after_sps_pps_and_idr_sync() {
        let dir = test_dir("sync");
        fs::create_dir_all(&dir).await.unwrap();
        let cfg = config(&dir);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            1,
            90_000,
            true,
            &[0x67, 0x64, 0x00],
        )))
        .await
        .unwrap();
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            2,
            93_600,
            true,
            &[0x68, 0xEE, 0x3C],
        )))
        .await
        .unwrap();
        tx.send(StreamMsg::Rtp(rtp_single_nal(
            3,
            97_200,
            true,
            &[0x65, 0x88, 0x99],
        )))
        .await
        .unwrap();
        drop(tx);

        handle.await.unwrap().unwrap();

        let pointer_raw = fs::read_to_string(cfg.output_root.join("current.json"))
            .await
            .expect("current pointer");
        let pointer: Value = serde_json::from_str(&pointer_raw).expect("valid pointer");
        let session_id = pointer
            .get("session_id")
            .and_then(Value::as_str)
            .expect("session id in pointer");
        let session_root = cfg.output_root.join(session_id);

        let playlist = fs::read_to_string(session_root.join("index.m3u8"))
            .await
            .unwrap();
        assert!(playlist.contains("#EXTM3U"));
        assert!(playlist.contains("seg_000001.ts"));
        assert!(fs::metadata(session_root.join("seg_000001.ts"))
            .await
            .is_ok());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn v2_motion_updates_health_counters_without_creating_h264_clips() {
        let dir = test_dir("motion_only");
        fs::create_dir_all(&dir).await.unwrap();
        let cfg = config(&dir);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        tx.send(StreamMsg::Motion {
            rule: "zone-a".to_string(),
            active: true,
            ts: "2026-04-18T12:00:00Z".to_string(),
        })
        .await
        .unwrap();
        drop(tx);

        handle.await.unwrap().unwrap();

        let health = fs::read_to_string(dir.join("stream_7.health.json"))
            .await
            .unwrap();
        assert!(health.contains("\"total_motion_events\": 1"));
        assert!(fs::metadata(cfg.output_root.join("index.m3u8"))
            .await
            .is_err());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn v2_startup_does_not_delete_existing_root_playlist_or_segments() {
        let dir = test_dir("startup_no_cleanup");
        fs::create_dir_all(&dir).await.unwrap();
        let cfg = config(&dir);
        fs::create_dir_all(&cfg.output_root).await.unwrap();
        fs::write(cfg.output_root.join("index.m3u8"), "#EXTM3U\n")
            .await
            .unwrap();
        fs::write(cfg.output_root.join("seg_000001.ts"), b"old-segment")
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        drop(tx);
        handle.await.unwrap().unwrap();

        assert!(fs::metadata(cfg.output_root.join("index.m3u8"))
            .await
            .is_ok());
        assert!(fs::metadata(cfg.output_root.join("seg_000001.ts"))
            .await
            .is_ok());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn v2_writes_current_session_pointer_on_startup() {
        let dir = test_dir("current_session_pointer");
        fs::create_dir_all(&dir).await.unwrap();
        let cfg = config(&dir);
        let (tx, rx) = mpsc::channel(8);

        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        drop(tx);
        handle.await.unwrap().unwrap();

        let pointer_raw = fs::read_to_string(cfg.output_root.join("current.json"))
            .await
            .expect("current session pointer");
        let pointer: Value = serde_json::from_str(&pointer_raw).expect("valid pointer json");
        assert!(pointer
            .get("session_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty()));
        let writer_state = pointer
            .get("writer_state")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(writer_state == "starting" || writer_state == "stopped");

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[test]
    fn parse_error_rate_limit_helper_matches_expected_pattern() {
        assert!(super::should_log_rate_limited(1));
        assert!(super::should_log_rate_limited(2));
        assert!(super::should_log_rate_limited(3));
        assert!(!super::should_log_rate_limited(4));
        assert!(super::should_log_rate_limited(100));
    }

    #[test]
    fn no_publish_watchdog_latches_and_resets_after_segment_publish() {
        let mut episode = NoPublishEpisode {
            first_rtp_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(31)),
            segment_seq_at_start: 0,
            logged: false,
        };
        let stats = StreamRuntimeStats {
            total_rtp_packets: 10,
            total_access_units: 0,
            total_parse_errors: 3,
            ..StreamRuntimeStats::default()
        };

        super::maybe_log_no_publish_watchdog(9, &stats, 0, &mut episode);
        assert!(episode.logged);

        super::maybe_log_no_publish_watchdog(9, &stats, 1, &mut episode);
        assert!(episode.first_rtp_at.is_none());
        assert!(!episode.logged);
    }
}
