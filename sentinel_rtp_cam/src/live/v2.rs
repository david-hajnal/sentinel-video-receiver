use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tracing::info;

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
    last_idr_at: Option<String>,
    last_segment_write_at: Option<String>,
    segment_seq: u64,
    discontinuity_count: u64,
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
        fs::rename(&self.temp_path, &self.final_path).await.with_context(|| {
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
    fs::create_dir_all(&cfg.output_root).await?;
    cleanup_v2_output(&cfg.output_root).await?;

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

    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                maybe_log_stream_stats(&cfg, &mut stats, latest_segment_seq, discontinuity_count);
                maybe_write_stream_health(
                    &cfg,
                    &mut stats,
                    latest_segment_seq,
                    discontinuity_count,
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
                            &cfg,
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
                    }
                    StreamMsg::Rtp(bytes) => {
                        stats.total_rtp_packets = stats.total_rtp_packets.saturating_add(1);
                        stats.last_rtp_at = Some(Utc::now());

                        let pkt = match RtpPacket::parse(&bytes) {
                            Ok(pkt) => pkt,
                            Err(_) => {
                                mark_stream_error(&mut stats);
                                handle_discontinuity(
                                    &cfg,
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
                                    &cfg,
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
                            Err(_) => {
                                mark_stream_error(&mut stats);
                                handle_discontinuity(
                                    &cfg,
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
                                    &cfg,
                                    &mut playlist,
                                    &mut current_segment,
                                    &mut latest_segment_seq,
                                )
                                .await?;
                            }

                            if current_segment.is_none() {
                                if !au.has_idr {
                                    continue;
                                }
                                current_segment = Some(open_segment(
                                    &cfg,
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
        &cfg,
        &mut playlist,
        &mut current_segment,
        &mut latest_segment_seq,
    )
    .await?;
    write_stream_health(&cfg, &stats, latest_segment_seq, discontinuity_count).await?;
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
    cfg: &LivePipelineV2Config,
    playlist: &mut PlaylistState,
    discontinuity: bool,
) -> Result<CurrentSegment> {
    let (seq, path) = playlist.next_segment_path(&cfg.output_root);
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
    cfg: &LivePipelineV2Config,
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
        .push_segment(&cfg.output_root, seq, duration_secs, discontinuity)
        .await?;
    *latest_segment_seq = seq;
    Ok(())
}

async fn handle_discontinuity(
    cfg: &LivePipelineV2Config,
    playlist: &mut PlaylistState,
    current_segment: &mut Option<CurrentSegment>,
    pending_discontinuity: &mut bool,
    discontinuity_count: &mut u64,
    latest_segment_seq: &mut u64,
) -> Result<()> {
    let saw_published_output = *latest_segment_seq > 0 || current_segment.is_some();
    finalize_segment(cfg, playlist, current_segment, latest_segment_seq).await?;
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
) -> Result<()> {
    let now = Instant::now();
    if stats
        .last_health_write_at
        .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
    {
        return Ok(());
    }
    write_stream_health(cfg, stats, segment_seq, discontinuity_count).await?;
    stats.last_health_write_at = Some(now);
    Ok(())
}

async fn write_stream_health(
    cfg: &LivePipelineV2Config,
    stats: &StreamRuntimeStats,
    segment_seq: u64,
    discontinuity_count: u64,
) -> Result<()> {
    let now = Utc::now();
    let snapshot = StreamHealthSnapshot {
        stream_id: cfg.stream_id,
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
        segment_seq,
        discontinuity_count,
    };

    AtomicPublisher::publish_bytes(
        &cfg.health_dir.join(format!("stream_{}.health.json", cfg.stream_id)),
        &serde_json::to_vec_pretty(&snapshot)?,
    )
    .await
}

async fn cleanup_v2_output(output_root: &Path) -> Result<()> {
    if fs::metadata(output_root).await.is_err() {
        return Ok(());
    }

    let mut read_dir = fs::read_dir(output_root).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let name = path.file_name().and_then(|value| value.to_str()).unwrap_or("");
        if name == "index.m3u8"
            || name.ends_with(".tmp")
            || (name.starts_with("seg_") && name.ends_with(".ts"))
        {
            let _ = fs::remove_file(&path).await;
        }
    }

    Ok(())
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

        packet[write_offset..write_offset + take]
            .copy_from_slice(&payload[offset..offset + take]);
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
    use super::{run_stream_v2, temp_path_for, AtomicPublisher, LivePipelineV2Config};
    use crate::server_pipeline::StreamMsg;
    use std::path::PathBuf;
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
        tx.send(StreamMsg::Rtp(rtp_single_nal(1, 90_000, true, &[0x41, 0x01])))
            .await
            .unwrap();
        drop(tx);

        handle.await.unwrap().unwrap();

        assert!(fs::metadata(cfg.output_root.join("index.m3u8")).await.is_err());
        let mut entries = fs::read_dir(&cfg.output_root).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn v2_publishes_segment_after_sps_pps_and_idr_sync() {
        let dir = test_dir("sync");
        fs::create_dir_all(&dir).await.unwrap();
        let cfg = config(&dir);
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(run_stream_v2(cfg.clone(), rx));
        tx.send(StreamMsg::Rtp(rtp_single_nal(1, 90_000, true, &[0x67, 0x64, 0x00])))
            .await
            .unwrap();
        tx.send(StreamMsg::Rtp(rtp_single_nal(2, 93_600, true, &[0x68, 0xEE, 0x3C])))
            .await
            .unwrap();
        tx.send(StreamMsg::Rtp(rtp_single_nal(3, 97_200, true, &[0x65, 0x88, 0x99])))
            .await
            .unwrap();
        drop(tx);

        handle.await.unwrap().unwrap();

        let playlist = fs::read_to_string(cfg.output_root.join("index.m3u8"))
            .await
            .unwrap();
        assert!(playlist.contains("#EXTM3U"));
        assert!(playlist.contains("seg_000001.ts"));
        assert!(fs::metadata(cfg.output_root.join("seg_000001.ts")).await.is_ok());

        fs::remove_dir_all(&dir).await.unwrap();
    }
}
