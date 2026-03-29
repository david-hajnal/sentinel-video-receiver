use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{path::PathBuf, time::Duration};
use tokio::{
    sync::mpsc,
    time::{sleep, Instant},
};
use tracing::{debug, error, info, warn};

use crate::core::clip_writer::ClipWriter;
use crate::core::video::nal_type_from_annexb;
use crate::event::MotionState;

/// Metadata sent when a clip finishes recording successfully
#[derive(Clone, Debug)]
pub struct ClipMeta {
    pub camera_id: String,
    pub event_id: String,
    pub rule: String,
    pub file_path: PathBuf,
    pub file_size: u64,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct ClipSidecar {
    camera_id: String,
    event_id: String,
    rule: String,
    filename: String,
    file_size_bytes: u64,
    started_at: String,
    ended_at: String,
    duration_secs: i64,
}

#[derive(Clone, Debug)]
struct StickyMotion {
    rule: String,
    camera_id: String,
    event_id: String,
}

#[derive(Clone, Debug)]
pub struct ClipRecorderConfig {
    pub output_dir: PathBuf,
    pub post_roll: Duration,
    /// Minimum clip duration - even if motion ends before IDR or shortly after, record at least this long
    pub min_clip_duration: Duration,
    /// Flush clip file periodically to reduce data loss on crash
    pub flush_interval: Duration,
    /// Delete stale .part files older than this on startup
    pub stale_part_max_age: Duration,
    /// Maximum buffered bytes before forcing a write
    pub write_batch_bytes: usize,
    /// Maximum number of .h264 files to keep (oldest deleted first)
    pub max_files: Option<usize>,
    /// Maximum age of .h264 files in seconds (older files deleted)
    pub max_age_secs: Option<u64>,
    /// Maximum total bytes of all .h264 files (oldest deleted until under limit)
    pub max_total_bytes: Option<u64>,
    /// Maximum size per clip in bytes (hard stop)
    pub max_clip_bytes: Option<u64>,
    /// Maximum duration per clip in seconds (hard stop)
    pub max_clip_secs: Option<u64>,
}

impl Default for ClipRecorderConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("clips"),
            post_roll: Duration::from_secs(3),
            min_clip_duration: Duration::from_secs(5),
            flush_interval: Duration::from_secs(1),
            stale_part_max_age: Duration::from_secs(24 * 60 * 60),
            write_batch_bytes: 256 * 1024,
            max_files: None,
            max_age_secs: None,
            max_total_bytes: None,
            max_clip_bytes: None,
            max_clip_secs: None,
        }
    }
}
enum State {
    Idle,
    Armed {
        rule: String,
        camera_id: String,
        event_id: String,
        armed_at: Instant,
    },
    Recording {
        rule: String,
        camera_id: String,
        event_id: String,
        writer: ClipWriter,
        stop_at: Option<Instant>,
        hard_stop_at: Option<Instant>,
        #[allow(dead_code)]
        started_at: Instant,
        started_at_utc: DateTime<Utc>,
        file_path: PathBuf,
    },
}

pub struct ClipRecorder {
    cfg: ClipRecorderConfig,
    state: State,
    // cached parameter sets (Annex-B NALs)
    last_sps: Option<Vec<u8>>,
    last_pps: Option<Vec<u8>>,
    last_motion_state: MotionState,
    sticky_motion: Option<StickyMotion>,
    // Optional channel to send clip metadata when recording completes
    clip_meta_tx: Option<mpsc::Sender<ClipMeta>>,
}

impl ClipRecorder {
    pub fn new(cfg: ClipRecorderConfig) -> Self {
        Self {
            cfg,
            state: State::Idle,
            last_sps: None,
            last_pps: None,
            last_motion_state: MotionState::new(),
            sticky_motion: None,
            clip_meta_tx: None,
        }
    }

    /// Set the channel for sending clip metadata when recordings complete
    pub fn with_clip_meta_tx(mut self, tx: mpsc::Sender<ClipMeta>) -> Self {
        self.clip_meta_tx = Some(tx);
        self
    }

    pub async fn run(
        mut self,
        mut motion_rx: tokio::sync::watch::Receiver<MotionState>,
        mut nal_rx: mpsc::Receiver<Vec<u8>>,
    ) -> Result<()> {
        tokio::fs::create_dir_all(&self.cfg.output_dir).await?;
        let _ = self.cleanup_stale_parts().await;

        // Periodic cleanup timer (every 60s)
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut flush_interval = tokio::time::interval(self.cfg.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Periodically check stop deadline
            let tick = sleep(Duration::from_millis(200));
            tokio::pin!(tick);

            tokio::select! {
                _ = &mut tick => {
                    self.maybe_stop_on_deadline().await?;
                }

                _ = cleanup_interval.tick() => {
                    let _ = self.cleanup_old_clips().await;
                }

                _ = flush_interval.tick() => {
                    let _ = self.flush_recording().await;
                }

                // Watch for motion state changes
                Ok(()) = motion_rx.changed() => {
                    let state = motion_rx.borrow_and_update().clone();
                    self.on_motion_state_change(&state).await?;
                }

                Some(nal) = nal_rx.recv() => {
                    self.on_nal(nal).await?;
                }

                else => break,
            }
        }

        // Best-effort finalize if still recording.
        let _ = self.stop_recording().await;

        Ok(())
    }

    /// Handle motion state changes (watch-based)
    async fn on_motion_state_change(&mut self, state: &MotionState) -> Result<()> {
        self.last_motion_state = state.clone();
        let recording_active = matches!(self.state, State::Recording { .. });
        if state.is_empty() && !recording_active {
            self.sticky_motion = None;
        } else if !state.is_empty() && self.sticky_motion.is_none() {
            if let Some((rule, meta)) = state.iter().min_by_key(|(k, _)| *k) {
                self.sticky_motion = Some(StickyMotion {
                    rule: rule.clone(),
                    camera_id: meta.camera_id.clone(),
                    event_id: meta.event_id.clone(),
                });
            }
        }
        let has_active_motion = !state.is_empty();
        debug!(
            active = has_active_motion,
            active_count = state.len(),
            "Motion state update"
        );

        match &mut self.state {
            State::Idle => {
                if has_active_motion {
                    // Transition to Armed using the first motion event (sticky until motion ends)
                    if let Some(sticky) = &self.sticky_motion {
                        info!(
                            rule = %sticky.rule,
                            camera_id = %sticky.camera_id,
                            event_id = %sticky.event_id,
                            "Motion detected, arming recorder (waiting for IDR frame)"
                        );
                        self.state = State::Armed {
                            rule: sticky.rule.clone(),
                            camera_id: sticky.camera_id.clone(),
                            event_id: sticky.event_id.clone(),
                            armed_at: Instant::now(),
                        };
                    } else if let Some((rule, meta)) = state.iter().min_by_key(|(k, _)| *k) {
                        info!(
                            rule = %rule,
                            camera_id = %meta.camera_id,
                            event_id = %meta.event_id,
                            "Motion detected, arming recorder (waiting for IDR frame)"
                        );
                        self.state = State::Armed {
                            rule: rule.clone(),
                            camera_id: meta.camera_id.clone(),
                            event_id: meta.event_id.clone(),
                            armed_at: Instant::now(),
                        };
                    }
                }
            }
            State::Armed {
                rule,
                camera_id,
                event_id,
                armed_at,
            } => {
                if has_active_motion {
                    // Merge subsequent motion events into the first while armed.
                    debug!(
                        rule = %rule,
                        camera_id = %camera_id,
                        event_id = %event_id,
                        "Motion detected while armed, keeping original event id"
                    );
                    *armed_at = Instant::now();
                } else {
                    // Motion ended before IDR -> stay armed and record minimum duration when IDR arrives
                    info!("Motion ended before IDR frame received, will record minimum duration clip when IDR arrives");

                    // Check for timeout - if armed for too long without starting recording, give up
                    if armed_at.elapsed() > Duration::from_secs(30) {
                        warn!(
                            elapsed_secs = armed_at.elapsed().as_secs(),
                            "Armed state timeout - no IDR frame with SPS/PPS received, disarming"
                        );
                        self.state = State::Idle;
                        return Ok(());
                    }
                }
                // Stay armed until IDR arrives to ensure we always record something
            }
            State::Recording {
                stop_at,
                started_at,
                ..
            } => {
                if has_active_motion {
                    // Motion continues or new motion started -> clear stop deadline (extend recording)
                    if stop_at.is_some() {
                        info!("Motion detected during recording, extending recording");
                    }
                    *stop_at = None;
                } else {
                    // Motion ended -> set stop deadline based on elapsed time
                    if stop_at.is_none() {
                        let elapsed = started_at.elapsed();
                        let remaining_min = self.cfg.min_clip_duration.saturating_sub(elapsed);

                        if remaining_min > Duration::ZERO {
                            // Still need to record more to meet minimum duration
                            let deadline = Instant::now() + remaining_min;
                            info!(
                                min_remaining_secs = remaining_min.as_secs(),
                                elapsed_secs = elapsed.as_secs(),
                                "Motion ended, recording minimum duration"
                            );
                            *stop_at = Some(deadline);
                        } else {
                            // Already met minimum duration, use normal post-roll
                            let deadline = Instant::now() + self.cfg.post_roll;
                            info!(
                                post_roll_secs = self.cfg.post_roll.as_secs(),
                                elapsed_secs = elapsed.as_secs(),
                                "Motion ended, post-roll timer started"
                            );
                            *stop_at = Some(deadline);
                        }
                    } else {
                        debug!("Motion ended, stop deadline already set");
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_nal(&mut self, nal: Vec<u8>) -> Result<()> {
        // Expect Annex-B NAL: 00 00 00 01 <header> ...
        let Some(nt) = nal_type_from_annexb(&nal) else {
            return Ok(());
        };

        // cache SPS/PPS always
        match nt {
            7 => {
                self.last_sps = Some(nal);
                if let State::Recording { writer, .. } = &mut self.state {
                    let _ = writer.write_nal(self.last_sps.as_ref().unwrap()).await;
                }
                return Ok(());
            }
            8 => {
                self.last_pps = Some(nal);
                if let State::Recording { writer, .. } = &mut self.state {
                    let _ = writer.write_nal(self.last_pps.as_ref().unwrap()).await;
                }
                return Ok(());
            }
            _ => {}
        }

        match &mut self.state {
            State::Idle => Ok(()),

            State::Armed {
                rule,
                camera_id,
                event_id,
                armed_at,
            } => {
                // Start only on IDR and only if SPS/PPS available (for decoders/MP4)
                if nt == 5 && self.last_sps.is_some() && self.last_pps.is_some() {
                    let armed_duration = armed_at.elapsed();
                    info!(
                        rule = %rule,
                        camera_id = %camera_id,
                        event_id = %event_id,
                        armed_duration_ms = armed_duration.as_millis(),
                        "IDR frame received while armed, starting recording"
                    );
                    let rule = rule.clone();
                    let camera_id = camera_id.clone();
                    let event_id = event_id.clone();
                    let (writer, file_path) =
                        self.spawn_clip_writer(&rule, &camera_id, &event_id).await?;
                    info!(file = ?file_path, "Clip writer created");
                    let started_at = Instant::now();
                    let started_at_utc = Utc::now();
                    let hard_stop_at = self
                        .cfg
                        .max_clip_secs
                        .map(|secs| started_at + Duration::from_secs(secs));

                    // Set stop_at to ensure minimum duration recording
                    let stop_at = Some(started_at + self.cfg.min_clip_duration);

                    let mut new_state = State::Recording {
                        rule,
                        camera_id,
                        event_id,
                        writer,
                        stop_at,
                        hard_stop_at,
                        started_at,
                        started_at_utc,
                        file_path,
                    };
                    // Write SPS/PPS + first IDR
                    self.write_param_sets_and_idr(&mut new_state, &nal).await?;
                    self.state = new_state;
                } else if nt == 5 {
                    warn!(
                        has_sps = self.last_sps.is_some(),
                        has_pps = self.last_pps.is_some(),
                        armed_duration_ms = armed_at.elapsed().as_millis(),
                        "IDR frame received but missing SPS/PPS, waiting for parameter sets"
                    );
                }
                Ok(())
            }

            State::Recording { writer, .. } => {
                // Keep feeding NALs
                if let Err(e) = writer.write_nal(&nal).await {
                    warn!(error = %e, "Clip write failed, stopping recording");
                    self.stop_recording().await?;
                    return Ok(());
                }

                if let Some(max_bytes) = self.cfg.max_clip_bytes {
                    if writer.total_bytes() >= max_bytes {
                        info!(
                            max_clip_bytes = max_bytes,
                            current_bytes = writer.total_bytes(),
                            "Clip size limit reached, stopping recording"
                        );
                        self.stop_recording().await?;
                    }
                }
                Ok(())
            }
        }
    }

    async fn maybe_stop_on_deadline(&mut self) -> Result<()> {
        let (should_stop, stop_reason, hard_due) = match &self.state {
            State::Recording {
                stop_at,
                hard_stop_at,
                ..
            } => {
                let now = Instant::now();
                let stop_due = stop_at.map(|t| now >= t).unwrap_or(false);
                let hard_due = hard_stop_at.map(|t| now >= t).unwrap_or(false);
                (
                    stop_due || hard_due,
                    if hard_due {
                        "hard_stop"
                    } else if stop_due {
                        "stop_at"
                    } else {
                        "none"
                    },
                    hard_due,
                )
            }
            _ => (false, "none", false),
        };

        if should_stop {
            info!(
                reason = stop_reason,
                "Stop deadline reached, finalizing clip"
            );
            self.stop_recording().await?;
            if hard_due {
                self.rearm_if_motion_active().await?;
            }
        }
        Ok(())
    }

    async fn stop_recording(&mut self) -> Result<()> {
        if let State::Recording {
            rule,
            camera_id,
            event_id,
            file_path,
            started_at_utc,
            mut writer,
            ..
        } = std::mem::replace(&mut self.state, State::Idle)
        {
            info!(file = ?file_path, "Stopping recording");
            if let Err(e) = writer.flush().await {
                warn!(error = %e, "Clip flush failed");
            }

            let part_path = writer.part_path().clone();
            let final_path = match writer.finalize().await {
                Ok(p) => p,
                Err(e) => {
                    error!(
                        error = %e,
                        part_file = ?part_path,
                        final_file = ?file_path,
                        "Failed to finalize clip file, keeping .part"
                    );
                    return Ok(());
                }
            };
            info!(part_file = ?part_path, final_file = ?final_path, "Clip finalized");

            let file_size = tokio::fs::metadata(&final_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0);

            let ended_at = Utc::now();
            let duration_secs = (ended_at - started_at_utc).num_seconds();

            info!(
                camera_id = %camera_id,
                event_id = %event_id,
                rule = %rule,
                file = ?final_path,
                size_mb = file_size / 1_000_000,
                duration_secs = duration_secs,
                "Clip saved successfully"
            );

            let _ = self
                .write_sidecar_json(
                    &final_path,
                    &camera_id,
                    &event_id,
                    &rule,
                    file_size,
                    started_at_utc,
                    ended_at,
                )
                .await;

            if let Some(tx) = &self.clip_meta_tx {
                let meta = ClipMeta {
                    camera_id,
                    event_id,
                    rule,
                    file_path: final_path.clone(),
                    file_size,
                    started_at: started_at_utc,
                    ended_at,
                };
                let _ = tx.try_send(meta);
            }

            let _ = self.cleanup_old_clips().await;
        }
        Ok(())
    }

    async fn rearm_if_motion_active(&mut self) -> Result<()> {
        if self.last_motion_state.is_empty() {
            return Ok(());
        }

        if let Some(sticky) = &self.sticky_motion {
            info!(
                rule = %sticky.rule,
                camera_id = %sticky.camera_id,
                event_id = %sticky.event_id,
                "Hard stop reached with motion active, re-arming recorder"
            );
            self.state = State::Armed {
                rule: sticky.rule.clone(),
                camera_id: sticky.camera_id.clone(),
                event_id: sticky.event_id.clone(),
                armed_at: Instant::now(),
            };
        } else if let Some((rule, meta)) = self.last_motion_state.iter().min_by_key(|(k, _)| *k) {
            info!(
                rule = %rule,
                camera_id = %meta.camera_id,
                event_id = %meta.event_id,
                "Hard stop reached with motion active, re-arming recorder"
            );
            self.state = State::Armed {
                rule: rule.clone(),
                camera_id: meta.camera_id.clone(),
                event_id: meta.event_id.clone(),
                armed_at: Instant::now(),
            };
        }

        Ok(())
    }

    async fn flush_recording(&mut self) -> Result<()> {
        if let State::Recording { writer, .. } = &mut self.state {
            if let Err(e) = writer.flush().await {
                debug!(error = %e, "Periodic clip flush failed");
            }
        }
        Ok(())
    }

    async fn write_sidecar_json(
        &self,
        file_path: &PathBuf,
        camera_id: &str,
        event_id: &str,
        rule: &str,
        file_size: u64,
        started_at: DateTime<Utc>,
        ended_at: DateTime<Utc>,
    ) -> Result<()> {
        let filename = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown.h264")
            .to_string();

        let sidecar = ClipSidecar {
            camera_id: camera_id.to_string(),
            event_id: event_id.to_string(),
            rule: rule.to_string(),
            filename,
            file_size_bytes: file_size,
            started_at: started_at.to_rfc3339(),
            ended_at: ended_at.to_rfc3339(),
            duration_secs: (ended_at - started_at).num_seconds(),
        };

        let json_path = file_path.with_extension("json");
        let json = serde_json::to_vec_pretty(&sidecar)?;
        tokio::fs::write(&json_path, json).await?;
        Ok(())
    }

    async fn cleanup_stale_parts(&self) -> Result<()> {
        use std::time::SystemTime;
        let cutoff = SystemTime::now() - self.cfg.stale_part_max_age;
        debug!(
            cutoff_secs = self.cfg.stale_part_max_age.as_secs(),
            "Scanning for stale .part clips"
        );

        let mut read_dir = tokio::fs::read_dir(&self.cfg.output_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
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

    async fn write_param_sets_and_idr(&mut self, st: &mut State, idr: &Vec<u8>) -> Result<()> {
        let (writer, rule) = match st {
            State::Recording { writer, rule, .. } => (writer, rule.clone()),
            _ => return Ok(()),
        };

        let sps = self
            .last_sps
            .as_ref()
            .ok_or_else(|| anyhow!("missing SPS"))?;
        let pps = self
            .last_pps
            .as_ref()
            .ok_or_else(|| anyhow!("missing PPS"))?;

        writer.write_nal(sps).await.context("write SPS")?;
        writer.write_nal(pps).await.context("write PPS")?;
        writer.write_nal(idr).await.context("write IDR")?;

        info!(rule = %rule, "Recording started");
        Ok(())
    }

    async fn spawn_clip_writer(
        &self,
        rule: &str,
        camera_id: &str,
        event_id: &str,
    ) -> Result<(ClipWriter, PathBuf)> {
        let ts = Utc::now().format("%Y%m%d_%H%M%S%.3fZ").to_string();
        let safe_rule = rule.replace(['/', '\\', ' '], "_");
        let file_path = self
            .cfg
            .output_dir
            .join(format!("{ts}_{camera_id}_{event_id}_{safe_rule}.h264"));
        let part_path = self
            .cfg
            .output_dir
            .join(format!("{ts}_{camera_id}_{event_id}_{safe_rule}.h264.part"));

        let writer =
            ClipWriter::create(part_path, file_path.clone(), self.cfg.write_batch_bytes).await?;
        Ok((writer, file_path))
    }

    /// Cleanup old clips based on config constraints
    async fn cleanup_old_clips(&self) -> Result<()> {
        use std::time::SystemTime;

        // Only operate on *.h264 files (not .part)
        let mut clips: Vec<(PathBuf, SystemTime, u64)> = Vec::new();

        let mut read_dir = tokio::fs::read_dir(&self.cfg.output_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("h264") {
                continue;
            }

            let metadata = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(_) => continue,
            };

            let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let size = metadata.len();
            clips.push((path, mtime, size));
        }

        if clips.is_empty() {
            return Ok(());
        }

        // Sort by mtime (oldest first)
        clips.sort_by_key(|(_, mtime, _)| *mtime);

        let mut to_delete: Vec<PathBuf> = Vec::new();

        // 1. Delete files older than max_age_secs
        if let Some(max_age) = self.cfg.max_age_secs {
            let cutoff = SystemTime::now() - Duration::from_secs(max_age);
            for (path, mtime, _) in &clips {
                if *mtime < cutoff {
                    to_delete.push(path.clone());
                }
            }
        }

        // Remove marked files from clips list
        clips.retain(|(path, _, _)| !to_delete.contains(path));

        // 2. Delete oldest files if count exceeds max_files
        if let Some(max_files) = self.cfg.max_files {
            if clips.len() > max_files {
                let excess = clips.len() - max_files;
                for (path, _, _) in clips.iter().take(excess) {
                    to_delete.push(path.clone());
                }
                clips.drain(0..excess);
            }
        }

        // 3. Delete oldest files if total size exceeds max_total_bytes
        if let Some(max_bytes) = self.cfg.max_total_bytes {
            let total_size: u64 = clips.iter().map(|(_, _, size)| size).sum();
            if total_size > max_bytes {
                let mut current_size = total_size;
                for (path, _, size) in &clips {
                    if current_size <= max_bytes {
                        break;
                    }
                    to_delete.push(path.clone());
                    current_size -= size;
                }
            }
        }

        // Delete marked files (and sidecar JSON if present)
        for path in to_delete {
            let sidecar = path.with_extension("json");
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    info!(file = ?path, "Deleted old clip");
                }
                Err(e) => {
                    warn!(error = %e, file = ?path, "Failed to delete old clip");
                }
            }
            let _ = tokio::fs::remove_file(&sidecar).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ClipRecorder, ClipRecorderConfig, State};
    use crate::event::{MotionMetadata, MotionState};
    use chrono::{Duration as ChronoDuration, Utc};
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::time::Instant;
    use ulid::Ulid;

    fn test_output_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sentinel_clip_recorder_{name}_{}", Ulid::new()));
        std::fs::create_dir_all(&dir).expect("create test output dir");
        dir
    }

    fn active_motion_state(rule: &str, camera_id: &str, event_id: &str) -> MotionState {
        let mut state = MotionState::new();
        state.insert(
            rule.to_string(),
            MotionMetadata {
                camera_id: camera_id.to_string(),
                event_id: event_id.to_string(),
            },
        );
        state
    }

    fn sps_nal() -> Vec<u8> {
        vec![0, 0, 0, 1, 0x67, 0x64, 0x00, 0x1F]
    }

    fn pps_nal() -> Vec<u8> {
        vec![0, 0, 0, 1, 0x68, 0xEE, 0x3C, 0x80]
    }

    fn idr_nal() -> Vec<u8> {
        vec![0, 0, 0, 1, 0x65, 0x88, 0x84]
    }

    #[tokio::test]
    async fn motion_end_before_min_duration_uses_remaining_minimum_deadline() {
        let output_dir = test_output_dir("remaining_min");
        let mut recorder = ClipRecorder::new(ClipRecorderConfig {
            output_dir: output_dir.clone(),
            post_roll: Duration::from_secs(1),
            min_clip_duration: Duration::from_secs(5),
            ..ClipRecorderConfig::default()
        });

        let (writer, file_path) = recorder
            .spawn_clip_writer("rule-a", "cam-1", "evt-1")
            .await
            .expect("spawn clip writer");

        recorder.state = State::Recording {
            rule: "rule-a".to_string(),
            camera_id: "cam-1".to_string(),
            event_id: "evt-1".to_string(),
            writer,
            stop_at: None,
            hard_stop_at: None,
            started_at: Instant::now() - Duration::from_secs(2),
            started_at_utc: Utc::now() - ChronoDuration::seconds(2),
            file_path,
        };

        recorder
            .on_motion_state_change(&MotionState::new())
            .await
            .expect("handle motion end");

        let remaining = match &recorder.state {
            State::Recording {
                stop_at: Some(stop_at),
                ..
            } => stop_at.saturating_duration_since(Instant::now()),
            _ => panic!("recorder should stay in recording state with a stop deadline"),
        };

        assert!(
            remaining >= Duration::from_millis(2400),
            "expected minimum-duration deadline, got {:?}",
            remaining
        );
        assert!(
            remaining <= Duration::from_millis(3600),
            "remaining minimum deadline drifted too far: {:?}",
            remaining
        );

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(&output_dir).await;
    }

    #[tokio::test]
    async fn motion_end_after_min_duration_uses_post_roll_deadline() {
        let output_dir = test_output_dir("post_roll");
        let mut recorder = ClipRecorder::new(ClipRecorderConfig {
            output_dir: output_dir.clone(),
            post_roll: Duration::from_secs(2),
            min_clip_duration: Duration::from_secs(5),
            ..ClipRecorderConfig::default()
        });

        let (writer, file_path) = recorder
            .spawn_clip_writer("rule-a", "cam-1", "evt-1")
            .await
            .expect("spawn clip writer");

        recorder.state = State::Recording {
            rule: "rule-a".to_string(),
            camera_id: "cam-1".to_string(),
            event_id: "evt-1".to_string(),
            writer,
            stop_at: None,
            hard_stop_at: None,
            started_at: Instant::now() - Duration::from_secs(8),
            started_at_utc: Utc::now() - ChronoDuration::seconds(8),
            file_path,
        };

        recorder
            .on_motion_state_change(&MotionState::new())
            .await
            .expect("handle motion end");

        let remaining = match &recorder.state {
            State::Recording {
                stop_at: Some(stop_at),
                ..
            } => stop_at.saturating_duration_since(Instant::now()),
            _ => panic!("recorder should stay in recording state with a stop deadline"),
        };

        assert!(
            remaining >= Duration::from_millis(1500),
            "expected post-roll deadline, got {:?}",
            remaining
        );
        assert!(
            remaining <= Duration::from_millis(2600),
            "post-roll deadline drifted too far: {:?}",
            remaining
        );

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(&output_dir).await;
    }

    #[tokio::test]
    async fn recording_start_sets_hard_stop_from_max_clip_secs() {
        let output_dir = test_output_dir("hard_stop");
        let mut recorder = ClipRecorder::new(ClipRecorderConfig {
            output_dir: output_dir.clone(),
            min_clip_duration: Duration::from_secs(5),
            max_clip_secs: Some(4),
            ..ClipRecorderConfig::default()
        });

        recorder
            .on_motion_state_change(&active_motion_state("rule-a", "cam-1", "evt-1"))
            .await
            .expect("arm recorder");
        recorder.on_nal(sps_nal()).await.expect("feed sps");
        recorder.on_nal(pps_nal()).await.expect("feed pps");
        recorder.on_nal(idr_nal()).await.expect("feed idr");

        let (hard_window, min_window) = match &recorder.state {
            State::Recording {
                started_at,
                stop_at: Some(stop_at),
                hard_stop_at: Some(hard_stop_at),
                ..
            } => (
                hard_stop_at.duration_since(*started_at),
                stop_at.duration_since(*started_at),
            ),
            _ => panic!("recorder should transition into recording with hard stop"),
        };

        assert!(
            hard_window >= Duration::from_millis(3500)
                && hard_window <= Duration::from_millis(4500),
            "hard-stop window should be close to max_clip_secs, got {:?}",
            hard_window
        );
        assert!(
            min_window >= Duration::from_millis(4500) && min_window <= Duration::from_millis(5500),
            "minimum clip window should be close to min_clip_duration, got {:?}",
            min_window
        );

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(&output_dir).await;
    }
}
