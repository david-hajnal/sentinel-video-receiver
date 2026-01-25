use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout, Instant},
};
use tracing::{error, info, warn};

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

#[derive(Clone, Debug)]
pub struct ClipRecorderConfig {
    pub output_dir: PathBuf,
    pub post_roll: Duration,
    /// Minimum clip duration - even if motion ends before IDR or shortly after, record at least this long
    pub min_clip_duration: Duration,
    /// Used only to help ffmpeg generate timestamps for raw h264 input.
    pub assumed_fps: u32,
    /// If true: -c:v copy (fast, no re-encode). If false: re-encode with libx264.
    pub stream_copy: bool,
    /// If true: add silent audio track for browser compatibility (CPU intensive on low-power devices)
    pub audio_enabled: bool,
    /// Maximum number of .mp4 files to keep (oldest deleted first)
    pub max_files: Option<usize>,
    /// Maximum age of .mp4 files in seconds (older files deleted)
    pub max_age_secs: Option<u64>,
    /// Maximum total bytes of all .mp4 files (oldest deleted until under limit)
    pub max_total_bytes: Option<u64>,
    /// Maximum size per clip in bytes (passed to ffmpeg -fs)
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
            assumed_fps: 25,
            stream_copy: true,
            audio_enabled: true,
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
        child: Child,
        stdin: ChildStdin,
        stop_at: Option<Instant>,
        hard_stop_at: Option<Instant>,
        #[allow(dead_code)]
        started_at: Instant,
        started_at_utc: DateTime<Utc>,
        file_path: PathBuf,
        /// Drain ffmpeg stderr to avoid deadlocks when stderr is piped.
        stderr_task: Option<JoinHandle<String>>,
    },
}

pub struct ClipRecorder {
    cfg: ClipRecorderConfig,
    state: State,
    // cached parameter sets (Annex-B NALs)
    last_sps: Option<Vec<u8>>,
    last_pps: Option<Vec<u8>>,
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

        // Periodic cleanup timer (every 60s)
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
        let has_active_motion = !state.is_empty();

        match &mut self.state {
            State::Idle => {
                if has_active_motion {
                    // Transition to Armed with first rule (sorted lexicographically)
                    if let Some((rule, meta)) = state.iter().min_by_key(|(k, _)| *k) {
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
            State::Armed { rule, camera_id, event_id, armed_at } => {
                if has_active_motion {
                    // New motion arrived while still armed - update to latest event
                    if let Some((new_rule, meta)) = state.iter().min_by_key(|(k, _)| *k) {
                        if meta.event_id != *event_id {
                            info!(
                                old_event_id = %event_id,
                                new_event_id = %meta.event_id,
                                "New motion detected while armed, updating to new event"
                            );
                            *rule = new_rule.clone();
                            *camera_id = meta.camera_id.clone();
                            *event_id = meta.event_id.clone();
                            *armed_at = Instant::now();
                        }
                    }
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
            State::Recording { stop_at, started_at, .. } => {
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
                                "Motion ended, recording minimum duration"
                            );
                            *stop_at = Some(deadline);
                        } else {
                            // Already met minimum duration, use normal post-roll
                            let deadline = Instant::now() + self.cfg.post_roll;
                            info!(
                                post_roll_secs = self.cfg.post_roll.as_secs(),
                                "Motion ended, post-roll timer started"
                            );
                            *stop_at = Some(deadline);
                        }
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
                return Ok(());
            }
            8 => {
                self.last_pps = Some(nal);
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
                    let (child, stdin, file_path, stderr_task) =
                        self.spawn_ffmpeg(&rule, &camera_id, &event_id).await?;
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
                        child,
                        stdin,
                        stop_at,
                        hard_stop_at,
                        started_at,
                        started_at_utc,
                        file_path,
                        stderr_task: Some(stderr_task),
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

            State::Recording { stdin, .. } => {
                // Keep feeding NALs
                if let Err(e) = stdin.write_all(&nal).await {
                    // ffmpeg died or pipe broken: stop and go idle
                    warn!(error = %e, "FFmpeg stdin write failed, stopping recording");
                    self.stop_recording().await?;
                }
                Ok(())
            }
        }
    }

    async fn maybe_stop_on_deadline(&mut self) -> Result<()> {
        let should_stop = match &self.state {
            State::Recording {
                stop_at,
                hard_stop_at,
                ..
            } => {
                let now = Instant::now();
                stop_at.map(|t| now >= t).unwrap_or(false)
                    || hard_stop_at.map(|t| now >= t).unwrap_or(false)
            }
            _ => false,
        };

        if should_stop {
            self.stop_recording().await?;
        }
        Ok(())
    }

    async fn stop_recording(&mut self) -> Result<()> {
        // Give ffmpeg enough time to finalize mp4 (faststart can add work).
        const GRACEFUL_WAIT: Duration = Duration::from_secs(15);

        if let State::Recording {
            rule,
            camera_id,
            event_id,
            mut child,
            mut stdin,
            file_path,
            started_at_utc,
            stderr_task,
            ..
        } = std::mem::replace(&mut self.state, State::Idle)
        {
            // Close stdin so ffmpeg can see EOF and finalize MP4 (moov atom).
            // IMPORTANT: drop(stdin) is the reliable "close" for pipes.
            let _ = stdin.shutdown().await;
            drop(stdin);

            // Wait for ffmpeg to exit cleanly.
            let status_res = timeout(GRACEFUL_WAIT, child.wait()).await;

            // Collect stderr (drained in background task).
            let stderr_txt = if let Some(t) = stderr_task {
                match timeout(Duration::from_secs(1), t).await {
                    Ok(Ok(s)) => s,
                    Ok(Err(join_err)) => format!("<<stderr task join error: {join_err}>>"),
                    Err(_) => "<<stderr task timeout>>".to_string(),
                }
            } else {
                String::new()
            };

            match status_res {
                Ok(Ok(status)) => {
                    if !status.success() {
                        error!(
                            status_code = status.code(),
                            part_file = ?file_path.with_extension("mp4.part"),
                            camera_id = %camera_id,
                            event_id = %event_id,
                            rule = %rule,
                            "FFmpeg exited with error, keeping .part file for debugging"
                        );
                        // Always log stderr on failure, even if empty
                        if stderr_txt.trim().is_empty() {
                            error!("FFmpeg stderr was empty - process may have crashed immediately");
                        } else {
                            error!(stderr = %stderr_txt, "FFmpeg stderr output");
                        }
                    } else {
                        // Success: rename .part to final .mp4
                        let part_path = file_path.with_extension("mp4.part");
                        if let Err(e) = tokio::fs::rename(&part_path, &file_path).await {
                            error!(
                                error = %e,
                                part_file = ?part_path,
                                final_file = ?file_path,
                                "Failed to rename .part file to final .mp4"
                            );
                        } else {
                            // Get file size
                            let file_size = tokio::fs::metadata(&file_path)
                                .await
                                .map(|m| m.len())
                                .unwrap_or(0);
                            
                            let ended_at = Utc::now();
                            let duration_secs = (ended_at - started_at_utc).num_seconds();
                            
                            info!(
                                camera_id = %camera_id,
                                event_id = %event_id,
                                rule = %rule,
                                file = ?file_path,
                                size_mb = file_size / 1_000_000,
                                duration_secs = duration_secs,
                                "Clip saved successfully"
                            );

                            // Send clip metadata if channel is configured
                            if let Some(tx) = &self.clip_meta_tx {
                                let meta = ClipMeta {
                                    camera_id,
                                    event_id,
                                    rule,
                                    file_path: file_path.clone(),
                                    file_size,
                                    started_at: started_at_utc,
                                    ended_at: Utc::now(),
                                };
                                // Non-blocking send: if consumer is slow, we don't wait
                                let _ = tx.try_send(meta);
                            }

                            // Trigger cleanup after successful save
                            let _ = self.cleanup_old_clips().await;
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        error = %e,
                        file = ?file_path,
                        camera_id = %camera_id,
                        event_id = %event_id,
                        rule = %rule,
                        "FFmpeg process wait error"
                    );
                    // Always log stderr on error
                    if stderr_txt.trim().is_empty() {
                        error!("FFmpeg stderr was empty");
                    } else {
                        error!(stderr = %stderr_txt, "FFmpeg stderr output");
                    }
                }
                Err(_) => {
                    // Timeout -> force kill. This WILL often produce a corrupt mp4.
                    // But draining stderr + closing stdin should make this rare.
                    let _ = child.kill().await;
                    error!(
                        file = ?file_path,
                        timeout_secs = GRACEFUL_WAIT.as_secs(),
                        "FFmpeg timeout, force killed (may produce corrupt file)"
                    );
                    if !stderr_txt.trim().is_empty() {
                        warn!(stderr = %stderr_txt, "FFmpeg stderr output");
                    }
                }
            }
        }
        Ok(())
    }

    async fn write_param_sets_and_idr(&mut self, st: &mut State, idr: &Vec<u8>) -> Result<()> {
        let (stdin, rule) = match st {
            State::Recording { stdin, rule, .. } => (stdin, rule.clone()),
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

        stdin.write_all(sps).await.context("write SPS")?;
        stdin.write_all(pps).await.context("write PPS")?;
        stdin.write_all(idr).await.context("write IDR")?;

        info!(rule = %rule, "Recording started");
        Ok(())
    }

    async fn spawn_ffmpeg(
        &self,
        rule: &str,
        camera_id: &str,
        event_id: &str,
    ) -> Result<(Child, ChildStdin, PathBuf, JoinHandle<String>)> {
        let ts = Utc::now().format("%Y%m%d_%H%M%S%.3fZ").to_string();
        let safe_rule = rule.replace(['/', '\\', ' '], "_");
        let file_path = self
            .cfg
            .output_dir
            .join(format!("{ts}_{camera_id}_{event_id}_{safe_rule}.mp4"));
        let part_path = self
            .cfg
            .output_dir
            .join(format!("{ts}_{camera_id}_{event_id}_{safe_rule}.mp4.part"));

        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-y")
            // input: raw H.264 Annex-B from stdin
            .arg("-fflags")
            .arg("+genpts")
            .arg("-r")
            .arg(self.cfg.assumed_fps.to_string())
            .arg("-f")
            .arg("h264")
            .arg("-i")
            .arg("pipe:0");

        // Conditionally add silent audio track for browser compatibility
        // (Firefox/Safari requirement, but CPU-intensive on low-power devices)
        if self.cfg.audio_enabled {
            cmd.arg("-f")
                .arg("lavfi")
                .arg("-i")
                .arg("anullsrc=channel_layout=stereo:sample_rate=48000");
        }

        if self.cfg.stream_copy {
            cmd.arg("-c:v").arg("copy");
        } else {
            cmd.arg("-c:v")
                .arg("libx264")
                .arg("-preset")
                .arg("veryfast")
                .arg("-tune")
                .arg("zerolatency");
        }
        
        // Encode silent audio as AAC if audio is enabled
        if self.cfg.audio_enabled {
            cmd.arg("-c:a")
                .arg("aac")
                .arg("-b:a")
                .arg("64k")
                .arg("-shortest");
        }

        // Add file size limit if configured
        if let Some(max_bytes) = self.cfg.max_clip_bytes {
            cmd.arg("-fs").arg(max_bytes.to_string());
        }

        cmd.arg("-movflags")
            .arg("+faststart")
            // Explicitly specify MP4 format for .part file
            .arg("-f")
            .arg("mp4")
            .arg(part_path.to_string_lossy().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            // IMPORTANT: keep stderr piped but DRAIN it to avoid deadlock.
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().context("spawn ffmpeg")?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("ffmpeg stdin missing"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("ffmpeg stderr missing"))?;

        // Drain stderr in the background and keep only a tail (avoid unbounded memory).
        let stderr_task: JoinHandle<String> = tokio::spawn(async move {
            const TAIL_MAX: usize = 64 * 1024;
            let mut tail: Vec<u8> = Vec::with_capacity(TAIL_MAX.min(8192));
            let mut buf = [0u8; 4096];

            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        tail.extend_from_slice(&buf[..n]);
                        if tail.len() > TAIL_MAX {
                            let drop_n = tail.len() - TAIL_MAX;
                            tail.drain(0..drop_n);
                        }
                    }
                    Err(_) => break,
                }
            }

            String::from_utf8_lossy(&tail).to_string()
        });

        Ok((child, stdin, file_path, stderr_task))
    }

    /// Cleanup old clips based on config constraints
    async fn cleanup_old_clips(&self) -> Result<()> {
        use std::time::SystemTime;

        // Only operate on *.mp4 files (not .part)
        let mut clips: Vec<(PathBuf, SystemTime, u64)> = Vec::new();

        let mut read_dir = tokio::fs::read_dir(&self.cfg.output_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("mp4") {
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

        // Delete marked files
        for path in to_delete {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    info!(file = ?path, "Deleted old clip");
                }
                Err(e) => {
                    warn!(error = %e, file = ?path, "Failed to delete old clip");
                }
            }
        }

        Ok(())
    }
}

fn nal_type_from_annexb(nal: &[u8]) -> Option<u8> {
    if nal.len() < 5 {
        return None;
    }
    if &nal[0..4] != [0, 0, 0, 1].as_slice() {
        return None;
    }
    Some(nal[4] & 0x1F)
}
