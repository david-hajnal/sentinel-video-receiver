use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout, Instant},
};
use tracing::{warn, info, error};

use crate::event_bus::{Event, MotionEvent};

#[derive(Clone, Debug)]
pub struct ClipRecorderConfig {
    pub output_dir: PathBuf,
    pub post_roll: Duration,
    /// Used only to help ffmpeg generate timestamps for raw h264 input.
    pub assumed_fps: u32,
    /// If true: -c:v copy (fast, no re-encode). If false: re-encode with libx264.
    pub stream_copy: bool,
}

impl Default for ClipRecorderConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("clips"),
            post_roll: Duration::from_secs(3),
            assumed_fps: 25,
            stream_copy: true,
        }
    }
}
enum State {
    Idle,
    Armed { rule: String },
    Recording {
        rule: String,
        child: Child,
        stdin: ChildStdin,
        stop_at: Option<Instant>,
        #[allow(dead_code)]
        started_at: Instant,
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
}

impl ClipRecorder {
    pub fn new(cfg: ClipRecorderConfig) -> Self {
        Self {
            cfg,
            state: State::Idle,
            last_sps: None,
            last_pps: None,
        }
    }

    pub async fn run(
        mut self,
        mut motion_rx: mpsc::Receiver<Event>,
        mut nal_rx: mpsc::Receiver<Vec<u8>>,
    ) -> Result<()> {
        tokio::fs::create_dir_all(&self.cfg.output_dir).await?;

        loop {
            // also periodically check stop deadline
            let tick = sleep(Duration::from_millis(200));
            tokio::pin!(tick);

            tokio::select! {
                _ = &mut tick => {
                    self.maybe_stop_on_deadline().await?;
                }

                Some(ev) = motion_rx.recv() => {
                    let Event::Motion(m) = ev;
                    self.on_motion(m).await?;
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

    async fn on_motion(&mut self, m: MotionEvent) -> Result<()> {
        if m.active {
            // Motion start: arm recording (we'll start at next IDR)
            self.state = State::Armed { rule: m.rule.clone() };
        } else {
            // Motion end: if recording, set stop deadline (post-roll)
            match &mut self.state {
                State::Recording { stop_at, .. } => {
                    *stop_at = Some(Instant::now() + self.cfg.post_roll);
                }
                State::Armed { .. } => {
                    // motion ended before we hit an IDR — disarm
                    self.state = State::Idle;
                }
                State::Idle => {}
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

            State::Armed { rule } => {
                // Start only on IDR and only if SPS/PPS available (for decoders/MP4)
                if nt == 5 && self.last_sps.is_some() && self.last_pps.is_some() {
                    let rule = rule.clone();
                    let (child, stdin, file_path, stderr_task) = self.spawn_ffmpeg(&rule).await?;
                    let mut new_state = State::Recording {
                        rule,
                        child,
                        stdin,
                        stop_at: None,
                        started_at: Instant::now(),
                        file_path,
                        stderr_task: Some(stderr_task),
                    };
                    // Write SPS/PPS + first IDR
                    self.write_param_sets_and_idr(&mut new_state, &nal).await?;
                    self.state = new_state;
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
            State::Recording { stop_at, .. } => stop_at.map(|t| Instant::now() >= t).unwrap_or(false),
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

        if let State::Recording { mut child, mut stdin, file_path, stderr_task, .. } =
            std::mem::replace(&mut self.state, State::Idle)
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
                        warn!(
                            status_code = status.code(),
                            file = ?file_path,
                            "FFmpeg exited with error"
                        );
                        if !stderr_txt.trim().is_empty() {
                            warn!(stderr = %stderr_txt, "FFmpeg stderr output");
                        }
                    } else {
                        info!(file = ?file_path, "Clip saved successfully");
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        error = %e,
                        file = ?file_path,
                        "FFmpeg process wait error"
                    );
                    if !stderr_txt.trim().is_empty() {
                        warn!(stderr = %stderr_txt, "FFmpeg stderr output");
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

        let sps = self.last_sps.as_ref().ok_or_else(|| anyhow!("missing SPS"))?;
        let pps = self.last_pps.as_ref().ok_or_else(|| anyhow!("missing PPS"))?;

        stdin.write_all(sps).await.context("write SPS")?;
        stdin.write_all(pps).await.context("write PPS")?;
        stdin.write_all(idr).await.context("write IDR")?;

        info!(rule = %rule, "Recording started");
        Ok(())
    }

    async fn spawn_ffmpeg(&self, rule: &str) -> Result<(Child, ChildStdin, PathBuf, JoinHandle<String>)> {
        let ts = Utc::now().format("%Y%m%d_%H%M%S%.3fZ").to_string();
        let safe_rule = rule.replace(['/', '\\', ' '], "_");
        let file_path = self.cfg.output_dir.join(format!("{ts}_{safe_rule}.mp4"));

        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-hide_banner")
            .arg("-loglevel").arg("error")
            .arg("-y")
            // input: raw H.264 Annex-B from stdin
            .arg("-fflags").arg("+genpts")
            .arg("-r").arg(self.cfg.assumed_fps.to_string())
            .arg("-f").arg("h264")
            .arg("-i").arg("pipe:0");

        if self.cfg.stream_copy {
            cmd.arg("-c:v").arg("copy");
        } else {
            cmd.arg("-c:v").arg("libx264")
                .arg("-preset").arg("veryfast")
                .arg("-tune").arg("zerolatency");
        }

        cmd.arg("-movflags").arg("+faststart")
            .arg(file_path.to_string_lossy().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            // IMPORTANT: keep stderr piped but DRAIN it to avoid deadlock.
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().context("spawn ffmpeg")?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow!("ffmpeg stdin missing"))?;
        let mut stderr = child.stderr.take().ok_or_else(|| anyhow!("ffmpeg stderr missing"))?;

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
}

fn nal_type_from_annexb(nal: &[u8]) -> Option<u8> {
    if nal.len() < 5 { return None; }
    if &nal[0..4] != [0,0,0,1].as_slice() { return None; }
    Some(nal[4] & 0x1F)
}
