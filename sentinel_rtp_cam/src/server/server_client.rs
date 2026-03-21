use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::{agent_config::ServerConfig, AgentConfig};
use crate::core::clip_recorder::ClipMeta;
use crate::event::MotionEvent;
use crate::onvif::{run_onvif_capability_probe, OnvifProbeCameraConfig};

const FIRMWARE_VERSION_PATH: &str = "/etc/sentinel_rtp_cam/firmware-version";
const FIRMWARE_BACKUP_BINARY_PATH: &str = "/usr/local/bin/sentinel_rtp_cam.prev";
const DEFAULT_FIRMWARE_UPDATER_CMD: &str = "/usr/local/bin/sentinel-firmware-update";

#[derive(Debug, Deserialize, Clone)]
struct FirmwareJobCommand {
    job_id: i64,
    action: String,
    target_version: Option<String>,
    #[allow(dead_code)]
    status: String,
    start_after_update: bool,
}

#[derive(Debug, Deserialize)]
struct FirmwareJobResponse {
    job: Option<FirmwareJobCommand>,
}

#[derive(Debug, Deserialize, Clone)]
struct OnvifProbeJobCommand {
    job_id: i64,
    action: String,
    camera_id: String,
}

#[derive(Debug, Deserialize)]
struct OnvifProbeJobResponse {
    job: Option<OnvifProbeJobCommand>,
}

#[derive(Debug, Clone, Default)]
struct FirmwareHeartbeatState {
    job_id: Option<i64>,
    current_version: Option<String>,
    target_version: Option<String>,
    status: Option<String>,
    error: Option<String>,
    can_rollback: Option<bool>,
    started_at: Option<String>,
    finished_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnvifProbeSummary {
    pub job_id: Option<i64>,
    pub status: String,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub last_reported_at: Option<String>,
}

impl Default for OnvifProbeSummary {
    fn default() -> Self {
        Self {
            job_id: None,
            status: "never_run".to_string(),
            error: None,
            started_at: None,
            finished_at: None,
            last_reported_at: None,
        }
    }
}

impl OnvifProbeSummary {
    fn as_json(&self) -> Value {
        json!({
            "job_id": self.job_id,
            "status": self.status,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "last_reported_at": self.last_reported_at,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OnvifProbeManager {
    known_cameras: Arc<HashSet<String>>,
    camera_configs: Arc<HashMap<String, OnvifProbeCameraConfig>>,
    summaries: Arc<RwLock<HashMap<String, OnvifProbeSummary>>>,
    in_flight: Arc<RwLock<HashSet<String>>>,
    agent_id: Option<String>,
}

impl OnvifProbeManager {
    pub fn new(
        known_camera_ids: Vec<String>,
        camera_configs: Vec<OnvifProbeCameraConfig>,
        agent_id: Option<String>,
    ) -> Self {
        let mut summaries = HashMap::new();
        let mut known_cameras = HashSet::new();
        for camera_id in known_camera_ids {
            summaries.insert(camera_id.clone(), OnvifProbeSummary::default());
            known_cameras.insert(camera_id);
        }

        let configs = camera_configs
            .into_iter()
            .map(|config| (config.camera_id.clone(), config))
            .collect();

        Self {
            known_cameras: Arc::new(known_cameras),
            camera_configs: Arc::new(configs),
            summaries: Arc::new(RwLock::new(summaries)),
            in_flight: Arc::new(RwLock::new(HashSet::new())),
            agent_id,
        }
    }

    pub async fn summary_for(&self, camera_id: &str) -> OnvifProbeSummary {
        self.summaries
            .read()
            .await
            .get(camera_id)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn poll_for_jobs(&self, client: &reqwest::Client, config: &ServerConfig) {
        let job = match fetch_onvif_probe_job_command(client, config).await {
            Ok(job) => job,
            Err(error) => {
                warn!(error = %error, "Failed to poll ONVIF probe job command");
                return;
            }
        };
        let Some(job) = job else {
            return;
        };

        let camera_id = job.camera_id.trim().to_string();
        if job.action != "probe" {
            let error = format!("Unsupported ONVIF probe action: {}", job.action);
            self.fail_job_immediately(client.clone(), config.clone(), job, error, None)
                .await;
            return;
        }
        if camera_id.is_empty() {
            self.fail_job_immediately(
                client.clone(),
                config.clone(),
                job,
                "Missing camera_id".to_string(),
                None,
            )
            .await;
            return;
        }

        let known = self.known_cameras.contains(&camera_id);
        let Some(camera_config) = self.camera_configs.get(&camera_id).cloned() else {
            let summary_id = known.then_some(camera_id.clone());
            let error = if known {
                "camera not configured for ONVIF".to_string()
            } else {
                format!("unknown camera_id: {}", camera_id)
            };
            self.fail_job_immediately(client.clone(), config.clone(), job, error, summary_id)
                .await;
            return;
        };

        if !self.try_mark_in_flight(&camera_id).await {
            self.fail_job_immediately(
                client.clone(),
                config.clone(),
                job,
                "busy".to_string(),
                Some(camera_id),
            )
            .await;
            return;
        }

        let started_at = Utc::now().to_rfc3339();
        self.set_running(&camera_id, job.job_id, started_at.clone())
            .await;

        let manager = self.clone();
        let client = client.clone();
        let config = config.clone();
        tokio::spawn(async move {
            manager
                .execute_probe_job(client, config, job, camera_config, started_at)
                .await;
        });
    }

    async fn execute_probe_job(
        &self,
        client: reqwest::Client,
        config: ServerConfig,
        job: OnvifProbeJobCommand,
        camera_config: OnvifProbeCameraConfig,
        started_at: String,
    ) {
        let camera_id = camera_config.camera_id.clone();
        let outcome = run_onvif_capability_probe(&camera_config).await;
        let finished_at = Utc::now().to_rfc3339();

        let (status, error, report) = match outcome {
            Ok(report) => ("succeeded".to_string(), None, Some(report)),
            Err(error) => ("failed".to_string(), Some(error.to_string()), None),
        };

        self.set_finished(
            &camera_id,
            job.job_id,
            status.clone(),
            error.clone(),
            started_at.clone(),
            finished_at.clone(),
        )
        .await;
        self.release_in_flight(&camera_id).await;

        let payload = json!({
            "job_id": job.job_id,
            "camera_id": camera_id,
            "agent_id": self.agent_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "error": error,
            "report": report,
        });

        retry_forever(
            || {
                let client = client.clone();
                let config = config.clone();
                let payload = payload.clone();
                async move { post_onvif_probe_report(&client, &config, &payload).await }
            },
            Duration::from_secs(config.retry_interval_secs),
            "post_onvif_probe_report",
        )
        .await;
        self.mark_reported(&camera_id).await;
    }

    async fn fail_job_immediately(
        &self,
        client: reqwest::Client,
        config: ServerConfig,
        job: OnvifProbeJobCommand,
        error: String,
        summary_camera_id: Option<String>,
    ) {
        let started_at = Utc::now().to_rfc3339();
        let finished_at = started_at.clone();
        if let Some(camera_id) = summary_camera_id.as_deref() {
            self.set_finished(
                camera_id,
                job.job_id,
                "failed".to_string(),
                Some(error.clone()),
                started_at.clone(),
                finished_at.clone(),
            )
            .await;
        }

        let payload = json!({
            "job_id": job.job_id,
            "camera_id": job.camera_id,
            "agent_id": self.agent_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": "failed",
            "error": error,
            "report": Value::Null,
        });

        let manager = self.clone();
        tokio::spawn(async move {
            retry_forever(
                || {
                    let client = client.clone();
                    let config = config.clone();
                    let payload = payload.clone();
                    async move { post_onvif_probe_report(&client, &config, &payload).await }
                },
                Duration::from_secs(config.retry_interval_secs),
                "post_onvif_probe_report",
            )
            .await;
            if let Some(camera_id) = summary_camera_id {
                manager.mark_reported(&camera_id).await;
            }
        });
    }

    async fn try_mark_in_flight(&self, camera_id: &str) -> bool {
        let mut in_flight = self.in_flight.write().await;
        in_flight.insert(camera_id.to_string())
    }

    async fn release_in_flight(&self, camera_id: &str) {
        self.in_flight.write().await.remove(camera_id);
    }

    async fn set_running(&self, camera_id: &str, job_id: i64, started_at: String) {
        let mut summaries = self.summaries.write().await;
        let last_reported_at = summaries
            .get(camera_id)
            .and_then(|summary| summary.last_reported_at.clone());
        summaries.insert(
            camera_id.to_string(),
            OnvifProbeSummary {
                job_id: Some(job_id),
                status: "running".to_string(),
                error: None,
                started_at: Some(started_at),
                finished_at: None,
                last_reported_at,
            },
        );
    }

    async fn set_finished(
        &self,
        camera_id: &str,
        job_id: i64,
        status: String,
        error: Option<String>,
        started_at: String,
        finished_at: String,
    ) {
        let mut summaries = self.summaries.write().await;
        let last_reported_at = summaries
            .get(camera_id)
            .and_then(|summary| summary.last_reported_at.clone());
        summaries.insert(
            camera_id.to_string(),
            OnvifProbeSummary {
                job_id: Some(job_id),
                status,
                error,
                started_at: Some(started_at),
                finished_at: Some(finished_at),
                last_reported_at,
            },
        );
    }

    async fn mark_reported(&self, camera_id: &str) {
        let mut summaries = self.summaries.write().await;
        if let Some(summary) = summaries.get_mut(camera_id) {
            summary.last_reported_at = Some(Utc::now().to_rfc3339());
        }
    }
}

impl FirmwareHeartbeatState {
    fn as_json(&self) -> Option<Value> {
        if self.job_id.is_none()
            && self.current_version.is_none()
            && self.target_version.is_none()
            && self.status.is_none()
            && self.error.is_none()
            && self.can_rollback.is_none()
            && self.started_at.is_none()
            && self.finished_at.is_none()
        {
            return None;
        }

        Some(json!({
            "job_id": self.job_id,
            "current_version": self.current_version,
            "target_version": self.target_version,
            "status": self.status,
            "error": self.error,
            "can_rollback": self.can_rollback,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }))
    }
}

fn read_installed_firmware_version_from_path(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_installed_firmware_version() -> Option<String> {
    read_installed_firmware_version_from_path(Path::new(FIRMWARE_VERSION_PATH)).or_else(|| {
        AgentConfig::runtime_var("SENTINEL_VERSION").filter(|value| !value.trim().is_empty())
    })
}

fn build_idle_firmware_state(
    current_version: Option<String>,
    can_rollback: bool,
) -> FirmwareHeartbeatState {
    if current_version.is_none() && !can_rollback {
        return FirmwareHeartbeatState::default();
    }

    FirmwareHeartbeatState {
        current_version: current_version.clone(),
        target_version: current_version,
        status: Some("idle".to_string()),
        can_rollback: Some(can_rollback),
        ..Default::default()
    }
}

#[cfg(test)]
fn baseline_firmware_state_from_paths(
    version_path: &Path,
    backup_path: &Path,
) -> FirmwareHeartbeatState {
    build_idle_firmware_state(
        read_installed_firmware_version_from_path(version_path),
        backup_path.exists(),
    )
}

fn baseline_firmware_state() -> FirmwareHeartbeatState {
    build_idle_firmware_state(
        read_installed_firmware_version(),
        Path::new(FIRMWARE_BACKUP_BINARY_PATH).exists(),
    )
}

async fn fetch_firmware_job_command(
    client: &reqwest::Client,
    config: &ServerConfig,
) -> Result<Option<FirmwareJobCommand>> {
    let base = config.base_url.trim_end_matches('/');
    for path in ["/api/agent/firmware/job", "/api/v1/agent/firmware/job"] {
        let url = format!("{base}{path}");
        let response = client
            .get(&url)
            .bearer_auth(&config.bearer_token)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            continue;
        }
        let response = response.error_for_status()?;
        let payload: FirmwareJobResponse = response.json().await?;
        return Ok(payload.job);
    }
    Ok(None)
}

fn pick_firmware_updater_cmd(
    runtime_override: Option<String>,
    env_override: Option<String>,
) -> String {
    runtime_override
        .filter(|value| !value.trim().is_empty())
        .or_else(|| env_override.filter(|value| !value.trim().is_empty()))
        .unwrap_or_else(|| DEFAULT_FIRMWARE_UPDATER_CMD.to_string())
}

fn firmware_updater_cmd() -> String {
    pick_firmware_updater_cmd(
        AgentConfig::runtime_var("FIRMWARE_UPDATER_CMD"),
        std::env::var("FIRMWARE_UPDATER_CMD").ok(),
    )
}

async fn execute_firmware_job(job: &FirmwareJobCommand) -> Result<()> {
    let updater_cmd = firmware_updater_cmd();
    if updater_cmd.contains(std::path::MAIN_SEPARATOR) && !Path::new(&updater_cmd).exists() {
        return Err(anyhow::anyhow!(
            "Firmware updater not found at {updater_cmd}; set FIRMWARE_UPDATER_CMD to the \
             installed updater path"
        ));
    }
    let mut cmd = Command::new(&updater_cmd);

    match job.action.as_str() {
        "update" => {
            let target = job
                .target_version
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("Missing target version for update job"))?;
            cmd.arg(target);
            if job.start_after_update {
                cmd.arg("--start");
            }
        }
        "rollback" => {
            cmd.arg("--rollback");
            if job.start_after_update {
                cmd.arg("--start");
            }
        }
        other => {
            return Err(anyhow::anyhow!("Unsupported firmware job action: {other}"));
        }
    }

    let output = timeout(Duration::from_secs(120), cmd.output())
        .await
        .map_err(|_| anyhow::anyhow!("Firmware updater timed out after 120 seconds"))?
        .map_err(|e| anyhow::anyhow!("Failed to execute firmware updater {updater_cmd}: {e}"))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        "no output".to_string()
    };
    Err(anyhow::anyhow!(
        "Firmware updater failed ({:?}): {}",
        output.status,
        detail
    ))
}

async fn maybe_process_firmware_job(
    client: &reqwest::Client,
    config: &ServerConfig,
    state: &mut FirmwareHeartbeatState,
) {
    let job = match fetch_firmware_job_command(client, config).await {
        Ok(job) => job,
        Err(e) => {
            warn!(error = %e, "Failed to poll firmware job command");
            return;
        }
    };

    let Some(job) = job else {
        return;
    };

    let running_status = if job.action == "rollback" {
        "rollback_running"
    } else {
        "installing"
    };
    state.job_id = Some(job.job_id);
    state.target_version = job.target_version.clone();
    state.status = Some(running_status.to_string());
    state.error = None;
    state.started_at = Some(Utc::now().to_rfc3339());
    state.finished_at = None;

    info!(
        job_id = job.job_id,
        action = %job.action,
        target_version = ?job.target_version,
        "Executing firmware job"
    );

    match execute_firmware_job(&job).await {
        Ok(_) => {
            let status = if job.action == "rollback" {
                "rollback_succeeded"
            } else {
                "succeeded"
            };
            state.status = Some(status.to_string());
            state.error = None;
            state.finished_at = Some(Utc::now().to_rfc3339());
            if job.action == "update" {
                state.current_version = job.target_version.clone();
                state.can_rollback = Some(true);
            } else {
                state.can_rollback = Some(false);
            }
            info!(job_id = job.job_id, status, "Firmware job completed");
        }
        Err(e) => {
            let status = if job.action == "rollback" {
                "rollback_failed"
            } else {
                "failed"
            };
            state.status = Some(status.to_string());
            state.error = Some(e.to_string());
            state.finished_at = Some(Utc::now().to_rfc3339());
            warn!(job_id = job.job_id, status, error = %e, "Firmware job failed");
        }
    }
}

async fn fetch_onvif_probe_job_command(
    client: &reqwest::Client,
    config: &ServerConfig,
) -> Result<Option<OnvifProbeJobCommand>> {
    let base = config.base_url.trim_end_matches('/');
    let payload = try_fallback_paths(
        &["/api/agent/onvif/job", "/api/v1/agent/onvif/job"],
        |path| {
            let url = format!("{base}{path}");
            async move {
                let response = client
                    .get(&url)
                    .bearer_auth(&config.bearer_token)
                    .send()
                    .await?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(FallbackOutcome::NotFound);
                }
                let response = response.error_for_status()?;
                let payload: OnvifProbeJobResponse = response.json().await?;
                Ok(FallbackOutcome::Success(payload))
            }
        },
    )
    .await?;
    Ok(payload.and_then(|payload| payload.job))
}

async fn post_onvif_probe_report(
    client: &reqwest::Client,
    config: &ServerConfig,
    payload: &Value,
) -> Result<()> {
    let base = config.base_url.trim_end_matches('/');
    let posted = try_fallback_paths(
        &["/api/agent/onvif/report", "/api/v1/agent/onvif/report"],
        |path| {
            let url = format!("{base}{path}");
            async move {
                let response = client
                    .post(&url)
                    .bearer_auth(&config.bearer_token)
                    .json(payload)
                    .send()
                    .await?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(FallbackOutcome::NotFound);
                }
                response.error_for_status()?;
                Ok(FallbackOutcome::Success(()))
            }
        },
    )
    .await?;
    if posted.is_some() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("No ONVIF probe report endpoint available"))
    }
}

enum FallbackOutcome<T> {
    NotFound,
    Success(T),
}

async fn try_fallback_paths<T, F, Fut>(paths: &[&str], mut attempt: F) -> Result<Option<T>>
where
    F: FnMut(&str) -> Fut,
    Fut: std::future::Future<Output = Result<FallbackOutcome<T>>>,
{
    for path in paths {
        match attempt(path).await? {
            FallbackOutcome::NotFound => continue,
            FallbackOutcome::Success(value) => return Ok(Some(value)),
        }
    }
    Ok(None)
}

/// Helper function that retries an async operation forever with exponential backoff
/// Useful for network operations that should never permanently fail
pub async fn retry_forever<F, Fut, T>(
    mut operation: F,
    retry_interval: Duration,
    operation_name: &str,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    info!(
                        operation = operation_name,
                        attempt = attempt,
                        "Operation succeeded after retries"
                    );
                }
                return result;
            }
            Err(e) => {
                let backoff = retry_interval * attempt.min(10);
                warn!(
                    operation = operation_name,
                    attempt = attempt,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "Operation failed, will retry"
                );
                sleep(backoff).await;
            }
        }
    }
}

/// Helper function that retries an async operation with a maximum attempt limit
/// Returns Ok(Some(T)) on success, Ok(None) if max retries exceeded, Err on unexpected failure
pub async fn retry_with_limit<F, Fut, T>(
    mut operation: F,
    retry_interval: Duration,
    max_attempts: u32,
    operation_name: &str,
) -> Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    info!(
                        operation = operation_name,
                        attempt = attempt,
                        "Operation succeeded after retries"
                    );
                }
                return Ok(Some(result));
            }
            Err(e) => {
                if attempt >= max_attempts {
                    error!(
                        operation = operation_name,
                        attempt = attempt,
                        error = %e,
                        "Operation failed after max retries, dropping message"
                    );
                    return Ok(None);
                }

                let backoff = retry_interval * attempt.min(10);
                warn!(
                    operation = operation_name,
                    attempt = attempt,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    remaining_attempts = max_attempts - attempt,
                    "Operation failed, will retry"
                );
                sleep(backoff).await;
            }
        }
    }
}

/// Posts motion events to the server
pub async fn run_motion_event_poster(
    mut event_rx: mpsc::Receiver<MotionEvent>,
    config: ServerConfig,
) -> Result<()> {
    info!(
        server_url = %config.base_url,
        max_retries = config.max_retries,
        "Starting motion event poster"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    while let Some(event) = event_rx.recv().await {
        let url = format!("{}/api/events/motion", config.base_url);
        let payload = json!({
            "event_id": event.event_id,
            "active": event.active,
            "rule": event.rule,
            "ts": event.ts.to_rfc3339(),
        });

        let config_clone = config.clone();
        let client_clone = client.clone();

        // Post in background with retry limit - don't block event processing
        // If max_retries is 0, use retry_forever for infinite retries (backward compatible)
        tokio::spawn(async move {
            if config_clone.max_retries == 0 {
                retry_forever(
                    || async {
                        client_clone
                            .post(&url)
                            .bearer_auth(&config_clone.bearer_token)
                            .json(&payload)
                            .send()
                            .await?
                            .error_for_status()?;
                        Ok(())
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    "post_motion_event",
                )
                .await
            } else {
                let _ = retry_with_limit(
                    || async {
                        client_clone
                            .post(&url)
                            .bearer_auth(&config_clone.bearer_token)
                            .json(&payload)
                            .send()
                            .await?
                            .error_for_status()?;
                        Ok(())
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    config_clone.max_retries,
                    "post_motion_event",
                )
                .await;
            }
        });
    }

    Ok(())
}

/// Posts clip metadata to the server when recordings complete
pub async fn run_clip_meta_poster(
    mut clip_rx: mpsc::Receiver<ClipMeta>,
    config: ServerConfig,
) -> Result<()> {
    info!(
        server_url = %config.base_url,
        max_retries = config.max_retries,
        "Starting clip metadata poster"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30)) // Increased for file uploads
        .build()?;

    while let Some(clip) = clip_rx.recv().await {
        let metadata_url = format!("{}/api/clips/metadata", config.base_url);

        // Extract just the filename (not the full path)
        let filename = clip
            .file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown.h264")
            .to_string();

        let payload = json!({
            "event_id": clip.event_id,
            "filename": format!("clips/{}", filename),
            "size_bytes": clip.file_size as i64,
        });

        let config_clone = config.clone();
        let client_clone = client.clone();
        let file_path = clip.file_path.clone();

        // Post metadata and upload file in background with retry
        tokio::spawn(async move {
            // First post metadata
            let metadata_success = if config_clone.max_retries == 0 {
                retry_forever(
                    || async {
                        client_clone
                            .post(&metadata_url)
                            .bearer_auth(&config_clone.bearer_token)
                            .json(&payload)
                            .send()
                            .await?
                            .error_for_status()?;
                        Ok(())
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    "post_clip_meta",
                )
                .await;
                true
            } else {
                match retry_with_limit(
                    || async {
                        client_clone
                            .post(&metadata_url)
                            .bearer_auth(&config_clone.bearer_token)
                            .json(&payload)
                            .send()
                            .await?
                            .error_for_status()?;
                        Ok(())
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    config_clone.max_retries,
                    "post_clip_meta",
                )
                .await
                {
                    Ok(Some(_)) => true,
                    _ => false,
                }
            };

            if !metadata_success {
                error!("Failed to post clip metadata after retries, skipping file upload");
                return;
            }

            // Then upload the file
            let upload_url = format!("{}/api/clips/upload", config_clone.base_url);

            let upload_success = if config_clone.max_retries == 0 {
                retry_forever(
                    || async {
                        upload_clip_file(
                            &client_clone,
                            &upload_url,
                            &config_clone.bearer_token,
                            &file_path,
                        )
                        .await
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    "upload_clip_file",
                )
                .await;
                true
            } else {
                match retry_with_limit(
                    || async {
                        upload_clip_file(
                            &client_clone,
                            &upload_url,
                            &config_clone.bearer_token,
                            &file_path,
                        )
                        .await
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    config_clone.max_retries,
                    "upload_clip_file",
                )
                .await
                {
                    Ok(Some(_)) => true,
                    _ => false,
                }
            };

            if !upload_success {
                error!(file_path = %file_path.display(), "Failed to upload clip file after retries");
            } else {
                info!(file_path = %file_path.display(), "Successfully uploaded clip file");
            }
        });
    }

    Ok(())
}

/// Helper function to upload a clip file via multipart form
async fn upload_clip_file(
    client: &reqwest::Client,
    url: &str,
    bearer_token: &str,
    file_path: &std::path::Path,
) -> Result<()> {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    // Read file content
    let mut file = File::open(file_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open clip file: {}", e))?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read clip file: {}", e))?;

    // Get filename
    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;

    // Create multipart form
    let part = reqwest::multipart::Part::bytes(buffer)
        .file_name(filename.to_string())
        .mime_str("video/h264")?;

    let form = reqwest::multipart::Form::new().part("file", part);

    // Send request
    client
        .post(url)
        .bearer_auth(bearer_token)
        .multipart(form)
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

/// Sends periodic heartbeat to server
pub async fn run_heartbeat_poster(
    config: ServerConfig,
    camera_id: String,
    onvif_probe: OnvifProbeManager,
) -> Result<()> {
    info!(
        server_url = %config.base_url,
        camera_id = %camera_id,
        "Starting heartbeat poster"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let url = format!("{}/api/heartbeat", config.base_url);
    let heartbeat_interval = Duration::from_secs(30);
    loop {
        sleep(heartbeat_interval).await;
        let mut firmware = baseline_firmware_state();
        maybe_process_firmware_job(&client, &config, &mut firmware).await;
        onvif_probe.poll_for_jobs(&client, &config).await;
        let onvif_summary = onvif_probe.summary_for(&camera_id).await;

        let mut payload = json!({
            "camera_id": camera_id,
            "timestamp": Utc::now().to_rfc3339(),
            "onvif_probe": onvif_summary.as_json(),
        });
        if let Some(firmware_payload) = firmware.as_json() {
            payload["firmware"] = firmware_payload;
        }

        let config_clone = config.clone();
        let client_clone = client.clone();
        let url_clone = url.clone();
        let payload_clone = payload.clone();

        // Post in background with retry_forever
        tokio::spawn(async move {
            retry_forever(
                || async {
                    client_clone
                        .post(&url_clone)
                        .bearer_auth(&config_clone.bearer_token)
                        .json(&payload_clone)
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(())
                },
                Duration::from_secs(config_clone.retry_interval_secs),
                "post_heartbeat",
            )
            .await
        });
    }
}

#[derive(Debug, Clone)]
pub struct CameraHeartbeatTarget {
    pub camera_id: String,
    pub rtsp_url: String,
}

async fn check_rtsp_reachability(rtsp_url: &str) -> (bool, Option<i64>, Option<String>) {
    let started = std::time::Instant::now();
    let parsed = match Url::parse(rtsp_url) {
        Ok(url) => url,
        Err(e) => return (false, None, Some(format!("Invalid RTSP URL: {e}"))),
    };
    let host = match parsed.host_str() {
        Some(h) => h,
        None => return (false, None, Some("RTSP URL missing host".to_string())),
    };
    let port = parsed.port().unwrap_or(554);
    let addr = format!("{host}:{port}");

    match timeout(Duration::from_secs(3), TcpStream::connect(&addr)).await {
        Ok(Ok(_)) => {
            let ms = started.elapsed().as_millis() as i64;
            (true, Some(ms), None)
        }
        Ok(Err(e)) => (false, None, Some(e.to_string())),
        Err(_) => (false, None, Some("connect timeout".to_string())),
    }
}

/// Sends periodic heartbeat to server using agent token (includes per-camera RTSP status if available)
pub async fn run_agent_heartbeat_poster(
    config: ServerConfig,
    agent_id: String,
    cameras: Arc<RwLock<Vec<CameraHeartbeatTarget>>>,
    onvif_probe: OnvifProbeManager,
) -> Result<()> {
    info!(
        server_url = %config.base_url,
        agent_id = %agent_id,
        "Starting agent heartbeat poster"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let url = format!("{}/api/heartbeat", config.base_url);
    let heartbeat_interval = Duration::from_secs(30);
    loop {
        sleep(heartbeat_interval).await;
        let mut firmware = baseline_firmware_state();
        maybe_process_firmware_job(&client, &config, &mut firmware).await;
        onvif_probe.poll_for_jobs(&client, &config).await;

        let camera_snapshot = cameras.read().await.clone();
        let mut camera_status = Vec::new();
        for cam in camera_snapshot {
            let (ok, latency_ms, error) = check_rtsp_reachability(&cam.rtsp_url).await;
            let onvif_summary = onvif_probe.summary_for(&cam.camera_id).await;
            camera_status.push(json!({
                "camera_id": cam.camera_id,
                "rtsp_ok": ok,
                "rtsp_latency_ms": latency_ms,
                "rtsp_error": error,
                "onvif_probe": onvif_summary.as_json(),
            }));
        }

        let mut payload = json!({
            "agent_id": agent_id,
            "timestamp": Utc::now().to_rfc3339(),
            "cameras": camera_status,
        });
        if let Some(firmware_payload) = firmware.as_json() {
            payload["firmware"] = firmware_payload;
        }

        let config_clone = config.clone();
        let client_clone = client.clone();
        let url_clone = url.clone();
        let payload_clone = payload.clone();

        tokio::spawn(async move {
            retry_forever(
                || async {
                    client_clone
                        .post(&url_clone)
                        .bearer_auth(&config_clone.bearer_token)
                        .json(&payload_clone)
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(())
                },
                Duration::from_secs(config_clone.retry_interval_secs),
                "post_agent_heartbeat",
            )
            .await
        });
    }
}

/// Listens to Server-Sent Events (SSE) for configuration updates
/// Returns a watch channel that broadcasts config changes
pub async fn run_sse_config_listener(
    config: ServerConfig,
    camera_id: String,
    stream_id: Option<u32>,
) -> Result<tokio::sync::watch::Receiver<serde_json::Value>> {
    info!(
        server_url = %config.base_url,
        camera_id = %camera_id,
        "Starting SSE config listener"
    );

    let (tx, rx) = tokio::sync::watch::channel(json!({}));

    let url = format!(
        "{}/api/v1/config/stream?camera_id={}",
        config.base_url, camera_id
    );

    tokio::spawn(async move {
        retry_forever(
            || async {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(300)) // Long timeout for SSE
                    .build()?;

                info!("Connecting to SSE endpoint: {}", url);

                let response = client
                    .get(&url)
                    .bearer_auth(&config.bearer_token)
                    .send()
                    .await?
                    .error_for_status()?;

                let mut stream = response.bytes_stream();
                use futures::StreamExt;

                let mut buffer = String::new();

                while let Some(chunk_result) = stream.next().await {
                    let chunk = chunk_result?;
                    let text = String::from_utf8_lossy(&chunk);
                    buffer.push_str(&text);

                    // Process complete lines (SSE messages end with \n\n)
                    while let Some(pos) = buffer.find('\n') {
                        let line = buffer[..pos].to_string();
                        buffer.drain(..=pos);

                        // Parse SSE format: "data: {...}"
                        if let Some(data) = line.strip_prefix("data:") {
                            let trimmed = data.trim();
                            if trimmed.is_empty() || trimmed == "keepalive" {
                                continue;
                            }
                            if let Ok(config_update) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                let normalized =
                                    normalize_config_update(config_update, &camera_id, stream_id);
                                info!(
                                    config = %serde_json::to_string(&normalized).unwrap_or_default(),
                                    "Received config update from SSE stream"
                                );
                                let _ = tx.send(normalized);
                            } else {
                                debug!("Failed to parse SSE data as JSON: {}", trimmed);
                            }
                        } else if line.starts_with("event:") {
                            debug!("SSE event type: {}", line);
                        } else if !line.trim().is_empty() && !line.starts_with(':') {
                            debug!("Unknown SSE line: {}", line);
                        }
                    }
                }

                Ok(())
            },
            Duration::from_secs(config.retry_interval_secs),
            "sse_config_listener",
        )
        .await
    });

    Ok(rx)
}

fn normalize_config_update(value: Value, camera_id: &str, stream_id: Option<u32>) -> Value {
    let value = unwrap_sse_payload(value);

    if let Some(cameras) = value.get("cameras").and_then(|v| v.as_array()) {
        let selected = select_camera(cameras, camera_id, stream_id);
        let mut out = serde_json::Map::new();
        out.insert("cameras".to_string(), Value::Array(vec![selected]));
        for key in [
            "server",
            "cleanup",
            "ingest",
            "forward_agent",
            "logging",
            "version",
        ] {
            if let Some(v) = value.get(key) {
                out.insert(key.to_string(), v.clone());
            }
        }
        return Value::Object(out);
    }

    normalize_legacy_config(value, camera_id, stream_id)
}

fn unwrap_sse_payload(value: Value) -> Value {
    let Some(config) = value.get("config") else {
        return value;
    };
    if !config.is_object() {
        return value;
    }

    let mut unwrapped = config.clone();
    if let Some(obj) = unwrapped.as_object_mut() {
        if !obj.contains_key("camera_id") {
            if let Some(camera_id) = value.get("camera_id").and_then(|v| v.as_str()) {
                if !camera_id.trim().is_empty() {
                    obj.insert(
                        "camera_id".to_string(),
                        Value::String(camera_id.to_string()),
                    );
                }
            }
        }
    }
    unwrapped
}

fn select_camera(cameras: &[Value], camera_id: &str, stream_id: Option<u32>) -> Value {
    if !camera_id.trim().is_empty() {
        if let Some(cam) = cameras.iter().find(|cam| {
            cam.get("id")
                .and_then(|v| v.as_str())
                .map(|id| id == camera_id)
                .unwrap_or(false)
                || cam
                    .get("camera_id")
                    .and_then(|v| v.as_str())
                    .map(|id| id == camera_id)
                    .unwrap_or(false)
        }) {
            return cam.clone();
        }
    }

    if let Some(stream_id) = stream_id {
        if let Some(cam) = cameras.iter().find(|cam| {
            cam.get("stream_id")
                .and_then(|v| v.as_u64())
                .or_else(|| {
                    cam.get("rtsp")
                        .and_then(|rtsp| rtsp.get("stream_id"))
                        .and_then(|v| v.as_u64())
                })
                .map(|id| id == stream_id as u64)
                .unwrap_or(false)
        }) {
            return cam.clone();
        }
    }

    cameras.first().cloned().unwrap_or_else(|| json!({}))
}

fn normalize_legacy_config(value: Value, camera_id: &str, stream_id: Option<u32>) -> Value {
    let mut out = serde_json::Map::new();

    let id = first_string(&value, &["camera_id", "id", "device_id", "deviceId"]).or_else(|| {
        let trimmed = camera_id.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let user = first_string(&value, &["user", "rtsp_user", "onvif_user"]);
    let pass = first_string(&value, &["pass", "rtsp_pass", "onvif_pass"]);
    let transport = first_string(&value, &["transport"]).or_else(|| {
        value
            .get("rtsp")
            .and_then(|r| r.get("transport"))
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    });
    let stream_id = first_u64(&value, &["stream_id"])
        .or_else(|| {
            value
                .get("rtsp")
                .and_then(|r| r.get("stream_id"))
                .and_then(|v| v.as_u64())
        })
        .or(stream_id.map(|v| v as u64));

    let motion_enabled = first_bool(&value, &["motion_enabled"]).or_else(|| {
        value
            .get("motion")
            .and_then(|m| m.get("enabled"))
            .and_then(|v| v.as_bool())
    });
    let local_clip_enabled = first_bool(&value, &["local_clip_enabled"]);
    let rtsp_receiver_enabled = first_bool(&value, &["rtsp_receiver_enabled"]);

    let mut rtsp = if let Some(rtsp) = value.get("rtsp") {
        if rtsp.is_object() {
            rtsp.clone()
        } else if let Some(url) = rtsp.as_str() {
            json!({ "url": url })
        } else {
            json!({})
        }
    } else {
        json!({
            "url": first_string(&value, &["rtsp_url"]),
            "host": first_string(&value, &["rtsp_host"]),
            "port": first_u64(&value, &["rtsp_port"]),
            "path": first_string(&value, &["rtsp_path"]),
        })
    };
    if let Some(rtsp_obj) = rtsp.as_object_mut() {
        if !rtsp_obj.contains_key("stream_id") {
            if let Some(stream_id) = stream_id {
                rtsp_obj.insert("stream_id".to_string(), json!(stream_id));
            }
        }
        rtsp_obj.remove("host");
        rtsp_obj.remove("port");
        rtsp_obj.remove("path");
    }

    let mut onvif = if let Some(onvif) = value.get("onvif") {
        if onvif.is_object() {
            onvif.clone()
        } else {
            json!({})
        }
    } else {
        json!({
            "host": first_string(&value, &["onvif_host"]),
            "port": first_u64(&value, &["onvif_port"]),
            "debug": first_bool(&value, &["onvif_debug"]),
            "dump_xml": first_bool(&value, &["onvif_dump_xml"]),
            "sub_termination": first_string(&value, &["onvif_sub_termination"]),
            "renew_every_secs": first_u64(&value, &["onvif_renew_every_secs"]),
            "pull_timeout": first_string(&value, &["onvif_pull_timeout"]),
            "pull_limit": first_u64(&value, &["onvif_pull_limit"]),
            "resubscribe_after_errors": first_u64(&value, &["onvif_resubscribe_after_errors"]),
            "min_poll_gap_ms": first_u64(&value, &["onvif_min_poll_gap_ms"]),
            "after_sub_delay_ms": first_u64(&value, &["onvif_after_sub_delay_ms"]),
            "connrefused_retries": first_u64(&value, &["onvif_connrefused_retries"]),
            "connrefused_backoff_ms": first_u64(&value, &["onvif_connrefused_backoff_ms"]),
        })
    };
    if let Some(onvif_obj) = onvif.as_object_mut() {
        if !onvif_obj.contains_key("url") {
            if let Some(url) = build_onvif_url(
                onvif_obj.get("host").and_then(|v| v.as_str()),
                onvif_obj.get("port").and_then(|v| v.as_u64()),
            ) {
                onvif_obj.insert("url".to_string(), Value::String(url));
            }
        }
        onvif_obj.remove("host");
        onvif_obj.remove("port");
    }

    let camera = json!({
        "id": id,
        "user": user,
        "pass": pass,
        "transport": transport,
        "motion": { "enabled": motion_enabled },
        "features": {
            "local_clip_enabled": local_clip_enabled,
            "rtsp_receiver_enabled": rtsp_receiver_enabled,
        },
        "rtsp": rtsp,
        "onvif": onvif,
    });

    out.insert("cameras".to_string(), Value::Array(vec![camera]));
    for key in [
        "server",
        "cleanup",
        "ingest",
        "forward_agent",
        "logging",
        "version",
    ] {
        if let Some(v) = value.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }

    Value::Object(out)
}

fn build_onvif_url(host: Option<&str>, port: Option<u64>) -> Option<String> {
    let host = host?.trim();
    if host.is_empty() {
        return None;
    }
    Some(format!(
        "http://{}:{}/onvif/service",
        host,
        port.unwrap_or(2020)
    ))
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(s) = v.as_str() {
                if !s.trim().is_empty() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(|v| v.as_u64()) {
            return Some(v);
        }
    }
    None
}

fn first_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(|v| v.as_bool()) {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        baseline_firmware_state_from_paths, normalize_config_update, pick_firmware_updater_cmd,
        try_fallback_paths, FallbackOutcome, OnvifProbeJobCommand, OnvifProbeJobResponse,
        OnvifProbeManager, DEFAULT_FIRMWARE_UPDATER_CMD,
    };
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sentinel-server-client-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn normalize_config_selects_camera_from_list() {
        let input = json!({
            "cameras": [
                { "id": "cam-1", "stream_id": 1 },
                { "id": "cam-2", "stream_id": 2 }
            ],
            "server": { "enabled": true }
        });

        let out = normalize_config_update(input, "cam-2", None);
        let cameras = out["cameras"].as_array().unwrap();
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0]["id"], "cam-2");
        assert!(out.get("server").is_some());
    }

    #[test]
    fn normalize_config_falls_back_to_legacy_fields() {
        let input = json!({
            "camera_id": "legacy-cam",
            "rtsp_url": "rtsp://10.0.0.1/stream",
            "onvif_host": "10.0.0.2",
            "onvif_port": 2020,
            "motion_enabled": false
        });

        let out = normalize_config_update(input, "legacy-cam", Some(7));
        let camera = &out["cameras"][0];
        assert_eq!(camera["id"], "legacy-cam");
        assert_eq!(camera["rtsp"]["url"], "rtsp://10.0.0.1/stream");
        assert_eq!(camera["rtsp"]["stream_id"], 7);
        assert_eq!(camera["onvif"]["url"], "http://10.0.0.2:2020/onvif/service");
        assert_eq!(camera["motion"]["enabled"], false);
    }

    #[test]
    fn normalize_config_unwraps_sse_envelope() {
        let input = json!({
            "camera_id": "cam-2",
            "version": 3,
            "config": {
                "cameras": [
                    { "id": "cam-1", "rtsp": { "url": "rtsp://10.0.0.1/live", "stream_id": 1 } },
                    { "id": "cam-2", "rtsp": { "url": "rtsp://10.0.0.2/live", "stream_id": 2 } }
                ],
                "cleanup": { "min_free_bytes": 1234 }
            }
        });

        let out = normalize_config_update(input, "cam-2", None);
        let cameras = out["cameras"].as_array().unwrap();
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0]["id"], "cam-2");
        assert_eq!(out["cleanup"]["min_free_bytes"], 1234);
    }

    #[test]
    fn normalize_legacy_config_unwraps_sse_envelope() {
        let input = json!({
            "camera_id": "legacy-cam",
            "version": 2,
            "config": {
                "rtsp_url": "rtsp://10.0.0.1/stream",
                "onvif_host": "10.0.0.2",
                "onvif_port": 2020,
                "motion_enabled": true
            }
        });

        let out = normalize_config_update(input, "legacy-cam", Some(7));
        let camera = &out["cameras"][0];
        assert_eq!(camera["id"], "legacy-cam");
        assert_eq!(camera["rtsp"]["url"], "rtsp://10.0.0.1/stream");
        assert_eq!(camera["rtsp"]["stream_id"], 7);
        assert_eq!(camera["onvif"]["url"], "http://10.0.0.2:2020/onvif/service");
        assert_eq!(camera["motion"]["enabled"], true);
    }

    #[test]
    fn baseline_firmware_state_reports_idle_version_and_rollback_capability() {
        let dir = unique_test_dir("firmware-state");
        fs::create_dir_all(&dir).expect("temp dir");
        let version_path = dir.join("firmware-version");
        let backup_path = dir.join("sentinel_rtp_cam.prev");
        fs::write(&version_path, "0.6.4\n").expect("version file");
        fs::write(&backup_path, "").expect("backup file");

        let state = baseline_firmware_state_from_paths(&version_path, &backup_path);

        assert_eq!(state.current_version.as_deref(), Some("0.6.4"));
        assert_eq!(state.target_version.as_deref(), Some("0.6.4"));
        assert_eq!(state.status.as_deref(), Some("idle"));
        assert_eq!(state.can_rollback, Some(true));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn baseline_firmware_state_is_empty_without_local_firmware_metadata() {
        let dir = unique_test_dir("firmware-empty");
        fs::create_dir_all(&dir).expect("temp dir");
        let version_path = dir.join("firmware-version");
        let backup_path = dir.join("sentinel_rtp_cam.prev");

        let state = baseline_firmware_state_from_paths(&version_path, &backup_path);

        assert!(state.as_json().is_none());

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn firmware_updater_command_prefers_runtime_then_env_then_default() {
        assert_eq!(
            pick_firmware_updater_cmd(
                Some("/runtime/updater".to_string()),
                Some("/env/updater".to_string()),
            ),
            "/runtime/updater"
        );
        assert_eq!(
            pick_firmware_updater_cmd(Some("   ".to_string()), Some("/env/updater".to_string())),
            "/env/updater"
        );
        assert_eq!(
            pick_firmware_updater_cmd(None, Some("   ".to_string())),
            DEFAULT_FIRMWARE_UPDATER_CMD
        );
    }

    #[tokio::test]
    async fn fetch_onvif_probe_job_falls_back_to_versioned_endpoint() {
        let payload = try_fallback_paths(
            &["/api/agent/onvif/job", "/api/v1/agent/onvif/job"],
            |path| {
                let path = path.to_string();
                async move {
                    if path == "/api/agent/onvif/job" {
                        Ok(FallbackOutcome::NotFound)
                    } else {
                        Ok(FallbackOutcome::Success(OnvifProbeJobResponse {
                            job: Some(OnvifProbeJobCommand {
                                job_id: 42,
                                action: "probe".to_string(),
                                camera_id: "cam-1".to_string(),
                            }),
                        }))
                    }
                }
            },
        )
        .await
        .expect("fallback request")
        .expect("payload exists");
        let job = payload.job.expect("job exists");

        assert_eq!(job.job_id, 42);
        assert_eq!(job.action, "probe");
        assert_eq!(job.camera_id, "cam-1");
    }

    #[tokio::test]
    async fn post_onvif_probe_report_falls_back_to_versioned_endpoint() {
        let attempts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let attempts_for_closure = attempts.clone();
        let posted = try_fallback_paths(
            &["/api/agent/onvif/report", "/api/v1/agent/onvif/report"],
            move |path| {
                let attempts = attempts_for_closure.clone();
                let path = path.to_string();
                async move {
                    attempts.lock().expect("attempt log").push(path.clone());
                    if path == "/api/agent/onvif/report" {
                        Ok(FallbackOutcome::NotFound)
                    } else {
                        Ok(FallbackOutcome::Success(()))
                    }
                }
            },
        )
        .await
        .expect("fallback post");
        assert!(posted.is_some());

        let attempts = attempts.lock().expect("attempt log");
        assert_eq!(
            *attempts,
            vec![
                "/api/agent/onvif/report".to_string(),
                "/api/v1/agent/onvif/report".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn onvif_probe_manager_tracks_summary_transitions() {
        let manager = OnvifProbeManager::new(vec!["cam-1".to_string()], Vec::new(), None);

        let initial = manager.summary_for("cam-1").await;
        assert_eq!(initial.status, "never_run");
        assert!(initial.started_at.is_none());

        manager
            .set_running("cam-1", 7, "2026-03-21T10:00:00Z".to_string())
            .await;
        let running = manager.summary_for("cam-1").await;
        assert_eq!(running.job_id, Some(7));
        assert_eq!(running.status, "running");
        assert_eq!(running.started_at.as_deref(), Some("2026-03-21T10:00:00Z"));
        assert!(running.finished_at.is_none());

        manager
            .set_finished(
                "cam-1",
                7,
                "succeeded".to_string(),
                None,
                "2026-03-21T10:00:00Z".to_string(),
                "2026-03-21T10:00:02Z".to_string(),
            )
            .await;
        manager.mark_reported("cam-1").await;

        let finished = manager.summary_for("cam-1").await;
        assert_eq!(finished.status, "succeeded");
        assert_eq!(
            finished.finished_at.as_deref(),
            Some("2026-03-21T10:00:02Z")
        );
        assert!(finished.last_reported_at.is_some());
    }
}
