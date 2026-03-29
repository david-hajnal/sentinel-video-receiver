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
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout, Duration};
use tracing::{info, warn};
use url::Url;

use crate::config::{agent_config::ServerConfig, AgentConfig};
use crate::onvif::{run_onvif_capability_probe, OnvifProbeCameraConfig};

const FIRMWARE_VERSION_PATH: &str = "/etc/sentinel_rtp_cam/firmware-version";
const FIRMWARE_BACKUP_BINARY_PATH: &str = "/usr/local/bin/sentinel-agent.prev";
const LEGACY_FIRMWARE_BACKUP_BINARY_PATH_ALT: &str = "/usr/local/bin/agent_forward.prev";
const LEGACY_FIRMWARE_BACKUP_BINARY_PATH: &str = "/usr/local/bin/sentinel_rtp_cam.prev";
const DEFAULT_FIRMWARE_UPDATER_CMD: &str = "/usr/local/bin/sentinel-firmware-update";
const PROC_STAT_PATH: &str = "/proc/stat";
const PROC_MEMINFO_PATH: &str = "/proc/meminfo";

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

#[derive(Debug, Default)]
struct FirmwareJobMemory {
    last_terminal_state: Option<FirmwareHeartbeatState>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TelemetryStatus {
    Operational,
    Degraded,
    Suspended,
}

impl TelemetryStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Operational => "operational",
            Self::Degraded => "degraded",
            Self::Suspended => "suspended",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TelemetryCapabilities {
    face_recognition: bool,
    license_plate: bool,
    object_tracking: bool,
    anomalous_motion: bool,
}

impl Default for TelemetryCapabilities {
    fn default() -> Self {
        Self {
            face_recognition: false,
            license_plate: false,
            object_tracking: false,
            anomalous_motion: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeartbeatTelemetry {
    status: String,
    cpu_load_pct: Option<u64>,
    memory_used_mb: Option<u64>,
    memory_total_mb: Option<u64>,
    active_models: Vec<String>,
    capabilities: TelemetryCapabilities,
}

impl HeartbeatTelemetry {
    fn as_json(&self) -> Value {
        json!({
            "status": self.status,
            "cpu_load_pct": self.cpu_load_pct,
            "memory_used_mb": self.memory_used_mb,
            "memory_total_mb": self.memory_total_mb,
            "active_models": self.active_models,
            "capabilities": {
                "face_recognition": self.capabilities.face_recognition,
                "license_plate": self.capabilities.license_plate,
                "object_tracking": self.capabilities.object_tracking,
                "anomalous_motion": self.capabilities.anomalous_motion,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryUsageMb {
    used_mb: u64,
    total_mb: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CpuSample {
    idle: u64,
    total: u64,
}

#[derive(Debug, Default)]
struct TelemetrySampler {
    last_cpu_sample: Option<CpuSample>,
}

impl TelemetrySampler {
    fn new() -> Self {
        Self {
            last_cpu_sample: read_cpu_sample_from_path(Path::new(PROC_STAT_PATH)),
        }
    }

    fn sample(&mut self, status: TelemetryStatus) -> HeartbeatTelemetry {
        build_heartbeat_telemetry(
            status,
            sample_cpu_load_pct(&mut self.last_cpu_sample, Path::new(PROC_STAT_PATH)),
            read_memory_usage_mb_from_path(Path::new(PROC_MEMINFO_PATH)),
            collect_active_models(),
            detect_telemetry_capabilities(),
        )
    }
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
    fn is_terminal(&self) -> bool {
        is_terminal_firmware_status(self.status.as_deref())
    }

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

impl FirmwareJobMemory {
    fn duplicate_terminal_state(
        &self,
        job_id: i64,
        current_version: Option<String>,
        can_rollback: bool,
    ) -> Option<FirmwareHeartbeatState> {
        let mut state = self.last_terminal_state.clone()?;
        if state.job_id != Some(job_id) || !state.is_terminal() {
            return None;
        }

        state.current_version = current_version.or(state.current_version.clone());
        state.can_rollback = Some(can_rollback);
        Some(state)
    }

    fn remember_terminal_state(&mut self, state: &FirmwareHeartbeatState) {
        if state.is_terminal() {
            self.last_terminal_state = Some(state.clone());
        }
    }
}

fn sanitize_active_models<I>(models: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for model in models {
        let trimmed = model.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            out.push(trimmed.to_string());
        }
    }
    out
}

fn collect_active_models() -> Vec<String> {
    sanitize_active_models(Vec::<String>::new())
}

fn detect_telemetry_capabilities() -> TelemetryCapabilities {
    TelemetryCapabilities::default()
}

fn build_heartbeat_telemetry(
    status: TelemetryStatus,
    cpu_load_pct: Option<u64>,
    memory: Option<MemoryUsageMb>,
    active_models: Vec<String>,
    capabilities: TelemetryCapabilities,
) -> HeartbeatTelemetry {
    HeartbeatTelemetry {
        status: status.as_str().to_string(),
        cpu_load_pct: cpu_load_pct.map(|value| value.min(100)),
        memory_used_mb: memory.map(|value| value.used_mb),
        memory_total_mb: memory.map(|value| value.total_mb),
        active_models: sanitize_active_models(active_models),
        capabilities,
    }
}

fn parse_meminfo_value_kb(raw: &str, key: &str) -> Option<u64> {
    raw.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

fn read_memory_usage_mb_from_path(path: &Path) -> Option<MemoryUsageMb> {
    let raw = fs::read_to_string(path).ok()?;
    let total_kb = parse_meminfo_value_kb(&raw, "MemTotal:")?;
    let available_kb = parse_meminfo_value_kb(&raw, "MemAvailable:").or_else(|| {
        Some(
            parse_meminfo_value_kb(&raw, "MemFree:")?
                + parse_meminfo_value_kb(&raw, "Buffers:")?
                + parse_meminfo_value_kb(&raw, "Cached:")?,
        )
    })?;
    let available_kb = available_kb.min(total_kb);
    Some(MemoryUsageMb {
        used_mb: total_kb.saturating_sub(available_kb) / 1024,
        total_mb: total_kb / 1024,
    })
}

fn read_cpu_sample_from_path(path: &Path) -> Option<CpuSample> {
    let raw = fs::read_to_string(path).ok()?;
    let line = raw.lines().find(|line| line.starts_with("cpu "))?;
    let values = line
        .split_whitespace()
        .skip(1)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if values.len() < 4 {
        return None;
    }
    let idle = values[3] + values.get(4).copied().unwrap_or(0);
    let total = values.iter().copied().sum();
    Some(CpuSample { idle, total })
}

fn sample_cpu_load_pct(last_sample: &mut Option<CpuSample>, path: &Path) -> Option<u64> {
    let current = read_cpu_sample_from_path(path)?;
    let pct = last_sample.and_then(|previous| {
        let total_delta = current.total.saturating_sub(previous.total);
        if total_delta == 0 {
            return None;
        }
        let idle_delta = current.idle.saturating_sub(previous.idle);
        let busy_delta = total_delta.saturating_sub(idle_delta);
        Some((busy_delta.saturating_mul(100) / total_delta).min(100))
    });
    *last_sample = Some(current);
    pct
}

fn read_installed_firmware_version_from_path(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn pick_installed_firmware_version(
    file_version: Option<String>,
    runtime_override: Option<String>,
    env_override: Option<String>,
) -> Option<String> {
    file_version
        .filter(|value| !value.trim().is_empty())
        .or_else(|| runtime_override.filter(|value| !value.trim().is_empty()))
        .or_else(|| env_override.filter(|value| !value.trim().is_empty()))
}

fn read_installed_firmware_version() -> Option<String> {
    pick_installed_firmware_version(
        read_installed_firmware_version_from_path(Path::new(FIRMWARE_VERSION_PATH)),
        AgentConfig::runtime_var("SENTINEL_VERSION"),
        std::env::var("SENTINEL_VERSION").ok(),
    )
}

fn build_idle_firmware_state(
    current_version: Option<String>,
    can_rollback: bool,
) -> FirmwareHeartbeatState {
    FirmwareHeartbeatState {
        current_version,
        target_version: None,
        status: Some("idle".to_string()),
        can_rollback: Some(can_rollback),
        ..Default::default()
    }
}

fn backup_binary_exists(primary_path: &Path, legacy_paths: &[&Path]) -> bool {
    primary_path.exists() || legacy_paths.iter().any(|path| path.exists())
}

#[cfg(test)]
fn baseline_firmware_state_from_paths(
    version_path: &Path,
    backup_path: &Path,
    backup_path_alt: &Path,
    legacy_backup_path: &Path,
) -> FirmwareHeartbeatState {
    build_idle_firmware_state(
        read_installed_firmware_version_from_path(version_path),
        backup_binary_exists(backup_path, &[backup_path_alt, legacy_backup_path]),
    )
}

fn baseline_firmware_state() -> FirmwareHeartbeatState {
    build_idle_firmware_state(
        read_installed_firmware_version(),
        backup_binary_exists(
            Path::new(FIRMWARE_BACKUP_BINARY_PATH),
            &[
                Path::new(LEGACY_FIRMWARE_BACKUP_BINARY_PATH_ALT),
                Path::new(LEGACY_FIRMWARE_BACKUP_BINARY_PATH),
            ],
        ),
    )
}

fn build_agent_heartbeat_payload(
    agent_id: &str,
    timestamp: &str,
    cameras: Vec<Value>,
    telemetry: &HeartbeatTelemetry,
    firmware: &FirmwareHeartbeatState,
) -> Value {
    let mut payload = json!({
        "agent_id": agent_id,
        "timestamp": timestamp,
        "telemetry": telemetry.as_json(),
        "cameras": cameras,
    });
    if let Some(firmware_payload) = firmware.as_json() {
        payload["firmware"] = firmware_payload;
    }
    payload
}

async fn post_heartbeat(
    client: &reqwest::Client,
    config: &ServerConfig,
    payload: &Value,
) -> Result<()> {
    let base = config.base_url.trim_end_matches('/');
    let posted = try_fallback_paths(&["/api/v1/heartbeat", "/api/heartbeat"], |path| {
        let url = format!("{base}{path}");
        let payload_json = payload.to_string();
        async move {
            info!(url = %url, payload = %payload_json, "Sending agent heartbeat");
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
    })
    .await?;
    if posted.is_some() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("No heartbeat endpoint available"))
    }
}

fn pending_firmware_status(job: &FirmwareJobCommand) -> &'static str {
    if job.action == "rollback" {
        "rollback_pending"
    } else {
        "pending"
    }
}

fn is_terminal_firmware_status(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("succeeded" | "failed" | "rollback_succeeded" | "rollback_failed")
    )
}

fn normalize_firmware_version(value: &str) -> Option<String> {
    let normalized = value.trim().trim_start_matches('v').trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn firmware_versions_match(installed_version: Option<&str>, target_version: Option<&str>) -> bool {
    match (installed_version, target_version) {
        (Some(installed), Some(target)) => {
            normalize_firmware_version(installed) == normalize_firmware_version(target)
        }
        _ => false,
    }
}

fn running_firmware_status(job: &FirmwareJobCommand) -> &'static str {
    if job.action == "rollback" {
        "rollback_running"
    } else {
        "installing"
    }
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

async fn maybe_process_firmware_job<R, Fut>(
    client: &reqwest::Client,
    config: &ServerConfig,
    state: &mut FirmwareHeartbeatState,
    memory: &mut FirmwareJobMemory,
    mut report: R,
) where
    R: FnMut(FirmwareHeartbeatState) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
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

    let installed_version = read_installed_firmware_version();
    let can_rollback = Path::new(FIRMWARE_BACKUP_BINARY_PATH).exists();

    if let Some(terminal_state) =
        memory.duplicate_terminal_state(job.job_id, installed_version.clone(), can_rollback)
    {
        *state = terminal_state.clone();
        info!(
            job_id = job.job_id,
            action = %job.action,
            target_version = ?job.target_version,
            status = ?state.status,
            "Skipping duplicate firmware job that already reached a terminal state"
        );
        report(state.clone()).await;
        return;
    }

    if job.action == "update"
        && firmware_versions_match(installed_version.as_deref(), job.target_version.as_deref())
    {
        let now = Utc::now().to_rfc3339();
        state.job_id = Some(job.job_id);
        state.current_version = installed_version.or(job.target_version.clone());
        state.target_version = job.target_version.clone();
        state.status = Some("succeeded".to_string());
        state.error = None;
        state.can_rollback = Some(can_rollback);
        state.started_at = Some(now.clone());
        state.finished_at = Some(now);
        memory.remember_terminal_state(state);
        info!(
            job_id = job.job_id,
            action = %job.action,
            target_version = ?job.target_version,
            start_after_update = job.start_after_update,
            current_version = ?state.current_version,
            "Skipping firmware update because target version is already installed"
        );
        report(state.clone()).await;
        return;
    }

    state.job_id = Some(job.job_id);
    state.target_version = job.target_version.clone();
    state.status = Some(pending_firmware_status(&job).to_string());
    state.error = None;
    state.started_at = Some(Utc::now().to_rfc3339());
    state.finished_at = None;
    report(state.clone()).await;

    if job.action == "update" {
        state.status = Some("downloading".to_string());
        report(state.clone()).await;
    }

    state.status = Some(running_firmware_status(&job).to_string());
    report(state.clone()).await;

    info!(
        job_id = job.job_id,
        action = %job.action,
        target_version = ?job.target_version,
        start_after_update = job.start_after_update,
        "Executing firmware job"
    );

    match execute_firmware_job(&job).await {
        Ok(_) => {
            if job.action == "update" && job.start_after_update {
                state.status = Some("restarting".to_string());
                report(state.clone()).await;
            }
            let status = if job.action == "rollback" {
                "rollback_succeeded"
            } else {
                "succeeded"
            };
            state.status = Some(status.to_string());
            state.error = None;
            state.finished_at = Some(Utc::now().to_rfc3339());
            state.current_version = read_installed_firmware_version();
            if state.current_version.is_none() && job.action == "update" {
                state.current_version = job.target_version.clone();
            }
            if job.action == "update" {
                state.can_rollback = Some(true);
            } else {
                state.can_rollback = Some(false);
            }
            memory.remember_terminal_state(state);
            info!(job_id = job.job_id, status, "Firmware job completed");
            report(state.clone()).await;
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
            state.current_version = read_installed_firmware_version();
            memory.remember_terminal_state(state);
            warn!(job_id = job.job_id, status, error = %e, "Firmware job failed");
            report(state.clone()).await;
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

/// Sends periodic heartbeat to server using agent token.
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

    let heartbeat_interval = Duration::from_secs(30);
    let mut telemetry_sampler = TelemetrySampler::new();
    let mut firmware_job_memory = FirmwareJobMemory::default();
    loop {
        sleep(heartbeat_interval).await;
        onvif_probe.poll_for_jobs(&client, &config).await;

        let camera_snapshot = cameras.read().await.clone();
        let mut camera_status = Vec::new();
        let mut any_camera_impaired = false;
        for cam in camera_snapshot {
            let (ok, latency_ms, error) = check_rtsp_reachability(&cam.rtsp_url).await;
            if !ok {
                any_camera_impaired = true;
            }
            let onvif_summary = onvif_probe.summary_for(&cam.camera_id).await;
            camera_status.push(json!({
                "camera_id": cam.camera_id,
                "rtsp_ok": ok,
                "rtsp_latency_ms": latency_ms,
                "rtsp_error": error,
                "onvif_probe": onvif_summary.as_json(),
            }));
        }
        let telemetry = telemetry_sampler.sample(if any_camera_impaired {
            TelemetryStatus::Degraded
        } else {
            TelemetryStatus::Operational
        });
        let mut firmware = baseline_firmware_state();
        maybe_process_firmware_job(
            &client,
            &config,
            &mut firmware,
            &mut firmware_job_memory,
            |firmware_state| {
                let client = client.clone();
                let config = config.clone();
                let agent_id = agent_id.clone();
                let telemetry = telemetry.clone();
                let camera_status = camera_status.clone();
                async move {
                    let timestamp = Utc::now().to_rfc3339();
                    let payload = build_agent_heartbeat_payload(
                        &agent_id,
                        &timestamp,
                        camera_status,
                        &telemetry,
                        &firmware_state,
                    );
                    if let Err(error) = post_heartbeat(&client, &config, &payload).await {
                        warn!(error = %error, "Failed to post firmware heartbeat update");
                    }
                }
            },
        )
        .await;

        let timestamp = Utc::now().to_rfc3339();
        let payload = build_agent_heartbeat_payload(
            &agent_id,
            &timestamp,
            camera_status,
            &telemetry,
            &firmware,
        );

        let config_clone = config.clone();
        let client_clone = client.clone();
        let payload_clone = payload.clone();

        tokio::spawn(async move {
            retry_forever(
                || async { post_heartbeat(&client_clone, &config_clone, &payload_clone).await },
                Duration::from_secs(config_clone.retry_interval_secs),
                "post_agent_heartbeat",
            )
            .await
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        baseline_firmware_state_from_paths, build_agent_heartbeat_payload,
        build_heartbeat_telemetry, build_idle_firmware_state, firmware_versions_match,
        pick_firmware_updater_cmd, pick_installed_firmware_version, try_fallback_paths,
        FallbackOutcome, FirmwareJobMemory, MemoryUsageMb, OnvifProbeJobCommand,
        OnvifProbeJobResponse, OnvifProbeManager, TelemetryCapabilities, TelemetryStatus,
        DEFAULT_FIRMWARE_UPDATER_CMD,
    };
    use serde_json::{json, Value};
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
    fn baseline_firmware_state_reports_idle_version_and_rollback_capability() {
        let dir = unique_test_dir("firmware-state");
        fs::create_dir_all(&dir).expect("temp dir");
        let version_path = dir.join("firmware-version");
        let backup_path = dir.join("sentinel-agent.prev");
        let backup_path_alt = dir.join("agent_forward.prev");
        let legacy_backup_path = dir.join("sentinel_rtp_cam.prev");
        fs::write(&version_path, "0.6.4\n").expect("version file");
        fs::write(&backup_path, "").expect("backup file");

        let state = baseline_firmware_state_from_paths(
            &version_path,
            &backup_path,
            &backup_path_alt,
            &legacy_backup_path,
        );

        assert_eq!(state.current_version.as_deref(), Some("0.6.4"));
        assert!(state.target_version.is_none());
        assert_eq!(state.status.as_deref(), Some("idle"));
        assert_eq!(state.can_rollback, Some(true));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn baseline_firmware_state_reports_idle_without_local_firmware_metadata() {
        let dir = unique_test_dir("firmware-empty");
        fs::create_dir_all(&dir).expect("temp dir");
        let version_path = dir.join("firmware-version");
        let backup_path = dir.join("sentinel-agent.prev");
        let backup_path_alt = dir.join("agent_forward.prev");
        let legacy_backup_path = dir.join("sentinel_rtp_cam.prev");

        let state = baseline_firmware_state_from_paths(
            &version_path,
            &backup_path,
            &backup_path_alt,
            &legacy_backup_path,
        );

        assert_eq!(state.current_version, None);
        assert!(state.target_version.is_none());
        assert_eq!(state.status.as_deref(), Some("idle"));
        assert_eq!(state.can_rollback, Some(false));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn baseline_firmware_state_refreshes_manual_version_changes() {
        let dir = unique_test_dir("firmware-refresh");
        fs::create_dir_all(&dir).expect("temp dir");
        let version_path = dir.join("firmware-version");
        let backup_path = dir.join("sentinel-agent.prev");
        let backup_path_alt = dir.join("agent_forward.prev");
        let legacy_backup_path = dir.join("sentinel_rtp_cam.prev");

        fs::write(&version_path, "1.1.1\n").expect("initial version");
        let first = baseline_firmware_state_from_paths(
            &version_path,
            &backup_path,
            &backup_path_alt,
            &legacy_backup_path,
        );
        assert_eq!(first.current_version.as_deref(), Some("1.1.1"));
        assert_eq!(first.status.as_deref(), Some("idle"));

        fs::write(&version_path, "1.1.2\n").expect("updated version");
        let second = baseline_firmware_state_from_paths(
            &version_path,
            &backup_path,
            &backup_path_alt,
            &legacy_backup_path,
        );
        assert_eq!(second.current_version.as_deref(), Some("1.1.2"));
        assert_eq!(second.status.as_deref(), Some("idle"));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn baseline_firmware_state_accepts_legacy_backup_path_for_rollback() {
        let dir = unique_test_dir("firmware-legacy-backup");
        fs::create_dir_all(&dir).expect("temp dir");
        let version_path = dir.join("firmware-version");
        let backup_path = dir.join("sentinel-agent.prev");
        let backup_path_alt = dir.join("agent_forward.prev");
        let legacy_backup_path = dir.join("sentinel_rtp_cam.prev");
        fs::write(&version_path, "2.0.0\n").expect("version file");
        fs::write(&legacy_backup_path, "").expect("legacy backup file");

        let state = baseline_firmware_state_from_paths(
            &version_path,
            &backup_path,
            &backup_path_alt,
            &legacy_backup_path,
        );

        assert_eq!(state.current_version.as_deref(), Some("2.0.0"));
        assert_eq!(state.can_rollback, Some(true));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn backup_binary_exists_accepts_current_or_legacy_path() {
        let dir = unique_test_dir("backup-exists");
        fs::create_dir_all(&dir).expect("temp dir");
        let backup_path = dir.join("sentinel-agent.prev");
        let backup_path_alt = dir.join("agent_forward.prev");
        let legacy_backup_path = dir.join("sentinel_rtp_cam.prev");

        assert!(!super::backup_binary_exists(
            &backup_path,
            &[&backup_path_alt, &legacy_backup_path]
        ));

        fs::write(&backup_path, "").expect("backup file");
        assert!(super::backup_binary_exists(
            &backup_path,
            &[&backup_path_alt, &legacy_backup_path]
        ));
        fs::remove_file(&backup_path).expect("remove current backup");

        fs::write(&backup_path_alt, "").expect("legacy alt backup file");
        assert!(super::backup_binary_exists(
            &backup_path,
            &[&backup_path_alt, &legacy_backup_path]
        ));
        fs::remove_file(&backup_path_alt).expect("remove legacy alt backup");

        fs::write(&legacy_backup_path, "").expect("legacy backup file");
        assert!(super::backup_binary_exists(
            &backup_path,
            &[&backup_path_alt, &legacy_backup_path]
        ));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn firmware_job_memory_reuses_terminal_state_for_same_job_id() {
        let mut memory = FirmwareJobMemory::default();
        let mut state = build_idle_firmware_state(Some("1.1.2".to_string()), true);
        state.job_id = Some(7);
        state.target_version = Some("1.1.3".to_string());
        state.status = Some("succeeded".to_string());
        state.finished_at = Some("2026-03-22T14:58:16Z".to_string());

        memory.remember_terminal_state(&state);

        let duplicate = memory
            .duplicate_terminal_state(7, Some("1.1.3".to_string()), true)
            .expect("duplicate terminal state");

        assert_eq!(duplicate.job_id, Some(7));
        assert_eq!(duplicate.target_version.as_deref(), Some("1.1.3"));
        assert_eq!(duplicate.status.as_deref(), Some("succeeded"));
        assert_eq!(duplicate.current_version.as_deref(), Some("1.1.3"));
        assert_eq!(duplicate.can_rollback, Some(true));
    }

    #[test]
    fn firmware_job_memory_ignores_non_terminal_state() {
        let mut memory = FirmwareJobMemory::default();
        let mut state = build_idle_firmware_state(Some("1.1.2".to_string()), false);
        state.job_id = Some(7);
        state.target_version = Some("1.1.3".to_string());
        state.status = Some("installing".to_string());

        memory.remember_terminal_state(&state);

        assert!(memory.duplicate_terminal_state(7, None, false).is_none());
    }

    #[test]
    fn agent_heartbeat_payload_uses_resolved_version_after_previous_state_reported_older() {
        let mut memory = FirmwareJobMemory::default();
        let mut previous = build_idle_firmware_state(Some("1.1.3".to_string()), true);
        previous.job_id = Some(11);
        previous.target_version = Some("1.1.3".to_string());
        previous.status = Some("succeeded".to_string());
        previous.finished_at = Some("2026-03-29T10:00:00Z".to_string());
        memory.remember_terminal_state(&previous);

        let resolved_version = pick_installed_firmware_version(
            Some("2.0.1".to_string()),
            Some("1.1.3".to_string()),
            Some("1.1.3".to_string()),
        )
        .expect("resolved version");
        let firmware = memory
            .duplicate_terminal_state(11, Some(resolved_version), true)
            .expect("duplicate terminal state");
        let telemetry = build_heartbeat_telemetry(
            TelemetryStatus::Operational,
            Some(12),
            Some(MemoryUsageMb {
                used_mb: 321,
                total_mb: 2048,
            }),
            vec!["model-1".to_string()],
            TelemetryCapabilities::default(),
        );

        let payload = build_agent_heartbeat_payload(
            "agent-1",
            "2026-03-29T10:00:30Z",
            vec![json!({
                "camera_id": "cam-1",
                "rtsp_ok": true,
                "rtsp_latency_ms": 5,
                "rtsp_error": Value::Null,
                "onvif_probe": {
                    "status": "never_run",
                },
            })],
            &telemetry,
            &firmware,
        );

        assert_eq!(payload["agent_id"], "agent-1");
        assert_eq!(payload["firmware"]["current_version"], "2.0.1");
        assert_eq!(payload["firmware"]["status"], "succeeded");
    }

    #[test]
    fn firmware_version_match_normalizes_optional_v_prefix() {
        assert!(firmware_versions_match(Some("1.1.3"), Some("1.1.3")));
        assert!(firmware_versions_match(Some("1.1.3"), Some("v1.1.3")));
        assert!(firmware_versions_match(Some(" v1.1.3 "), Some("1.1.3")));
        assert!(!firmware_versions_match(Some("1.1.2"), Some("1.1.3")));
        assert!(!firmware_versions_match(Some("1.1.3"), None));
    }

    #[test]
    fn installed_firmware_version_prefers_file_then_runtime_then_env() {
        assert_eq!(
            pick_installed_firmware_version(
                Some("1.1.3".to_string()),
                Some("2.0.1".to_string()),
                Some("2.0.1".to_string()),
            )
            .as_deref(),
            Some("1.1.3")
        );
        assert_eq!(
            pick_installed_firmware_version(
                Some("   ".to_string()),
                Some("2.0.1".to_string()),
                Some("1.1.3".to_string()),
            )
            .as_deref(),
            Some("2.0.1")
        );
        assert_eq!(
            pick_installed_firmware_version(
                None,
                Some("   ".to_string()),
                Some("2.0.1".to_string())
            )
            .as_deref(),
            Some("2.0.1")
        );
    }

    #[test]
    fn telemetry_json_uses_memory_fields_and_sanitizes_models() {
        let telemetry = build_heartbeat_telemetry(
            TelemetryStatus::Degraded,
            Some(140),
            Some(MemoryUsageMb {
                used_mb: 512,
                total_mb: 1024,
            }),
            vec![
                " detector-a ".to_string(),
                "".to_string(),
                "detector-a".to_string(),
                "detector-b".to_string(),
            ],
            TelemetryCapabilities {
                face_recognition: false,
                license_plate: false,
                object_tracking: false,
                anomalous_motion: false,
            },
        );

        let payload = telemetry.as_json();

        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["cpu_load_pct"], 100);
        assert_eq!(payload["memory_used_mb"], 512);
        assert_eq!(payload["memory_total_mb"], 1024);
        assert_eq!(
            payload["active_models"],
            json!(["detector-a", "detector-b"])
        );
        assert!(payload.get("gpu_vram_used_mb").is_none());
        assert!(payload.get("gpu_vram_total_mb").is_none());
    }

    #[test]
    fn agent_heartbeat_payload_includes_telemetry_and_idle_firmware() {
        let telemetry = build_heartbeat_telemetry(
            TelemetryStatus::Operational,
            Some(12),
            Some(MemoryUsageMb {
                used_mb: 321,
                total_mb: 2048,
            }),
            vec!["model-1".to_string()],
            TelemetryCapabilities::default(),
        );
        let firmware = build_idle_firmware_state(Some("1.1.2".to_string()), true);
        let payload = build_agent_heartbeat_payload(
            "agent-1",
            "2026-03-21T18:00:00Z",
            vec![json!({
                "camera_id": "cam-1",
                "rtsp_ok": true,
                "rtsp_latency_ms": 5,
                "rtsp_error": Value::Null,
                "onvif_probe": {
                    "status": "never_run",
                },
            })],
            &telemetry,
            &firmware,
        );

        assert_eq!(payload["agent_id"], "agent-1");
        assert_eq!(payload["telemetry"]["status"], "operational");
        assert_eq!(payload["telemetry"]["memory_used_mb"], 321);
        assert_eq!(payload["firmware"]["current_version"], "1.1.2");
        assert_eq!(payload["firmware"]["status"], "idle");
        assert_eq!(payload["firmware"]["can_rollback"], true);
        assert_eq!(payload["firmware"]["target_version"], Value::Null);
        assert_eq!(payload["cameras"][0]["camera_id"], "cam-1");
    }

    #[tokio::test]
    async fn heartbeat_post_falls_back_to_legacy_endpoint() {
        let attempts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let attempts_for_closure = attempts.clone();
        let posted = try_fallback_paths(&["/api/v1/heartbeat", "/api/heartbeat"], move |path| {
            let attempts = attempts_for_closure.clone();
            let path = path.to_string();
            async move {
                attempts.lock().expect("attempt log").push(path.clone());
                if path == "/api/v1/heartbeat" {
                    Ok(FallbackOutcome::NotFound)
                } else {
                    Ok(FallbackOutcome::Success(()))
                }
            }
        })
        .await
        .expect("fallback post");
        assert!(posted.is_some());

        let attempts = attempts.lock().expect("attempt log");
        assert_eq!(
            *attempts,
            vec![
                "/api/v1/heartbeat".to_string(),
                "/api/heartbeat".to_string(),
            ]
        );
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
