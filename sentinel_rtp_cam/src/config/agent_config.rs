use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Agent configuration that can be updated dynamically via server SSE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    /// Camera/device identifier
    pub camera_id: String,

    /// Motion detection configuration
    pub motion: MotionConfig,

    /// Server communication configuration
    pub server: ServerConfig,

    /// Disk cleanup configuration
    pub cleanup: CleanupConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MotionConfig {
    /// Whether to publish motion events (can be disabled by server)
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Whether server integration is enabled
    pub enabled: bool,

    /// Base URL for server API (e.g., "http://127.0.0.1:8080")
    pub base_url: String,

    /// Bearer token for authentication
    pub bearer_token: String,

    /// Retry interval for failed requests (seconds)
    pub retry_interval_secs: u64,

    /// Maximum number of retry attempts before dropping message (0 = infinite)
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupConfig {
    /// How often to check disk space (seconds)
    pub interval_secs: u64,

    /// Minimum free bytes to maintain
    pub min_free_bytes: u64,
}

impl AgentConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let camera_id = std::env::var("CAMERA_ID")
            .or_else(|_| std::env::var("ONVIF_HOST"))
            .unwrap_or_else(|_| "unknown-camera".to_string());

        let motion = MotionConfig {
            enabled: env_bool("MOTION_ENABLED", true),
        };

        let server = ServerConfig {
            enabled: env_bool("SERVER_ENABLED", false),
            base_url: std::env::var("SERVER_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string()),
            bearer_token: std::env::var("SERVER_BEARER_TOKEN")
                .unwrap_or_else(|_| "devtoken".to_string()),
            retry_interval_secs: env_u64("SERVER_RETRY_INTERVAL_SECS", 2),
            max_retries: env_u64("SERVER_MAX_RETRIES", 5) as u32,
        };

        let cleanup = CleanupConfig {
            interval_secs: env_u64("CLIP_CLEANUP_INTERVAL_SECS", 30),
            min_free_bytes: env_u64("CLIP_MIN_FREE_BYTES", 2_000_000_000),
        };

        Self {
            camera_id,
            motion,
            server,
            cleanup,
        }
    }

    /// Load configuration from a JSON file, creating a default empty file if missing.
    pub fn from_json_file(path: &Path) -> std::io::Result<Self> {
        let value = Self::load_json_value(path)?;
        Ok(Self::from_json_value(value))
    }

    /// Load the raw JSON value from disk, creating a default empty file if missing.
    pub fn load_json_value(path: &Path) -> std::io::Result<Value> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let empty = default_empty_json();
            let pretty = serde_json::to_string_pretty(&empty).unwrap_or_else(|_| "{}".to_string());
            fs::write(path, pretty)?;
        }

        let raw = fs::read_to_string(path)?;
        let value: Value =
            serde_json::from_str(&raw).unwrap_or_else(|_| Value::Object(Default::default()));
        Ok(value)
    }

    /// Build the agent config from a JSON value (merging with defaults).
    pub fn from_json_value(value: Value) -> Self {
        Self::merge_with_defaults(value)
    }

    /// Overwrite the JSON file with the provided config value.
    pub fn write_json_file(path: &Path, value: &Value) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string());
        fs::write(path, pretty)
    }

    /// Merge an update into the existing JSON file and persist it.
    pub fn merge_json_file(path: &Path, update: &Value) -> std::io::Result<()> {
        let mut current = Self::load_json_value(path)?;
        merge_value(&mut current, update);
        Self::write_json_file(path, &current)
    }

    /// Apply JSON overrides to process environment variables.
    pub fn apply_json_env_overrides(value: &Value) {
        set_env_if_value("CAMERA_ID", value.get("camera_id"));

        let server = value.get("server").unwrap_or(&Value::Null);
        set_env_if_value("SERVER_ENABLED", server.get("enabled"));
        set_env_if_value("SERVER_BASE_URL", server.get("base_url"));
        set_env_if_value("SERVER_BEARER_TOKEN", server.get("bearer_token"));
        set_env_if_value("SERVER_RETRY_INTERVAL_SECS", server.get("retry_interval_secs"));
        set_env_if_value("SERVER_MAX_RETRIES", server.get("max_retries"));

        let cleanup = value.get("cleanup").unwrap_or(&Value::Null);
        set_env_if_value("CLIP_CLEANUP_INTERVAL_SECS", cleanup.get("interval_secs"));
        set_env_if_value("CLIP_MIN_FREE_BYTES", cleanup.get("min_free_bytes"));

        let local_clip = value.get("local_clip").unwrap_or(&Value::Null);
        set_env_if_value("CLIP_DIR", local_clip.get("dir"));
        set_env_if_value("OUTPUT_DIR", local_clip.get("dir"));
        set_env_if_value("CLIP_PRE_ROLL_SECS", local_clip.get("pre_roll_secs"));
        set_env_if_value("CLIP_POST_ROLL_SECS", local_clip.get("post_roll_secs"));
        set_env_if_value("CLIP_MIN_DURATION_SECS", local_clip.get("min_duration_secs"));
        set_env_if_value("CLIP_FLUSH_SECS", local_clip.get("flush_secs"));
        set_env_if_value("CLIP_STALE_PART_SECS", local_clip.get("stale_part_secs"));
        set_env_if_value("CLIP_WRITE_BATCH_BYTES", local_clip.get("write_batch_bytes"));
        set_env_if_value("CLIP_MAX_FILES", local_clip.get("max_files"));
        set_env_if_value("CLIP_MAX_AGE_SECS", local_clip.get("max_age_secs"));
        set_env_if_value("CLIP_MAX_TOTAL_BYTES", local_clip.get("max_total_bytes"));
        set_env_if_value("CLIP_MAX_BYTES", local_clip.get("max_bytes"));
        set_env_if_value("CLIP_MAX_SECS", local_clip.get("max_secs"));
        set_env_if_value("CLIP_FPS", local_clip.get("fps"));
        set_env_if_value("CLIP_STREAM_COPY", local_clip.get("stream_copy"));
        set_env_if_value("CLIP_AUDIO_ENABLED", local_clip.get("audio_enabled"));

        let ingest = value.get("ingest").unwrap_or(&Value::Null);
        set_env_if_unset("CLIP_DIR", ingest.get("clip_dir"));
        set_env_if_value("CLIP_PRE_SECS", ingest.get("clip_pre_secs"));
        set_env_if_value("CLIP_POST_SECS", ingest.get("clip_post_secs"));
        set_env_if_value("CLIP_RING_SECS", ingest.get("clip_ring_secs"));
        set_env_if_unset("CLIP_STALE_PART_SECS", ingest.get("clip_stale_part_secs"));
        set_env_if_unset("CLIP_MAX_SECS", ingest.get("clip_max_secs"));

        let forward = value.get("forward_agent").unwrap_or(&Value::Null);
        set_env_if_value("AGENT_MODE", forward.get("mode"));
        set_env_if_value("SERVER_ADDR", forward.get("server_addr"));
        set_env_if_value("MOTION_MERGE_SECS", forward.get("motion_merge_secs"));

        let logging = value.get("logging").unwrap_or(&Value::Null);
        set_env_if_value("RUST_LOG", logging.get("rust_log"));

        let version = value.get("version").unwrap_or(&Value::Null);
        set_env_if_value("SENTINEL_VERSION", version.get("sentinel_version"));

        if let Some(cameras) = value.get("cameras").and_then(|v| v.as_array()) {
            for (idx, cam) in cameras.iter().enumerate() {
                let slot = idx + 1;
                let prefix = format!("CAM{slot}_");
                let user = cam.get("user");
                let pass = cam.get("pass");
                let rtsp = cam.get("rtsp").unwrap_or(&Value::Null);
                let onvif = cam.get("onvif").unwrap_or(&Value::Null);
                let motion = cam.get("motion").unwrap_or(&Value::Null);
                let features = cam.get("features").unwrap_or(&Value::Null);

                set_env_if_value(&format!("{prefix}CAMERA_ID"), cam.get("id"));
                set_env_if_value(&format!("{prefix}AGENT_ID"), cam.get("id"));
                set_env_if_value(&format!("{prefix}AGENT_TOKEN"), cam.get("token"));
                set_env_if_value(&format!("{prefix}STREAM_ID"), cam.get("stream_id"));
                set_env_if_value(&format!("{prefix}TRANSPORT"), cam.get("transport"));

                set_env_if_string(&format!("{prefix}RTSP_URL"), build_rtsp_url(rtsp));
                set_env_if_value(&format!("{prefix}RTSP_USER"), user);
                set_env_if_value(&format!("{prefix}RTSP_PASS"), pass);

                set_env_if_value(&format!("{prefix}ONVIF_HOST"), onvif.get("host"));
                set_env_if_value(&format!("{prefix}ONVIF_PORT"), onvif.get("port"));
                set_env_if_value(&format!("{prefix}ONVIF_USER"), user);
                set_env_if_value(&format!("{prefix}ONVIF_PASS"), pass);
                set_env_if_value(&format!("{prefix}ONVIF_DEBUG"), onvif.get("debug"));
                set_env_if_value(&format!("{prefix}ONVIF_DUMP_XML"), onvif.get("dump_xml"));
                set_env_if_value(
                    &format!("{prefix}ONVIF_SUB_TERMINATION"),
                    onvif.get("sub_termination"),
                );
                set_env_if_value(
                    &format!("{prefix}ONVIF_RENEW_EVERY_SECS"),
                    onvif.get("renew_every_secs"),
                );
                set_env_if_value(
                    &format!("{prefix}ONVIF_PULL_TIMEOUT"),
                    onvif.get("pull_timeout"),
                );
                set_env_if_value(&format!("{prefix}ONVIF_PULL_LIMIT"), onvif.get("pull_limit"));
                set_env_if_value(
                    &format!("{prefix}ONVIF_RESUBSCRIBE_AFTER_ERRORS"),
                    onvif.get("resubscribe_after_errors"),
                );
                set_env_if_value(
                    &format!("{prefix}ONVIF_MIN_POLL_GAP_MS"),
                    onvif.get("min_poll_gap_ms"),
                );
                set_env_if_value(
                    &format!("{prefix}ONVIF_AFTER_SUB_DELAY_MS"),
                    onvif.get("after_sub_delay_ms"),
                );
                set_env_if_value(
                    &format!("{prefix}ONVIF_CONNREFUSED_RETRIES"),
                    onvif.get("connrefused_retries"),
                );
                set_env_if_value(
                    &format!("{prefix}ONVIF_CONNREFUSED_BACKOFF_MS"),
                    onvif.get("connrefused_backoff_ms"),
                );
                set_env_if_value(&format!("{prefix}MOTION_ENABLED"), motion.get("enabled"));
                set_env_if_value(
                    &format!("{prefix}LOCAL_CLIP_ENABLED"),
                    features.get("local_clip_enabled"),
                );
                set_env_if_value(
                    &format!("{prefix}RTSP_RECEIVER_ENABLED"),
                    features.get("rtsp_receiver_enabled"),
                );
            }

            if let Some(first) = cameras.first() {
                let user = first.get("user");
                let pass = first.get("pass");
                let rtsp = first.get("rtsp").unwrap_or(&Value::Null);
                let onvif = first.get("onvif").unwrap_or(&Value::Null);
                let motion = first.get("motion").unwrap_or(&Value::Null);
                let features = first.get("features").unwrap_or(&Value::Null);

                set_env_if_unset("CAMERA_ID", first.get("id"));
                set_env_if_unset("AGENT_ID", first.get("id"));
                set_env_if_unset("AGENT_TOKEN", first.get("token"));
                set_env_if_unset("MOTION_ENABLED", motion.get("enabled"));
                set_env_if_unset("LOCAL_CLIP_ENABLED", features.get("local_clip_enabled"));
                set_env_if_unset(
                    "RTSP_RECEIVER_ENABLED",
                    features.get("rtsp_receiver_enabled"),
                );
                set_env_if_unset_string("RTSP_URL", build_rtsp_url(rtsp));
                set_env_if_unset("RTSP_HOST", rtsp.get("host"));
                set_env_if_unset("RTSP_PORT", rtsp.get("port"));
                set_env_if_unset("RTSP_PATH", rtsp.get("path"));
                set_env_if_unset("RTSP_USER", user);
                set_env_if_unset("RTSP_PASS", pass);
                set_env_if_unset("ONVIF_HOST", onvif.get("host"));
                set_env_if_unset("ONVIF_PORT", onvif.get("port"));
                set_env_if_unset("ONVIF_USER", user);
                set_env_if_unset("ONVIF_PASS", pass);
                set_env_if_unset("ONVIF_DEBUG", onvif.get("debug"));
                set_env_if_unset("ONVIF_DUMP_XML", onvif.get("dump_xml"));
                set_env_if_unset("ONVIF_SUB_TERMINATION", onvif.get("sub_termination"));
                set_env_if_unset("ONVIF_RENEW_EVERY_SECS", onvif.get("renew_every_secs"));
                set_env_if_unset("ONVIF_PULL_TIMEOUT", onvif.get("pull_timeout"));
                set_env_if_unset("ONVIF_PULL_LIMIT", onvif.get("pull_limit"));
                set_env_if_unset(
                    "ONVIF_RESUBSCRIBE_AFTER_ERRORS",
                    onvif.get("resubscribe_after_errors"),
                );
                set_env_if_unset("ONVIF_MIN_POLL_GAP_MS", onvif.get("min_poll_gap_ms"));
                set_env_if_unset("ONVIF_AFTER_SUB_DELAY_MS", onvif.get("after_sub_delay_ms"));
                set_env_if_unset("ONVIF_CONNREFUSED_RETRIES", onvif.get("connrefused_retries"));
                set_env_if_unset(
                    "ONVIF_CONNREFUSED_BACKOFF_MS",
                    onvif.get("connrefused_backoff_ms"),
                );
            }
        }
    }

    /// Get retry interval as Duration
    pub fn retry_interval(&self) -> Duration {
        Duration::from_secs(self.server.retry_interval_secs)
    }

    /// Get cleanup interval as Duration
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup.interval_secs)
    }
}

fn default_empty_json() -> Value {
    serde_json::json!({
        "server": {
            "enabled": null,
            "base_url": null,
            "bearer_token": null,
            "retry_interval_secs": null,
            "max_retries": null
        },
        "cleanup": {
            "interval_secs": null,
            "min_free_bytes": null
        },
        "local_clip": {
            "dir": null,
            "pre_roll_secs": null,
            "post_roll_secs": null,
            "min_duration_secs": null,
            "flush_secs": null,
            "stale_part_secs": null,
            "write_batch_bytes": null,
            "max_files": null,
            "max_age_secs": null,
            "max_total_bytes": null,
            "max_bytes": null,
            "max_secs": null,
            "fps": null,
            "stream_copy": null,
            "audio_enabled": null
        },
        "ingest": {
            "clip_dir": null,
            "clip_pre_secs": null,
            "clip_post_secs": null,
            "clip_ring_secs": null,
            "clip_stale_part_secs": null,
            "clip_max_secs": null
        },
        "forward_agent": {
            "mode": null,
            "server_addr": null,
            "motion_merge_secs": null
        },
        "cameras": [
            {
                "id": null,
                "token": null,
                "user": null,
                "pass": null,
                "stream_id": null,
                "transport": null,
                "motion": {
                    "enabled": null
                },
                "features": {
                    "local_clip_enabled": null,
                    "rtsp_receiver_enabled": null
                },
                "rtsp": {
                    "url": null,
                    "host": null,
                    "port": null,
                    "path": null
                },
                "onvif": {
                    "host": null,
                    "port": null,
                    "debug": null,
                    "dump_xml": null,
                    "sub_termination": null,
                    "renew_every_secs": null,
                    "pull_timeout": null,
                    "pull_limit": null,
                    "resubscribe_after_errors": null,
                    "min_poll_gap_ms": null,
                    "after_sub_delay_ms": null,
                    "connrefused_retries": null,
                    "connrefused_backoff_ms": null
                }
            }
        ],
        "logging": {
            "rust_log": null
        },
        "version": {
            "sentinel_version": null
        }
    })
}

fn merge_string(value: &Value, key: &str, default: &str) -> String {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| default.to_string())
}

fn merge_bool(value: &Value, key: &str, default: bool) -> bool {
    value.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
}

fn merge_u64(value: &Value, key: &str, default: u64) -> u64 {
    value.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
}

fn merge_with_section<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or(&Value::Null)
}

fn merge_camera_id(value: &Value, default: &str) -> String {
    let direct = merge_string(value, "camera_id", default);
    if direct != default {
        return direct;
    }

    let cameras = value.get("cameras").and_then(|v| v.as_array());
    if let Some(cameras) = cameras {
        if let Some(first) = cameras.first() {
            if let Some(id) = first.get("id").and_then(|v| v.as_str()) {
                if !id.trim().is_empty() {
                    return id.to_string();
                }
            }
        }
    }

    direct
}

fn merge_motion_enabled(value: &Value, default: bool) -> bool {
    if let Some(motion) = value.get("motion") {
        if let Some(enabled) = motion.get("enabled").and_then(|v| v.as_bool()) {
            return enabled;
        }
    }

    let cameras = value.get("cameras").and_then(|v| v.as_array());
    if let Some(cameras) = cameras {
        if let Some(first) = cameras.first() {
            if let Some(enabled) = first
                .get("motion")
                .and_then(|m| m.get("enabled"))
                .and_then(|v| v.as_bool())
            {
                return enabled;
            }
        }
    }

    default
}

fn build_rtsp_url(rtsp: &Value) -> Option<String> {
    if let Some(url) = rtsp.get("url").and_then(|v| v.as_str()) {
        if !url.trim().is_empty() {
            return Some(url.to_string());
        }
    }

    let host = rtsp.get("host").and_then(|v| v.as_str())?;
    if host.trim().is_empty() {
        return None;
    }
    let port = rtsp.get("port").and_then(|v| v.as_u64()).unwrap_or(554);
    let path = rtsp
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let path = if path.is_empty() {
        String::new()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };
    Some(format!("rtsp://{}:{}{}", host, port, path))
}

fn merge_value(base: &mut Value, update: &Value) {
    match (base, update) {
        (Value::Object(base_map), Value::Object(update_map)) => {
            for (key, value) in update_map {
                match base_map.get_mut(key) {
                    Some(existing) => merge_value(existing, value),
                    None => {
                        base_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base_val, update_val) => {
            *base_val = update_val.clone();
        }
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) if s.trim().is_empty() => None,
        Value::String(s) => Some(s.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(true) => Some("1".to_string()),
        Value::Bool(false) => Some("0".to_string()),
        _ => None,
    }
}

fn set_env_if_value(key: &str, value: Option<&Value>) {
    if let Some(value) = value.and_then(value_to_string) {
        std::env::set_var(key, value);
    }
}

fn set_env_if_string(key: &str, value: Option<String>) {
    if let Some(value) = value {
        if !value.trim().is_empty() {
            std::env::set_var(key, value);
        }
    }
}

fn set_env_if_unset_string(key: &str, value: Option<String>) {
    if std::env::var_os(key).is_some() {
        return;
    }
    set_env_if_string(key, value);
}

fn set_env_if_unset(key: &str, value: Option<&Value>) {
    if std::env::var_os(key).is_some() {
        return;
    }
    set_env_if_value(key, value);
}

impl AgentConfig {
    fn merge_with_defaults(value: Value) -> Self {
        let defaults = AgentConfig::default();

        let motion_val = merge_with_section(&value, "motion");
        let server_val = merge_with_section(&value, "server");
        let cleanup_val = merge_with_section(&value, "cleanup");
        let camera_id = merge_camera_id(&value, &defaults.camera_id);
        let motion_enabled = merge_motion_enabled(&value, defaults.motion.enabled);

        AgentConfig {
            camera_id,
            motion: MotionConfig {
                enabled: merge_bool(motion_val, "enabled", motion_enabled),
            },
            server: ServerConfig {
                enabled: merge_bool(server_val, "enabled", defaults.server.enabled),
                base_url: merge_string(server_val, "base_url", &defaults.server.base_url),
                bearer_token: merge_string(
                    server_val,
                    "bearer_token",
                    &defaults.server.bearer_token,
                ),
                retry_interval_secs: merge_u64(
                    server_val,
                    "retry_interval_secs",
                    defaults.server.retry_interval_secs,
                ),
                max_retries: merge_u64(server_val, "max_retries", defaults.server.max_retries as u64)
                    as u32,
            },
            cleanup: CleanupConfig {
                interval_secs: merge_u64(
                    cleanup_val,
                    "interval_secs",
                    defaults.cleanup.interval_secs,
                ),
                min_free_bytes: merge_u64(
                    cleanup_val,
                    "min_free_bytes",
                    defaults.cleanup.min_free_bytes,
                ),
            },
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            camera_id: "unknown-camera".to_string(),
            motion: MotionConfig { enabled: true },
            server: ServerConfig {
                enabled: false,
                base_url: "http://127.0.0.1:8080".to_string(),
                bearer_token: "devtoken".to_string(),
                retry_interval_secs: 2,
                max_retries: 5,
            },
            cleanup: CleanupConfig {
                interval_secs: 30,
                min_free_bytes: 2_000_000_000,
            },
        }
    }
}

// Helper functions for parsing env vars
fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|v| match v.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
