use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;
use url::Url;

static RUNTIME_OVERRIDES: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
#[cfg(test)]
static RUNTIME_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn overrides_map() -> &'static RwLock<HashMap<String, String>> {
    RUNTIME_OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

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
    /// Load configuration from runtime overrides initialized from JSON.
    pub fn from_env() -> Self {
        let camera_id = Self::runtime_var("CAMERA_ID")
            .or_else(|| Self::runtime_var("ONVIF_HOST"))
            .unwrap_or_else(|| "unknown-camera".to_string());

        let motion = MotionConfig {
            enabled: Self::runtime_bool("MOTION_ENABLED", true),
        };

        let server = ServerConfig {
            enabled: Self::runtime_bool("SERVER_ENABLED", false),
            base_url: Self::runtime_var("SERVER_BASE_URL")
                .unwrap_or_else(|| "http://127.0.0.1:8080".to_string()),
            bearer_token: Self::runtime_var("SERVER_BEARER_TOKEN")
                .unwrap_or_else(|| "devtoken".to_string()),
            retry_interval_secs: Self::runtime_u64("SERVER_RETRY_INTERVAL_SECS", 2),
            max_retries: Self::runtime_u64("SERVER_MAX_RETRIES", 5) as u32,
        };

        let cleanup = CleanupConfig {
            interval_secs: Self::runtime_u64("CLIP_CLEANUP_INTERVAL_SECS", 30),
            min_free_bytes: Self::runtime_u64("CLIP_MIN_FREE_BYTES", 2_000_000_000),
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
        Self::load_json_value_with_default(path, default_empty_json())
    }

    /// Load the raw JSON value from disk, creating a file from the provided default if missing.
    pub fn load_json_value_with_default(path: &Path, default: Value) -> std::io::Result<Value> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let pretty =
                serde_json::to_string_pretty(&default).unwrap_or_else(|_| "{}".to_string());
            fs::write(path, pretty)?;
        }

        let raw = fs::read_to_string(path)?;
        let value: Value =
            serde_json::from_str(&raw).unwrap_or_else(|_| Value::Object(Default::default()));
        Ok(value)
    }

    /// Load the server config JSON, creating the default if missing.
    pub fn load_server_json(path: &Path) -> std::io::Result<Value> {
        Self::load_json_value_with_default(path, Self::default_server_json())
    }

    /// Load the camera config JSON, creating the default if missing.
    pub fn load_camera_json(path: &Path) -> std::io::Result<Value> {
        Self::load_json_value_with_default(path, Self::default_camera_json())
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
        Self::merge_json_file_with_default(path, update, default_empty_json())
    }

    /// Merge an update into the existing JSON file using a provided default and persist it.
    pub fn merge_json_file_with_default(
        path: &Path,
        update: &Value,
        default: Value,
    ) -> std::io::Result<()> {
        let mut current = Self::load_json_value_with_default(path, default)?;
        merge_value(&mut current, update);
        Self::write_json_file(path, &current)
    }

    /// Merge a remote camera config into camera.json.
    ///
    /// Canonical remote configs carry cameras[]; when one arrives, stale legacy
    /// top-level camera fields must not survive from an older local file.
    pub fn merge_remote_camera_json(path: &Path, update: &Value) -> std::io::Result<()> {
        let mut current = Self::load_json_value_with_default(path, Self::default_camera_json())?;
        let mut update = update.clone();
        if has_canonical_cameras(&update) {
            strip_top_level_legacy_camera_fields(&mut current);
            strip_top_level_legacy_camera_fields(&mut update);
        }
        merge_value(&mut current, &update);
        Self::write_json_file(path, &current)
    }

    /// Build and install runtime key-value overrides from JSON.
    pub fn install_runtime_overrides(value: &Value) {
        let mut map = HashMap::new();

        set_map_if_value(&mut map, "CAMERA_ID", value.get("camera_id"));
        set_map_if_value(&mut map, "AGENT_ID", value.get("agent_id"));

        let server = value.get("server").unwrap_or(&Value::Null);
        set_map_if_value(&mut map, "SERVER_ENABLED", server.get("enabled"));
        set_map_if_value(&mut map, "SERVER_BASE_URL", server.get("base_url"));
        set_map_if_value(&mut map, "SERVER_BEARER_TOKEN", server.get("bearer_token"));
        set_map_if_unset(&mut map, "AGENT_TOKEN", server.get("bearer_token"));
        set_map_if_unset(&mut map, "SERVER_TOKEN", server.get("bearer_token"));
        set_map_if_value(
            &mut map,
            "SERVER_RETRY_INTERVAL_SECS",
            server.get("retry_interval_secs"),
        );
        set_map_if_value(&mut map, "SERVER_MAX_RETRIES", server.get("max_retries"));

        let cleanup = value.get("cleanup").unwrap_or(&Value::Null);
        set_map_if_value(
            &mut map,
            "CLIP_CLEANUP_INTERVAL_SECS",
            cleanup.get("interval_secs"),
        );
        set_map_if_value(
            &mut map,
            "CLIP_MIN_FREE_BYTES",
            cleanup.get("min_free_bytes"),
        );

        let ingest = value.get("ingest").unwrap_or(&Value::Null);
        set_map_if_unset(&mut map, "CLIP_DIR", ingest.get("clip_dir"));
        set_map_if_value(&mut map, "CLIP_PRE_SECS", ingest.get("clip_pre_secs"));
        set_map_if_value(&mut map, "CLIP_POST_SECS", ingest.get("clip_post_secs"));
        set_map_if_value(&mut map, "CLIP_RING_SECS", ingest.get("clip_ring_secs"));
        set_map_if_unset(
            &mut map,
            "CLIP_STALE_PART_SECS",
            ingest.get("clip_stale_part_secs"),
        );
        set_map_if_unset(&mut map, "CLIP_MAX_SECS", ingest.get("clip_max_secs"));
        set_map_if_unset(
            &mut map,
            "CLIP_WRITE_TIMEOUT_MS",
            ingest.get("clip_write_timeout_ms"),
        );
        set_map_if_unset(
            &mut map,
            "CLIP_WRITE_RETRY_COUNT",
            ingest.get("clip_write_retry_count"),
        );
        set_map_if_unset(
            &mut map,
            "CLIP_WRITE_RETRY_BACKOFF_MS",
            ingest.get("clip_write_retry_backoff_ms"),
        );
        set_map_if_value(
            &mut map,
            "LIVE_PIPELINE_VERSION",
            ingest.get("live_pipeline_version"),
        );
        set_map_if_value(
            &mut map,
            "LIVE_HLS_SEGMENT_SECS",
            ingest.get("live_hls_segment_secs"),
        );
        set_map_if_value(
            &mut map,
            "LIVE_HLS_WINDOW_SECS",
            ingest.get("live_hls_window_secs"),
        );

        let forward = value.get("forward_agent").unwrap_or(&Value::Null);
        set_map_if_value(&mut map, "AGENT_MODE", forward.get("mode"));
        set_map_if_value(&mut map, "SERVER_ADDR", forward.get("server_addr"));
        set_map_if_value(
            &mut map,
            "MOTION_MERGE_SECS",
            forward.get("motion_merge_secs"),
        );
        set_map_if_value(
            &mut map,
            "HEARTBEAT_INTERVAL_SECS",
            forward.get("heartbeat_interval_secs"),
        );
        set_map_if_value(
            &mut map,
            "CONFIG_PULL_INTERVAL_SECS",
            forward.get("config_pull_interval_secs"),
        );

        let logging = value.get("logging").unwrap_or(&Value::Null);
        set_map_if_value(&mut map, "RUST_LOG", logging.get("rust_log"));

        let version = value.get("version").unwrap_or(&Value::Null);
        set_map_if_value(
            &mut map,
            "SENTINEL_VERSION",
            version.get("sentinel_version"),
        );

        if let Some(cameras) = value.get("cameras").and_then(|v| v.as_array()) {
            for (idx, cam) in cameras.iter().enumerate() {
                let slot = idx + 1;
                let prefix = format!("CAM{slot}_");
                let user = cam.get("user");
                let pass = cam.get("pass");
                let rtsp = cam.get("rtsp").unwrap_or(&Value::Null);
                let onvif = cam.get("onvif").unwrap_or(&Value::Null);
                let features = cam.get("features").unwrap_or(&Value::Null);

                let cam_id = cam.get("camera_id").or_else(|| cam.get("id"));
                let cam_agent_id = cam.get("agent_id").or(cam_id);
                set_map_if_value(&mut map, &format!("{prefix}CAMERA_ID"), cam_id);
                set_map_if_value(&mut map, &format!("{prefix}AGENT_ID"), cam_agent_id);
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}STREAM_ID"),
                    rtsp.get("stream_id").or_else(|| cam.get("stream_id")),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}TRANSPORT"),
                    cam.get("transport").or_else(|| rtsp.get("transport")),
                );

                set_map_if_string(&mut map, &format!("{prefix}RTSP_URL"), build_rtsp_url(rtsp));
                set_map_if_string(&mut map, &format!("{prefix}RTSP_HOST"), rtsp_host(rtsp));
                set_map_if_string(&mut map, &format!("{prefix}RTSP_PORT"), rtsp_port(rtsp));
                set_map_if_string(&mut map, &format!("{prefix}RTSP_PATH"), rtsp_path(rtsp));
                set_map_if_value(&mut map, &format!("{prefix}RTSP_USER"), user);
                set_map_if_value(&mut map, &format!("{prefix}RTSP_PASS"), pass);

                set_map_if_string(&mut map, &format!("{prefix}ONVIF_HOST"), onvif_host(onvif));
                set_map_if_string(&mut map, &format!("{prefix}ONVIF_PORT"), onvif_port(onvif));
                set_map_if_value(&mut map, &format!("{prefix}ONVIF_USER"), user);
                set_map_if_value(&mut map, &format!("{prefix}ONVIF_PASS"), pass);
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_DEBUG"),
                    onvif.get("debug"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_DUMP_XML"),
                    onvif.get("dump_xml"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_SUB_TERMINATION"),
                    onvif.get("sub_termination"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_RENEW_EVERY_SECS"),
                    onvif.get("renew_every_secs"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_PULL_TIMEOUT"),
                    onvif.get("pull_timeout"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_PULL_LIMIT"),
                    onvif.get("pull_limit"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_RESUBSCRIBE_AFTER_ERRORS"),
                    onvif.get("resubscribe_after_errors"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_MIN_POLL_GAP_MS"),
                    onvif.get("min_poll_gap_ms"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_AFTER_SUB_DELAY_MS"),
                    onvif.get("after_sub_delay_ms"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_CONNREFUSED_RETRIES"),
                    onvif.get("connrefused_retries"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}ONVIF_CONNREFUSED_BACKOFF_MS"),
                    onvif.get("connrefused_backoff_ms"),
                );
                set_map_if_bool(
                    &mut map,
                    &format!("{prefix}MOTION_ENABLED"),
                    motion_enabled_value(cam),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}LOCAL_CLIP_ENABLED"),
                    features.get("local_clip_enabled"),
                );
                set_map_if_value(
                    &mut map,
                    &format!("{prefix}RTSP_RECEIVER_ENABLED"),
                    features.get("rtsp_receiver_enabled"),
                );
            }

            if let Some(first) = cameras.first() {
                let user = first.get("user");
                let pass = first.get("pass");
                let rtsp = first.get("rtsp").unwrap_or(&Value::Null);
                let onvif = first.get("onvif").unwrap_or(&Value::Null);
                let features = first.get("features").unwrap_or(&Value::Null);

                let cam_id = first.get("camera_id").or_else(|| first.get("id"));
                let cam_agent_id = first.get("agent_id").or(cam_id);
                set_map_if_unset(&mut map, "CAMERA_ID", cam_id);
                set_map_if_unset(&mut map, "AGENT_ID", cam_agent_id);
                set_map_if_unset_bool(&mut map, "MOTION_ENABLED", motion_enabled_value(first));
                set_map_if_unset(
                    &mut map,
                    "LOCAL_CLIP_ENABLED",
                    features.get("local_clip_enabled"),
                );
                set_map_if_unset(
                    &mut map,
                    "RTSP_RECEIVER_ENABLED",
                    features.get("rtsp_receiver_enabled"),
                );
                set_map_if_unset_string(&mut map, "RTSP_URL", build_rtsp_url(rtsp));
                set_map_if_unset_string(&mut map, "RTSP_HOST", rtsp_host(rtsp));
                set_map_if_unset_string(&mut map, "RTSP_PORT", rtsp_port(rtsp));
                set_map_if_unset_string(&mut map, "RTSP_PATH", rtsp_path(rtsp));
                set_map_if_unset(&mut map, "CAM1_STREAM_ID", rtsp.get("stream_id"));
                set_map_if_unset(&mut map, "RTSP_USER", user);
                set_map_if_unset(&mut map, "RTSP_PASS", pass);
                set_map_if_unset_string(&mut map, "ONVIF_HOST", onvif_host(onvif));
                set_map_if_unset_string(&mut map, "ONVIF_PORT", onvif_port(onvif));
                set_map_if_unset(&mut map, "ONVIF_USER", user);
                set_map_if_unset(&mut map, "ONVIF_PASS", pass);
                set_map_if_unset(&mut map, "ONVIF_DEBUG", onvif.get("debug"));
                set_map_if_unset(&mut map, "ONVIF_DUMP_XML", onvif.get("dump_xml"));
                set_map_if_unset(
                    &mut map,
                    "ONVIF_SUB_TERMINATION",
                    onvif.get("sub_termination"),
                );
                set_map_if_unset(
                    &mut map,
                    "ONVIF_RENEW_EVERY_SECS",
                    onvif.get("renew_every_secs"),
                );
                set_map_if_unset(&mut map, "ONVIF_PULL_TIMEOUT", onvif.get("pull_timeout"));
                set_map_if_unset(&mut map, "ONVIF_PULL_LIMIT", onvif.get("pull_limit"));
                set_map_if_unset(
                    &mut map,
                    "ONVIF_RESUBSCRIBE_AFTER_ERRORS",
                    onvif.get("resubscribe_after_errors"),
                );
                set_map_if_unset(
                    &mut map,
                    "ONVIF_MIN_POLL_GAP_MS",
                    onvif.get("min_poll_gap_ms"),
                );
                set_map_if_unset(
                    &mut map,
                    "ONVIF_AFTER_SUB_DELAY_MS",
                    onvif.get("after_sub_delay_ms"),
                );
                set_map_if_unset(
                    &mut map,
                    "ONVIF_CONNREFUSED_RETRIES",
                    onvif.get("connrefused_retries"),
                );
                set_map_if_unset(
                    &mut map,
                    "ONVIF_CONNREFUSED_BACKOFF_MS",
                    onvif.get("connrefused_backoff_ms"),
                );
            }
        }

        if let Ok(mut guard) = overrides_map().write() {
            *guard = map;
        }
    }

    /// Backward-compatible alias.
    pub fn apply_json_env_overrides(value: &Value) {
        Self::install_runtime_overrides(value);
    }

    pub fn runtime_var(key: &str) -> Option<String> {
        overrides_map()
            .read()
            .ok()
            .and_then(|m| m.get(key).cloned())
    }

    pub fn runtime_bool(key: &str, default: bool) -> bool {
        Self::runtime_var(key)
            .and_then(|v| match v.as_str() {
                "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
                "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
                _ => None,
            })
            .unwrap_or(default)
    }

    pub fn runtime_u64(key: &str, default: u64) -> u64 {
        Self::runtime_var(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    pub fn runtime_u64_nonzero(key: &str, default: u64) -> u64 {
        Self::runtime_var(key)
            .and_then(|v| v.parse().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default)
    }

    pub fn clear_runtime_overrides() {
        if let Ok(mut guard) = overrides_map().write() {
            guard.clear();
        }
    }

    #[cfg(test)]
    pub fn runtime_test_lock() -> &'static Mutex<()> {
        RUNTIME_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(test)]
    pub fn runtime_snapshot() -> HashMap<String, String> {
        overrides_map()
            .read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub fn runtime_restore(snapshot: HashMap<String, String>) {
        if let Ok(mut guard) = overrides_map().write() {
            *guard = snapshot;
        }
    }

    pub fn merge_json_values(base: &mut Value, update: &Value) {
        merge_value(base, update);
    }

    pub fn merge_server_camera_configs(camera_value: &Value, server_value: &Value) -> Value {
        let mut merged = camera_value.clone();
        if let Some(server_section) = server_value.get("server") {
            if has_non_null(server_section) {
                if let Some(obj) = merged.as_object_mut() {
                    obj.insert("server".to_string(), server_section.clone());
                }
            }
        }
        merged
    }

    pub fn default_server_json() -> Value {
        serde_json::json!({
            "server": {
                "enabled": null,
                "base_url": null,
                "bearer_token": null,
                "retry_interval_secs": null,
                "max_retries": null
            }
        })
    }

    pub fn default_camera_json() -> Value {
        serde_json::json!({
            "cameras": [
                {
                    "id": null,
                    "user": null,
                    "pass": null,
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
                        "stream_id": null
                    },
                    "onvif": {
                        "url": null,
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
            "cleanup": {
                "interval_secs": null,
                "min_free_bytes": null
            },
            "ingest": {
                "clip_dir": null,
                "clip_pre_secs": null,
                "clip_post_secs": null,
                "clip_ring_secs": null,
                "clip_stale_part_secs": null,
                "clip_max_secs": null,
                "clip_write_timeout_ms": null,
                "clip_write_retry_count": null,
                "clip_write_retry_backoff_ms": null
            },
            "forward_agent": {
                "mode": null,
                "server_addr": null,
                "motion_merge_secs": null,
                "heartbeat_interval_secs": null,
                "config_pull_interval_secs": null
            },
            "logging": {
                "rust_log": null
            },
            "version": {
                "sentinel_version": null
            }
        })
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
        "ingest": {
            "clip_dir": null,
            "clip_pre_secs": null,
            "clip_post_secs": null,
            "clip_ring_secs": null,
            "clip_stale_part_secs": null,
            "clip_max_secs": null,
            "clip_write_timeout_ms": null,
            "clip_write_retry_count": null,
            "clip_write_retry_backoff_ms": null
        },
        "forward_agent": {
            "mode": null,
            "server_addr": null,
            "motion_merge_secs": null,
            "heartbeat_interval_secs": null,
            "config_pull_interval_secs": null
        },
        "cameras": [
            {
                "id": null,
                "user": null,
                "pass": null,
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
                    "stream_id": null
                },
                "onvif": {
                    "url": null,
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
    if let Some(enabled) = motion_enabled_value(value) {
        return enabled;
    }

    let cameras = value.get("cameras").and_then(|v| v.as_array());
    if let Some(cameras) = cameras {
        if let Some(first) = cameras.first() {
            if let Some(enabled) = motion_enabled_value(first) {
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
    let path = rtsp.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let path = if path.is_empty() {
        String::new()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };
    Some(format!("rtsp://{}:{}{}", host, port, path))
}

fn rtsp_host(rtsp: &Value) -> Option<String> {
    if let Some(host) = rtsp.get("host").and_then(|v| v.as_str()) {
        if !host.trim().is_empty() {
            return Some(host.to_string());
        }
    }
    let url = build_rtsp_url(rtsp)?;
    Url::parse(&url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
}

fn rtsp_port(rtsp: &Value) -> Option<String> {
    if let Some(port) = rtsp.get("port").and_then(value_to_string) {
        return Some(port);
    }
    let url = build_rtsp_url(rtsp)?;
    Url::parse(&url)
        .ok()
        .map(|parsed| parsed.port().unwrap_or(554).to_string())
}

fn rtsp_path(rtsp: &Value) -> Option<String> {
    if let Some(path) = rtsp.get("path").and_then(|v| v.as_str()) {
        if !path.trim().is_empty() {
            return Some(path.to_string());
        }
    }
    let url = build_rtsp_url(rtsp)?;
    let parsed = Url::parse(&url).ok()?;
    let path = parsed.path();
    if path.trim().is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

fn onvif_host(onvif: &Value) -> Option<String> {
    if let Some(host) = onvif.get("host").and_then(|v| v.as_str()) {
        if !host.trim().is_empty() {
            return Some(host.to_string());
        }
    }
    let url = onvif.get("url").and_then(|v| v.as_str())?;
    Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
}

fn onvif_port(onvif: &Value) -> Option<String> {
    if let Some(port) = onvif.get("port").and_then(value_to_string) {
        return Some(port);
    }
    let url = onvif.get("url").and_then(|v| v.as_str())?;
    Url::parse(url)
        .ok()
        .map(|parsed| parsed.port_or_known_default().unwrap_or(2020).to_string())
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

fn has_canonical_cameras(value: &Value) -> bool {
    value.get("cameras").and_then(Value::as_array).is_some()
}

fn strip_top_level_legacy_camera_fields(value: &mut Value) {
    let Some(map) = value.as_object_mut() else {
        return;
    };
    let legacy_keys = [
        "camera_id",
        "user",
        "pass",
        "stream_id",
        "transport",
        "motion",
        "motion_enabled",
        "features",
        "local_clip_enabled",
        "rtsp_receiver_enabled",
        "rtsp",
        "rtsp_url",
        "rtsp_host",
        "rtsp_port",
        "rtsp_path",
        "rtsp_user",
        "rtsp_pass",
        "rtp_port",
        "rtcp_port",
        "onvif",
        "onvif_url",
        "onvif_host",
        "onvif_port",
        "onvif_user",
        "onvif_pass",
        "onvif_debug",
        "onvif_dump_xml",
        "onvif_sub_termination",
        "onvif_renew_every_secs",
        "onvif_pull_timeout",
        "onvif_pull_limit",
        "onvif_resubscribe_after_errors",
        "onvif_min_poll_gap_ms",
        "onvif_after_sub_delay_ms",
        "onvif_connrefused_retries",
        "onvif_connrefused_backoff_ms",
        "cam1_camera_id",
        "cam1_stream_id",
        "cam1_transport",
        "cam1_rtsp_url",
        "cam1_rtsp_user",
        "cam1_rtsp_pass",
        "cam1_rtp_port",
        "cam1_rtcp_port",
        "cam2_camera_id",
        "cam2_stream_id",
        "cam2_transport",
        "cam2_rtsp_url",
        "cam2_rtsp_user",
        "cam2_rtsp_pass",
        "cam2_rtp_port",
        "cam2_rtcp_port",
    ];
    let keys_to_remove: Vec<String> = map
        .keys()
        .filter(|key| legacy_keys.contains(&key.to_lowercase().as_str()))
        .cloned()
        .collect();
    for key in keys_to_remove {
        map.remove(&key);
    }
}

fn has_non_null(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Array(items) => items.iter().any(has_non_null),
        Value::Object(map) => map.values().any(has_non_null),
        _ => true,
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

fn bool_to_runtime_string(value: bool) -> String {
    if value {
        "1".to_string()
    } else {
        "0".to_string()
    }
}

fn motion_enabled_value(value: &Value) -> Option<bool> {
    value
        .get("motion")
        .and_then(|motion| motion.get("enabled"))
        .and_then(Value::as_bool)
        .or_else(|| value.get("motion_enabled").and_then(Value::as_bool))
}

fn set_map_if_value(map: &mut HashMap<String, String>, key: &str, value: Option<&Value>) {
    if let Some(value) = value.and_then(value_to_string) {
        map.insert(key.to_string(), value);
    }
}

fn set_map_if_bool(map: &mut HashMap<String, String>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        map.insert(key.to_string(), bool_to_runtime_string(value));
    }
}

fn set_map_if_string(map: &mut HashMap<String, String>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        if !value.trim().is_empty() {
            map.insert(key.to_string(), value);
        }
    }
}

fn set_map_if_unset_string(map: &mut HashMap<String, String>, key: &str, value: Option<String>) {
    if map.contains_key(key) {
        return;
    }
    set_map_if_string(map, key, value);
}

fn set_map_if_unset(map: &mut HashMap<String, String>, key: &str, value: Option<&Value>) {
    if map.contains_key(key) {
        return;
    }
    set_map_if_value(map, key, value);
}

fn set_map_if_unset_bool(map: &mut HashMap<String, String>, key: &str, value: Option<bool>) {
    if map.contains_key(key) {
        return;
    }
    set_map_if_bool(map, key, value);
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
                max_retries: merge_u64(
                    server_val,
                    "max_retries",
                    defaults.server.max_retries as u64,
                ) as u32,
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

#[cfg(test)]
mod tests {
    use super::AgentConfig;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::MutexGuard;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        saved: HashMap<String, String>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let lock = AgentConfig::runtime_test_lock()
                .lock()
                .expect("lock runtime mutex");
            let saved = AgentConfig::runtime_snapshot();
            AgentConfig::clear_runtime_overrides();
            Self { _lock: lock, saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            AgentConfig::runtime_restore(std::mem::take(&mut self.saved));
        }
    }

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sentinel-agent-config-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn apply_json_env_overrides_applies_ingest_clip_settings() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "ingest": {
                "clip_dir": "/var/lib/sentinel_rtp_cam/clips",
                "clip_max_secs": 30,
                "clip_post_secs": 120,
                "clip_pre_secs": 2,
                "clip_ring_secs": 15,
                "clip_stale_part_secs": 3600,
                "clip_write_timeout_ms": 5000,
                "clip_write_retry_count": 2,
                "clip_write_retry_backoff_ms": 250,
                "live_pipeline_version": "v2",
                "live_hls_segment_secs": 2,
                "live_hls_window_secs": 12
            }
        });

        AgentConfig::apply_json_env_overrides(&cfg);

        assert_eq!(
            AgentConfig::runtime_var("CLIP_DIR").as_deref(),
            Some("/var/lib/sentinel_rtp_cam/clips")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_MAX_SECS").as_deref(),
            Some("30")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_POST_SECS").as_deref(),
            Some("120")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_PRE_SECS").as_deref(),
            Some("2")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_RING_SECS").as_deref(),
            Some("15")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_STALE_PART_SECS").as_deref(),
            Some("3600")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_WRITE_TIMEOUT_MS").as_deref(),
            Some("5000")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_WRITE_RETRY_COUNT").as_deref(),
            Some("2")
        );
        assert_eq!(
            AgentConfig::runtime_var("CLIP_WRITE_RETRY_BACKOFF_MS").as_deref(),
            Some("250")
        );
        assert_eq!(
            AgentConfig::runtime_var("LIVE_PIPELINE_VERSION").as_deref(),
            Some("v2")
        );
        assert_eq!(
            AgentConfig::runtime_var("LIVE_HLS_SEGMENT_SECS").as_deref(),
            Some("2")
        );
        assert_eq!(
            AgentConfig::runtime_var("LIVE_HLS_WINDOW_SECS").as_deref(),
            Some("12")
        );
    }

    #[test]
    fn apply_json_env_overrides_sets_multi_camera_runtime_slots() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "agent_id": "01KH1V1AMEP4NDDDJ0SRV067TW",
            "server": {
                "bearer_token": "agent-token"
            },
            "cameras": [
                {
                    "id": "ABLAK",
                    "rtsp": {
                        "url": "rtsp://192.168.1.187:554/stream2",
                        "stream_id": 1
                    },
                    "transport": "udp"
                },
                {
                    "id": "aff8812b-c6be-4e59-aefd-40b59b425d92",
                    "rtsp": {
                        "url": "rtsp://192.168.1.189:554/stream2",
                        "stream_id": 2
                    },
                    "transport": "udp"
                }
            ]
        });

        AgentConfig::apply_json_env_overrides(&cfg);

        assert_eq!(
            AgentConfig::runtime_var("CAM1_CAMERA_ID").as_deref(),
            Some("ABLAK")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM1_STREAM_ID").as_deref(),
            Some("1")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM2_CAMERA_ID").as_deref(),
            Some("aff8812b-c6be-4e59-aefd-40b59b425d92")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM2_STREAM_ID").as_deref(),
            Some("2")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM2_RTSP_URL").as_deref(),
            Some("rtsp://192.168.1.189:554/stream2")
        );
    }

    #[test]
    fn apply_json_env_overrides_maps_legacy_motion_enabled_fields() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "motion_enabled": false,
            "cameras": [
                {
                    "id": "ABLAK",
                    "motion_enabled": false,
                    "rtsp": {
                        "url": "rtsp://192.168.1.187:554/stream2",
                        "stream_id": 1
                    }
                },
                {
                    "id": "Room",
                    "motion_enabled": true,
                    "rtsp": {
                        "url": "rtsp://192.168.1.189:554/stream2",
                        "stream_id": 2
                    }
                }
            ]
        });

        AgentConfig::apply_json_env_overrides(&cfg);

        assert_eq!(
            AgentConfig::runtime_var("MOTION_ENABLED").as_deref(),
            Some("0")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM1_MOTION_ENABLED").as_deref(),
            Some("0")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM2_MOTION_ENABLED").as_deref(),
            Some("1")
        );
    }

    #[test]
    fn apply_json_env_overrides_maps_forward_agent_intervals() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "forward_agent": {
                "heartbeat_interval_secs": 10,
                "config_pull_interval_secs": 15
            }
        });

        AgentConfig::apply_json_env_overrides(&cfg);

        assert_eq!(
            AgentConfig::runtime_var("HEARTBEAT_INTERVAL_SECS").as_deref(),
            Some("10")
        );
        assert_eq!(
            AgentConfig::runtime_var("CONFIG_PULL_INTERVAL_SECS").as_deref(),
            Some("15")
        );
    }

    #[test]
    fn runtime_u64_nonzero_falls_back_to_default_for_zero_or_invalid() {
        let _guard = EnvGuard::new();
        let default = 30;

        assert_eq!(
            AgentConfig::runtime_u64_nonzero("HEARTBEAT_INTERVAL_SECS", default),
            default
        );

        AgentConfig::runtime_restore(
            [("HEARTBEAT_INTERVAL_SECS".to_string(), "".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            AgentConfig::runtime_u64_nonzero("HEARTBEAT_INTERVAL_SECS", default),
            default
        );

        AgentConfig::runtime_restore(
            [("HEARTBEAT_INTERVAL_SECS".to_string(), "0".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            AgentConfig::runtime_u64_nonzero("HEARTBEAT_INTERVAL_SECS", default),
            default
        );

        AgentConfig::runtime_restore(
            [("HEARTBEAT_INTERVAL_SECS".to_string(), "abc".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            AgentConfig::runtime_u64_nonzero("HEARTBEAT_INTERVAL_SECS", default),
            default
        );

        AgentConfig::runtime_restore(
            [("HEARTBEAT_INTERVAL_SECS".to_string(), "9".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            AgentConfig::runtime_u64_nonzero("HEARTBEAT_INTERVAL_SECS", default),
            9
        );
    }

    #[test]
    fn merge_remote_camera_json_strips_stale_top_level_legacy_camera_fields() {
        let dir = unique_test_dir("canonical-remote");
        fs::create_dir_all(&dir).expect("test dir");
        let camera_path = dir.join("camera.json");
        AgentConfig::write_json_file(
            &camera_path,
            &json!({
                "camera_id": "ABLAK",
                "rtsp_url": "rtsp://192.168.1.187:554/stream2",
                "onvif": { "host": "192.168.1.187", "port": 2020 },
                "cameras": [{
                    "id": "ABLAK",
                    "rtsp": { "url": "rtsp://192.168.1.187:554/stream2", "stream_id": 1 }
                }],
                "cleanup": { "min_free_bytes": 12345 }
            }),
        )
        .expect("write stale camera config");

        AgentConfig::merge_remote_camera_json(
            &camera_path,
            &json!({
                "cameras": [{
                    "id": "aff8812b-c6be-4e59-aefd-40b59b425d92",
                    "rtsp": { "url": "rtsp://192.168.1.189/stream2", "stream_id": 1 },
                    "onvif": { "url": "http://192.168.1.189:2020/onvif/service" }
                }]
            }),
        )
        .expect("merge remote config");

        let written = AgentConfig::load_camera_json(&camera_path).expect("read written config");
        assert!(written.get("camera_id").is_none());
        assert!(written.get("rtsp_url").is_none());
        assert!(written.get("onvif").is_none());
        assert_eq!(
            written["cameras"][0]["id"],
            "aff8812b-c6be-4e59-aefd-40b59b425d92"
        );
        assert_eq!(written["cleanup"]["min_free_bytes"], 12345);

        fs::remove_dir_all(dir).expect("remove test dir");
    }
}
