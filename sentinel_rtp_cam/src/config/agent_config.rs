use serde::{Deserialize, Serialize};
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

    /// Get retry interval as Duration
    pub fn retry_interval(&self) -> Duration {
        Duration::from_secs(self.server.retry_interval_secs)
    }

    /// Get cleanup interval as Duration
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup.interval_secs)
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
