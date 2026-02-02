use anyhow::Result;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::config::agent_config::ServerConfig;
use crate::core::clip_recorder::ClipMeta;
use crate::event::MotionEvent;

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
pub async fn run_heartbeat_poster(config: ServerConfig, camera_id: String) -> Result<()> {
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

        let payload = json!({
            "camera_id": camera_id,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

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
    use super::normalize_config_update;
    use serde_json::json;

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
}
