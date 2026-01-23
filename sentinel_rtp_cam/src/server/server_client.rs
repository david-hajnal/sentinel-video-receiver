use anyhow::Result;
use serde_json::json;
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
        let payload = json!({
            "event_id": clip.event_id,
            "filename": clip.file_path.display().to_string(),
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
                        upload_clip_file(&client_clone, &upload_url, &config_clone.bearer_token, &file_path).await
                    },
                    Duration::from_secs(config_clone.retry_interval_secs),
                    "upload_clip_file",
                )
                .await;
                true
            } else {
                match retry_with_limit(
                    || async {
                        upload_clip_file(&client_clone, &upload_url, &config_clone.bearer_token, &file_path).await
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
    let mut file = File::open(file_path).await
        .map_err(|e| anyhow::anyhow!("Failed to open clip file: {}", e))?;
    
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await
        .map_err(|e| anyhow::anyhow!("Failed to read clip file: {}", e))?;

    // Get filename
    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;

    // Create multipart form
    let part = reqwest::multipart::Part::bytes(buffer)
        .file_name(filename.to_string())
        .mime_str("video/mp4")?;

    let form = reqwest::multipart::Form::new()
        .part("file", part);

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
) -> Result<tokio::sync::watch::Receiver<serde_json::Value>> {
    info!(
        server_url = %config.base_url,
        camera_id = %camera_id,
        "Starting SSE config listener"
    );

    let (tx, rx) = tokio::sync::watch::channel(json!({}));

    let url = format!(
        "{}/api/config/stream?camera_id={}",
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
                            if let Ok(config_update) =
                                serde_json::from_str::<serde_json::Value>(trimmed)
                            {
                                info!(
                                    config = %serde_json::to_string(&config_update).unwrap_or_default(),
                                    "Received config update from SSE stream"
                                );
                                let _ = tx.send(config_update);
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
