use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, Response, Sse},
    routing::{get, post},
    Form, Json, Router,
};
use serde::Deserialize;
use std::{collections::HashSet, convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};

use sentinel_rtp_cam::AgentConfig;

const DEFAULT_SERVER_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/server.json";
const DEFAULT_CAMERA_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/camera.json";

fn runtime_var(name: &str) -> Option<String> {
    AgentConfig::runtime_var(name).filter(|v| !v.trim().is_empty())
}

#[derive(Clone)]
struct AppState {
    config_json_pretty: Arc<RwLock<String>>,
    config_json_compact: Arc<RwLock<String>>,
    config_tx: broadcast::Sender<String>,
    bearer_token: String,
    seen_events: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug, Deserialize)]
struct MotionEventPayload {
    camera_id: String,
    event_id: String,
    rule: String,
    active: bool,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct HeartbeatPayload {
    camera_id: String,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct ClipMetaPayload {
    camera_id: String,
    event_id: String,
    rule: String,
    file_path: String,
    file_size: u64,
    #[allow(dead_code)]
    started_at: String,
    #[allow(dead_code)]
    ended_at: String,
    duration_secs: i64,
}

// Auth middleware check
fn check_auth(headers: &HeaderMap, expected_token: &str) -> Result<(), StatusCode> {
    let auth_header = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !auth_header.starts_with("Bearer ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = &auth_header[7..];
    if token != expected_token {
        warn!("Invalid bearer token: {}", token);
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(())
}

// SSE config stream handler
async fn sse_config_stream(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<axum::response::sse::Event, Infallible>>> {
    info!("Client connected to SSE config stream");

    let config_rx = state.config_tx.subscribe();
    let initial_config = state.config_json_compact.read().await.clone();

    // Create stream that starts with current config, then broadcasts
    let stream = async_stream::stream! {
        // Send initial config (compact JSON, no newlines)
        yield Ok(axum::response::sse::Event::default()
            .event("config")
            .data(initial_config));

        // Then forward all broadcasts (already compact)
        let mut broadcast_stream = BroadcastStream::new(config_rx);
        while let Some(Ok(config_json)) = broadcast_stream.next().await {
            yield Ok(axum::response::sse::Event::default()
                .event("config")
                .data(config_json));
        }
    };

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

// Admin page handler
async fn admin_page(State(state): State<AppState>) -> Html<String> {
    let config = state.config_json_pretty.read().await;
    Html(format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>Dummy Server Admin</title>
    <style>
        body {{ font-family: monospace; margin: 40px; background: #1e1e1e; color: #d4d4d4; }}
        h1 {{ color: #4ec9b0; }}
        textarea {{ 
            width: 100%; 
            height: 400px; 
            font-family: monospace; 
            font-size: 14px;
            background: #252526;
            color: #d4d4d4;
            border: 1px solid #3c3c3c;
            padding: 10px;
        }}
        button {{ 
            margin-top: 10px; 
            padding: 10px 20px; 
            font-size: 16px;
            background: #007acc;
            color: white;
            border: none;
            cursor: pointer;
        }}
        button:hover {{ background: #005a9e; }}
        .info {{ color: #6a9955; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <h1>Dummy Server - Config Editor</h1>
    <div class="info">
        Edit the JSON config below and submit. Changes will be broadcast to all connected agents via SSE.
    </div>
    <form method="post" action="/admin/config">
        <textarea name="config">{}</textarea><br>
        <button type="submit">Update Config & Broadcast</button>
    </form>
    <div class="info" style="margin-top: 20px;">
        <strong>Endpoints:</strong><br>
        • SSE: GET /api/v1/config/stream?camera_id=CAM_ID<br>
        • Motion: POST /api/events/motion<br>
        • Heartbeat: POST /api/heartbeat<br>
        • Clips: POST /api/clips<br>
    </div>
</body>
</html>"#,
        html_escape(&config)
    ))
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[derive(Debug, Deserialize)]
struct ConfigForm {
    config: String,
}

// Admin config update handler
async fn admin_update_config(
    State(state): State<AppState>,
    Form(form): Form<ConfigForm>,
) -> Result<Response, StatusCode> {
    let config_json = form.config;

    // Validate JSON and parse
    let config_value: serde_json::Value =
        serde_json::from_str(&config_json).map_err(|_| StatusCode::BAD_REQUEST)?;

    info!("Config updated via admin");

    // Create pretty version for admin UI
    let pretty_json = serde_json::to_string_pretty(&config_value)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create compact version for SSE (no newlines)
    let compact_json =
        serde_json::to_string(&config_value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Update stored configs
    *state.config_json_pretty.write().await = pretty_json;
    *state.config_json_compact.write().await = compact_json.clone();

    // Broadcast compact version to SSE clients
    let _ = state.config_tx.send(compact_json);

    // Redirect back to admin page
    Ok(Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header("Location", "/admin")
        .body(Body::empty())
        .unwrap())
}

// Motion event handler
async fn handle_motion_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<MotionEventPayload>,
) -> Result<StatusCode, StatusCode> {
    check_auth(&headers, &state.bearer_token)?;

    // Create dedupe key
    let dedupe_key = format!(
        "{}:{}:{}",
        payload.camera_id, payload.event_id, payload.active
    );

    let mut seen = state.seen_events.lock().await;
    let is_new = seen.insert(dedupe_key);

    if is_new {
        info!(
            camera_id = %payload.camera_id,
            event_id = %payload.event_id,
            rule = %payload.rule,
            active = payload.active,
            timestamp = %payload.timestamp,
            "Motion event received"
        );
    } else {
        warn!(
            camera_id = %payload.camera_id,
            event_id = %payload.event_id,
            "Duplicate motion event ignored"
        );
    }

    Ok(StatusCode::OK)
}

// Heartbeat handler
async fn handle_heartbeat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<HeartbeatPayload>,
) -> Result<StatusCode, StatusCode> {
    check_auth(&headers, &state.bearer_token)?;

    info!(
        camera_id = %payload.camera_id,
        timestamp = %payload.timestamp,
        "Heartbeat received"
    );

    Ok(StatusCode::OK)
}

// Clip metadata handler
async fn handle_clip_meta(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ClipMetaPayload>,
) -> Result<StatusCode, StatusCode> {
    check_auth(&headers, &state.bearer_token)?;

    info!(
        camera_id = %payload.camera_id,
        event_id = %payload.event_id,
        rule = %payload.rule,
        file_path = %payload.file_path,
        file_size_mb = payload.file_size / 1_000_000,
        duration_secs = payload.duration_secs,
        "Clip metadata received"
    );

    Ok(StatusCode::OK)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server_value = AgentConfig::load_server_json(std::path::Path::new(DEFAULT_SERVER_CONFIG_PATH))
        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
    let camera_value = AgentConfig::load_camera_json(std::path::Path::new(DEFAULT_CAMERA_CONFIG_PATH))
        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
    let config_value = AgentConfig::merge_server_camera_configs(&camera_value, &server_value);
    AgentConfig::apply_json_env_overrides(&config_value);

    // Initialize tracing
    let log_filter = runtime_var("RUST_LOG").unwrap_or_else(|| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
        .with_target(false)
        .init();

    let bind_addr = runtime_var("DUMMY_SERVER_BIND").unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let bearer_token =
        runtime_var("DUMMY_SERVER_TOKEN").unwrap_or_else(|| "test-token-12345".to_string());

    // Default config
    let default_config = serde_json::json!({
        "camera_id": "cam-001",
        "motion": {
            "enabled": true
        },
        "server": {
            "enabled": true,
            "base_url": "http://127.0.0.1:8080",
            "bearer_token": bearer_token,
            "retry_interval_secs": 30
        },
        "cleanup": {
            "interval_secs": 60,
            "min_free_bytes": 1073741824
        }
    });

    let (config_tx, _) = broadcast::channel(16);

    let state = AppState {
        config_json_pretty: Arc::new(RwLock::new(serde_json::to_string_pretty(&default_config)?)),
        config_json_compact: Arc::new(RwLock::new(serde_json::to_string(&default_config)?)),
        config_tx,
        bearer_token: bearer_token.clone(),
        seen_events: Arc::new(Mutex::new(HashSet::new())),
    };

    let app = Router::new()
        .route("/api/v1/config/stream", get(sse_config_stream))
        .route("/api/events/motion", post(handle_motion_event))
        .route("/api/heartbeat", post(handle_heartbeat))
        .route("/api/clips", post(handle_clip_meta))
        .route("/admin", get(admin_page))
        .route("/admin/config", post(admin_update_config))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║         Dummy Server for Sentinel Agent Testing         ║");
    info!("╚══════════════════════════════════════════════════════════╝");
    info!("");
    info!("Server listening on: http://{}", bind_addr);
    info!("Bearer token: {}", bearer_token);
    info!("");
    info!("Admin UI:        http://{}/admin", bind_addr);
    info!("SSE config:      http://{}/api/v1/config/stream", bind_addr);
    info!(
        "Motion events:   POST http://{}/api/events/motion",
        bind_addr
    );
    info!("Heartbeat:       POST http://{}/api/heartbeat", bind_addr);
    info!("Clip metadata:   POST http://{}/api/clips", bind_addr);
    info!("");
    info!("Configure agent with:");
    info!("  SERVER_BASE_URL=http://{}", bind_addr);
    info!("  SERVER_BEARER_TOKEN={}", bearer_token);
    info!("");

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
