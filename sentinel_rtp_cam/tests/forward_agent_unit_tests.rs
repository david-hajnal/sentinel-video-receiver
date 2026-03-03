use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use chrono::Utc;
use serde_json::json;

use sentinel_rtp_cam::event::MotionEvent;
use sentinel_rtp_cam::forward_agent::{
    basic_auth_value, build_stream_maps, forward_motion_event, load_cameras_from_env,
    parse_rtsp_url, CamConfig, MotionSender,
};
use sentinel_rtp_cam::AgentConfig;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn new(config: serde_json::Value) -> Self {
        let lock = ENV_LOCK.lock().unwrap();
        AgentConfig::clear_runtime_overrides();
        AgentConfig::apply_json_env_overrides(&config);
        Self { _lock: lock }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        AgentConfig::clear_runtime_overrides();
    }
}

#[derive(Clone, Debug)]
struct MotionCall {
    stream_id: u32,
    rule: String,
    active: bool,
    ts: String,
    camera_id: String,
    event_id: String,
}

#[derive(Default)]
struct MockSender {
    calls: Mutex<Vec<MotionCall>>,
}

impl MotionSender for MockSender {
    fn send_motion(
        &self,
        stream_id: u32,
        rule: String,
        active: bool,
        ts: String,
        camera_id: String,
        event_id: String,
    ) {
        self.calls.lock().unwrap().push(MotionCall {
            stream_id,
            rule,
            active,
            ts,
            camera_id,
            event_id,
        });
    }
}

#[test]
fn basic_auth_value_builds_basic_header() {
    // Scenario: basic auth header should be correctly base64-encoded.
    let header = basic_auth_value("user", "pass");
    assert_eq!(header, "Basic dXNlcjpwYXNz");
}

#[test]
fn parse_rtsp_url_parses_components() {
    // Scenario: RTSP URL should parse host, port, and path.
    let (host, port, path) = parse_rtsp_url("rtsp://example.com:8554/stream").unwrap();
    assert_eq!(host, "example.com");
    assert_eq!(port, 8554);
    assert_eq!(path, "/stream");
}

#[test]
fn load_cameras_from_env_uses_per_camera_override() {
    // Scenario: camera-level credentials from JSON runtime config should be used.
    let _guard = EnvGuard::new(json!({
        "server": {
            "bearer_token": "test-token"
        },
        "cameras": [
            {
                "id": "cam-test-1",
                "user": "cam_user",
                "pass": "cam_pass",
                "transport": "tcp",
                "rtsp": {
                    "url": "rtsp://example.com/stream",
                    "stream_id": 7
                }
            }
        ]
    }));

    let cams = load_cameras_from_env().unwrap();
    assert_eq!(cams.len(), 1);
    assert_eq!(cams[0].rtsp_user.as_deref(), Some("cam_user"));
    assert_eq!(cams[0].rtsp_pass.as_deref(), Some("cam_pass"));
}

#[test]
fn load_cameras_from_env_falls_back_to_global_camera_id() {
    // Scenario: when CAM1_CAMERA_ID is missing, CAMERA_ID should be used for the first camera.
    let _guard = EnvGuard::new(json!({
        "camera_id": "cam-global",
        "server": {
            "bearer_token": "test-token"
        },
        "cameras": [
            {
                "user": "cam_user",
                "pass": "cam_pass",
                "transport": "tcp",
                "rtsp": {
                    "url": "rtsp://example.com/stream",
                    "stream_id": 7
                }
            }
        ]
    }));

    let cams = load_cameras_from_env().unwrap();
    assert_eq!(cams.len(), 1);
    assert_eq!(cams[0].camera_id, "cam-global");
}

#[test]
fn build_stream_maps_sets_camera_mapping() {
    // Scenario: camera_id and stream_id should map in both directions.
    let cams = vec![
        CamConfig {
            name: "cam1".to_string(),
            rtsp_url: "rtsp://example.com/stream".to_string(),
            rtsp_user: None,
            rtsp_pass: None,
            stream_id: 1,
            transport: "tcp".to_string(),
            rtp_port: 5004,
            rtcp_port: 5005,
            camera_id: "cam-a".to_string(),
            agent_id: "agent-a".to_string(),
            agent_token: "token-a".to_string(),
        },
        CamConfig {
            name: "cam2".to_string(),
            rtsp_url: "rtsp://example.com/stream2".to_string(),
            rtsp_user: None,
            rtsp_pass: None,
            stream_id: 2,
            transport: "tcp".to_string(),
            rtp_port: 5006,
            rtcp_port: 5007,
            camera_id: "cam-b".to_string(),
            agent_id: "agent-b".to_string(),
            agent_token: "token-b".to_string(),
        },
    ];

    let (stream_map, camera_map) = build_stream_maps(&cams);
    assert_eq!(stream_map.get(&1).map(String::as_str), Some("cam1"));
    assert_eq!(stream_map.get(&2).map(String::as_str), Some("cam2"));
    assert_eq!(camera_map.get("cam-a").copied(), Some(1));
    assert_eq!(camera_map.get("cam-b").copied(), Some(2));
}

#[test]
fn forward_motion_event_sends_expected_payload() {
    // Scenario: forwarded motion event should preserve rule, ids, and timestamps.
    let sender = MockSender::default();
    let mut cam_map = HashMap::new();
    cam_map.insert("cam-1".to_string(), 3);

    let event = MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts: Utc::now(),
        camera_id: "cam-1".to_string(),
        event_id: "event-123".to_string(),
    };

    let stream_id = forward_motion_event(&sender, &cam_map, &event);
    assert_eq!(stream_id, 3);

    let calls = sender.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.stream_id, 3);
    assert_eq!(call.rule, "rule-1");
    assert_eq!(call.active, true);
    assert_eq!(call.camera_id, "cam-1");
    assert_eq!(call.event_id, "event-123");
    assert_eq!(call.ts, event.ts.to_rfc3339());
}

#[test]
fn forward_motion_event_falls_back_to_first_stream() {
    // Scenario: unknown camera_id should fall back to the first known stream_id.
    let sender = MockSender::default();
    let mut cam_map = HashMap::new();
    cam_map.insert("cam-1".to_string(), 5);

    let event = MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts: Utc::now(),
        camera_id: "cam-unknown".to_string(),
        event_id: "event-xyz".to_string(),
    };

    let stream_id = forward_motion_event(&sender, &cam_map, &event);
    assert_eq!(stream_id, 5);
}
