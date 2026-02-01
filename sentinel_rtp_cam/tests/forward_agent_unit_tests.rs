use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use chrono::Utc;

use sentinel_rtp_cam::event::MotionEvent;
use sentinel_rtp_cam::forward_agent::{
    basic_auth_value, build_stream_maps, forward_motion_event, load_cameras_from_env,
    parse_rtsp_url, CamConfig, MotionSender,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: HashMap<String, Option<String>>,
}

impl EnvGuard {
    fn new(pairs: &[(&str, &str)]) -> Self {
        let lock = ENV_LOCK.lock().unwrap();
        let mut saved = HashMap::new();
        for (key, value) in pairs {
            saved.insert((*key).to_string(), std::env::var(*key).ok());
            std::env::set_var(*key, value);
        }
        Self { _lock: lock, saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain() {
            match value {
                Some(v) => std::env::set_var(&key, v),
                None => std::env::remove_var(&key),
            }
        }
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
    // Scenario: per-camera RTSP credentials should override global defaults.
    let _guard = EnvGuard::new(&[
        ("CAM1_RTSP_URL", "rtsp://example.com/stream"),
        ("CAM1_STREAM_ID", "7"),
        ("CAM1_TRANSPORT", "tcp"),
        ("CAM1_CAMERA_ID", "cam-test-1"),
        ("RTSP_USER", "global_user"),
        ("RTSP_PASS", "global_pass"),
        ("CAM1_RTSP_USER", "cam_user"),
        ("CAM1_RTSP_PASS", "cam_pass"),
    ]);

    let cams = load_cameras_from_env().unwrap();
    assert_eq!(cams.len(), 1);
    assert_eq!(cams[0].rtsp_user.as_deref(), Some("cam_user"));
    assert_eq!(cams[0].rtsp_pass.as_deref(), Some("cam_pass"));
}

#[test]
fn load_cameras_from_env_falls_back_to_global_camera_id() {
    // Scenario: when CAM1_CAMERA_ID is missing, CAMERA_ID should be used for the first camera.
    let _guard = EnvGuard::new(&[
        ("CAM1_RTSP_URL", "rtsp://example.com/stream"),
        ("CAM1_STREAM_ID", "7"),
        ("CAM1_TRANSPORT", "tcp"),
        ("CAMERA_ID", "cam-global"),
    ]);

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
