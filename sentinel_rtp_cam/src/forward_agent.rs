use anyhow::{anyhow, bail, Result};
use base64::Engine;
use std::collections::HashMap;
use url::Url;

use crate::agent_uplink::Uplink;
use crate::event::MotionEvent;

#[derive(Debug, Clone)]
pub struct CamConfig {
    pub name: String,
    pub rtsp_url: String,
    pub rtsp_user: Option<String>,
    pub rtsp_pass: Option<String>,
    pub stream_id: u32,
    pub transport: String,
    pub rtp_port: u16,
    pub rtcp_port: u16,
    pub camera_id: String,
    pub agent_id: String,
    pub agent_token: String,
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

pub fn load_cameras_from_env() -> Result<Vec<CamConfig>> {
    let default_camera_id = env_string("CAMERA_ID");
    let mut cams = Vec::new();
    for i in 1..=4 {
        let prefix = format!("CAM{}_", i);
        let rtsp = match env_string(&format!("{prefix}RTSP_URL")) {
            Some(v) => v,
            None => continue,
        };
        let user = env_string(&format!("{prefix}RTSP_USER")).or_else(|| env_string("RTSP_USER"));
        let pass = env_string(&format!("{prefix}RTSP_PASS")).or_else(|| env_string("RTSP_PASS"));
        let stream_id: u32 = env_string(&format!("{prefix}STREAM_ID"))
            .ok_or_else(|| anyhow!("Missing {prefix}STREAM_ID"))?
            .parse()?;
        let transport =
            env_string(&format!("{prefix}TRANSPORT")).unwrap_or_else(|| "tcp".to_string());
        let rtp_port: u16 = env_string(&format!("{prefix}RTP_PORT"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(5004 + (i as u16 - 1) * 2);
        let rtcp_port: u16 = env_string(&format!("{prefix}RTCP_PORT"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(rtp_port + 1);
        let camera_id = env_string(&format!("{prefix}CAMERA_ID"))
            .or_else(|| {
                if i == 1 {
                    default_camera_id.clone()
                } else {
                    None
                }
            })
            .unwrap_or_else(|| format!("cam-{}", i));
        let agent_id =
            env_string(&format!("{prefix}AGENT_ID")).unwrap_or_else(|| camera_id.clone());
        let agent_token = env_string(&format!("{prefix}AGENT_TOKEN"))
            .or_else(|| env_string("SERVER_BEARER_TOKEN"))
            .or_else(|| env_string("AGENT_TOKEN"))
            .or_else(|| env_string("SERVER_TOKEN"))
            .ok_or_else(|| anyhow!("Missing AGENT_TOKEN/SERVER_BEARER_TOKEN for {prefix}"))?;

        cams.push(CamConfig {
            name: format!("cam{}", i),
            rtsp_url: rtsp,
            rtsp_user: user,
            rtsp_pass: pass,
            stream_id,
            transport: transport.to_lowercase(),
            rtp_port,
            rtcp_port,
            camera_id,
            agent_id,
            agent_token,
        });
    }
    if cams.is_empty() {
        bail!("No cameras configured (CAM1_RTSP_URL...)");
    }
    Ok(cams)
}

pub fn basic_auth_value(user: &str, pass: &str) -> String {
    let token = format!("{user}:{pass}");
    let b64 = base64::engine::general_purpose::STANDARD.encode(token.as_bytes());
    format!("Basic {b64}")
}

pub fn parse_rtsp_url(rtsp_url: &str) -> Result<(String, u16, String)> {
    let url = Url::parse(rtsp_url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("RTSP URL missing host"))?;
    let port = url.port().unwrap_or(554);
    Ok((host.to_string(), port, url.path().to_string()))
}

pub fn build_stream_maps(cams: &[CamConfig]) -> (HashMap<u32, String>, HashMap<String, u32>) {
    let mut stream_map = HashMap::new();
    let mut camera_to_stream = HashMap::new();
    for cam in cams {
        stream_map.insert(cam.stream_id, cam.name.clone());
        camera_to_stream.insert(cam.camera_id.clone(), cam.stream_id);
    }
    (stream_map, camera_to_stream)
}

pub fn resolve_stream_id(camera_id: &str, cam_map: &HashMap<String, u32>) -> u32 {
    cam_map
        .get(camera_id)
        .copied()
        .or_else(|| cam_map.values().next().copied())
        .unwrap_or(1)
}

pub trait MotionSender: Send + Sync {
    fn send_motion(
        &self,
        stream_id: u32,
        rule: String,
        active: bool,
        ts: String,
        camera_id: String,
        event_id: String,
    );
}

impl MotionSender for Uplink {
    fn send_motion(
        &self,
        stream_id: u32,
        rule: String,
        active: bool,
        ts: String,
        camera_id: String,
        event_id: String,
    ) {
        self.send_motion(stream_id, rule, active, ts, camera_id, event_id);
    }
}

pub fn forward_motion_event<S: MotionSender>(
    sender: &S,
    cam_map: &HashMap<String, u32>,
    event: &MotionEvent,
) -> u32 {
    let stream_id = resolve_stream_id(&event.camera_id, cam_map);
    sender.send_motion(
        stream_id,
        event.rule.clone(),
        event.active,
        event.ts.to_rfc3339(),
        event.camera_id.clone(),
        event.event_id.clone(),
    );
    stream_id
}

pub use crate::motion_event_latch::MotionEventIdLatch;
