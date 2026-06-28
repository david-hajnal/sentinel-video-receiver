use anyhow::{anyhow, bail, Result};
use base64::Engine;
use chrono::Utc;
use quick_xml::events::Event as XmlEvent;
use quick_xml::Reader;
use rand::{rngs::OsRng, RngCore};
use reqwest::header::{HeaderMap, HeaderValue, CONNECTION, CONTENT_TYPE};
use std::time::Duration;
use tracing::{info, warn};

use crate::config::AgentConfig;

const DEVICE_GET_SERVICES_ACTION: &str = "http://www.onvif.org/ver10/device/wsdl/GetServices";
const MEDIA_GET_PROFILES_ACTION: &str = "http://www.onvif.org/ver10/media/wsdl/GetProfiles";
const IMAGING_SET_IMAGING_SETTINGS_ACTION: &str =
    "http://www.onvif.org/ver20/imaging/wsdl/SetImagingSettings";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NightVisionMode {
    Auto,
    Day,
    Night,
}

impl NightVisionMode {
    fn from_runtime_value(value: &str) -> Option<Self> {
        match value.trim() {
            "auto" => Some(Self::Auto),
            "day" => Some(Self::Day),
            "night" => Some(Self::Night),
            _ => None,
        }
    }

    fn as_ir_cut_filter(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::Day => "Off",
            Self::Night => "On",
        }
    }

    fn as_config_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Day => "day",
            Self::Night => "night",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnvifNightVisionCameraConfig {
    pub camera_id: String,
    pub service_url: String,
    pub username: String,
    pub password: String,
    pub mode: NightVisionMode,
}

pub fn load_onvif_night_vision_cameras_from_env() -> Result<Vec<OnvifNightVisionCameraConfig>> {
    let mut cameras = Vec::new();

    for i in 1..=4 {
        let prefix = format!("CAM{}_ONVIF_", i);
        let Some(host) = AgentConfig::runtime_var(&format!("{prefix}HOST")) else {
            continue;
        };
        if host.trim().is_empty() {
            continue;
        }
        let Some(mode_raw) = AgentConfig::runtime_var(&format!("{prefix}NIGHT_VISION_MODE")) else {
            continue;
        };
        let Some(mode) = NightVisionMode::from_runtime_value(&mode_raw) else {
            continue;
        };
        let port = AgentConfig::runtime_var(&format!("{prefix}PORT"))
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(2020);
        let username = AgentConfig::runtime_var(&format!("{prefix}USER"))
            .ok_or_else(|| anyhow!("Missing CAM{}_ONVIF_USER", i))?;
        let password = AgentConfig::runtime_var(&format!("{prefix}PASS"))
            .ok_or_else(|| anyhow!("Missing CAM{}_ONVIF_PASS", i))?;
        let camera_id = AgentConfig::runtime_var(&format!("CAM{}_CAMERA_ID", i))
            .unwrap_or_else(|| format!("cam-{}", i));
        cameras.push(OnvifNightVisionCameraConfig {
            camera_id,
            service_url: format!("http://{}:{}/onvif/service", host, port),
            username,
            password,
            mode,
        });
    }

    if !cameras.is_empty() {
        return Ok(cameras);
    }

    let Some(host) = AgentConfig::runtime_var("ONVIF_HOST") else {
        return Ok(Vec::new());
    };
    if host.trim().is_empty() {
        return Ok(Vec::new());
    }
    let Some(mode_raw) = AgentConfig::runtime_var("ONVIF_NIGHT_VISION_MODE") else {
        return Ok(Vec::new());
    };
    let Some(mode) = NightVisionMode::from_runtime_value(&mode_raw) else {
        return Ok(Vec::new());
    };
    let port = AgentConfig::runtime_var("ONVIF_PORT")
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(2020);
    let username =
        AgentConfig::runtime_var("ONVIF_USER").ok_or_else(|| anyhow!("Missing ONVIF_USER"))?;
    let password =
        AgentConfig::runtime_var("ONVIF_PASS").ok_or_else(|| anyhow!("Missing ONVIF_PASS"))?;
    let camera_id = AgentConfig::runtime_var("CAMERA_ID")
        .or_else(|| AgentConfig::runtime_var("CAM1_CAMERA_ID"))
        .unwrap_or_else(|| "unknown-camera".to_string());

    Ok(vec![OnvifNightVisionCameraConfig {
        camera_id,
        service_url: format!("http://{}:{}/onvif/service", host, port),
        username,
        password,
        mode,
    }])
}

pub async fn run_onvif_night_vision_sync_once() -> Result<()> {
    let cameras = load_onvif_night_vision_cameras_from_env()?;
    if cameras.is_empty() {
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .pool_idle_timeout(Duration::from_secs(1))
        .pool_max_idle_per_host(0)
        .tcp_keepalive(Duration::from_secs(20))
        .build()?;

    for camera in cameras {
        match apply_night_vision_mode(&client, &camera).await {
            Ok(()) => info!(
                camera_id = %camera.camera_id,
                mode = %camera.mode.as_config_value(),
                "Applied ONVIF night vision mode"
            ),
            Err(error) => warn!(
                error = %error,
                camera_id = %camera.camera_id,
                mode = %camera.mode.as_config_value(),
                "Failed to apply ONVIF night vision mode"
            ),
        }
    }

    Ok(())
}

async fn apply_night_vision_mode(
    client: &reqwest::Client,
    camera: &OnvifNightVisionCameraConfig,
) -> Result<()> {
    let services_xml = soap_call(
        client,
        &camera.service_url,
        &camera.username,
        &camera.password,
        DEVICE_GET_SERVICES_ACTION,
        "<tds:GetServices><tds:IncludeCapability>true</tds:IncludeCapability></tds:GetServices>",
    )
    .await?;

    let media_url = parse_service_endpoint(&services_xml, "media")
        .unwrap_or_else(|| camera.service_url.clone());
    let imaging_url = parse_service_endpoint(&services_xml, "imaging")
        .unwrap_or_else(|| camera.service_url.clone());

    let profiles_xml = soap_call(
        client,
        &media_url,
        &camera.username,
        &camera.password,
        MEDIA_GET_PROFILES_ACTION,
        "<trt:GetProfiles/>",
    )
    .await?;

    let video_source_token = parse_first_video_source_token(&profiles_xml)
        .ok_or_else(|| anyhow!("GetProfiles returned no VideoSourceConfiguration SourceToken"))?;
    let body = build_set_imaging_settings_body(&video_source_token, camera.mode);

    let _ = soap_call(
        client,
        &imaging_url,
        &camera.username,
        &camera.password,
        IMAGING_SET_IMAGING_SETTINGS_ACTION,
        &body,
    )
    .await?;

    Ok(())
}

fn build_set_imaging_settings_body(video_source_token: &str, mode: NightVisionMode) -> String {
    format!(
        r#"<timg:SetImagingSettings><timg:VideoSourceToken>{}</timg:VideoSourceToken><timg:ImagingSettings><tt:IrCutFilter>{}</tt:IrCutFilter></timg:ImagingSettings><timg:ForcePersistence>true</timg:ForcePersistence></timg:SetImagingSettings>"#,
        xml_escape(video_source_token),
        mode.as_ir_cut_filter()
    )
}

fn parse_service_endpoint(xml: &str, service_kind: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut in_service = false;
    let mut current_tag: Option<String> = None;
    let mut namespace: Option<String> = None;
    let mut xaddr: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(e)) => {
                let local = local_name(e.name().as_ref());
                if local == "Service" {
                    in_service = true;
                    namespace = None;
                    xaddr = None;
                } else if in_service {
                    current_tag = Some(local);
                }
            }
            Ok(XmlEvent::Text(t)) => {
                if !in_service {
                    buf.clear();
                    continue;
                }
                let value = t.unescape().ok()?.to_string();
                match current_tag.as_deref() {
                    Some("Namespace") => namespace = Some(value),
                    Some("XAddr") => xaddr = Some(value),
                    _ => {}
                }
            }
            Ok(XmlEvent::End(e)) => {
                let local = local_name(e.name().as_ref());
                if local == "Service" {
                    if service_matches(namespace.as_deref(), service_kind) {
                        return xaddr.filter(|value| !value.trim().is_empty());
                    }
                    in_service = false;
                    namespace = None;
                    xaddr = None;
                }
                current_tag = None;
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    None
}

fn parse_first_video_source_token(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut capture_text = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(e)) => {
                capture_text = local_name(e.name().as_ref()) == "SourceToken";
            }
            Ok(XmlEvent::Text(t)) => {
                if capture_text {
                    let value = t.unescape().ok()?.trim().to_string();
                    if !value.is_empty() {
                        return Some(value);
                    }
                }
            }
            Ok(XmlEvent::End(_)) => capture_text = false,
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    None
}

async fn soap_call(
    client: &reqwest::Client,
    url: &str,
    username: &str,
    password: &str,
    action: &str,
    body: &str,
) -> Result<String> {
    let security = wsse_header(username, password);
    let envelope = soap_envelope(url, action, &security, body);

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!(
            r#"application/soap+xml; charset=utf-8; action="{}""#,
            action
        ))?,
    );
    headers.insert(
        "SOAPAction",
        HeaderValue::from_str(&format!(r#""{}""#, action))?,
    );
    headers.insert(CONNECTION, HeaderValue::from_static("close"));

    let response = client
        .post(url)
        .headers(headers)
        .body(envelope)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("SOAP HTTP {}: {}", status, text);
    }
    Ok(text)
}

fn wsse_header(username: &str, password: &str) -> String {
    use sha1::{Digest, Sha1};

    let mut nonce = [0u8; 20];
    OsRng.fill_bytes(&mut nonce);

    let created = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut sha1 = Sha1::new();
    sha1.update(nonce);
    sha1.update(created.as_bytes());
    sha1.update(password.as_bytes());
    let digest = base64::engine::general_purpose::STANDARD.encode(sha1.finalize());
    let nonce_b64 = base64::engine::general_purpose::STANDARD.encode(nonce);

    format!(
        r#"<wsse:Security SOAP-ENV:mustUnderstand="1"
  xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
  xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
  <wsse:UsernameToken>
    <wsse:Username>{}</wsse:Username>
    <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{}</wsse:Password>
    <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{}</wsse:Nonce>
    <wsu:Created>{}</wsu:Created>
  </wsse:UsernameToken>
</wsse:Security>"#,
        xml_escape(username),
        xml_escape(&digest),
        xml_escape(&nonce_b64),
        xml_escape(&created)
    )
}

fn soap_envelope(to: &str, action: &str, security: &str, body: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope
  xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope"
  xmlns:wsa="http://www.w3.org/2005/08/addressing"
  xmlns:tds="http://www.onvif.org/ver10/device/wsdl"
  xmlns:trt="http://www.onvif.org/ver10/media/wsdl"
  xmlns:timg="http://www.onvif.org/ver20/imaging/wsdl"
  xmlns:tt="http://www.onvif.org/ver10/schema">
  <SOAP-ENV:Header>
    <wsa:Action SOAP-ENV:mustUnderstand="1">{}</wsa:Action>
    <wsa:To SOAP-ENV:mustUnderstand="1">{}</wsa:To>
    {}
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>{}</SOAP-ENV:Body>
</SOAP-ENV:Envelope>"#,
        xml_escape(action),
        xml_escape(to),
        security,
        body
    )
}

fn service_matches(namespace: Option<&str>, service_kind: &str) -> bool {
    let Some(namespace) = namespace else {
        return false;
    };
    let lower = namespace.to_ascii_lowercase();
    match service_kind {
        "imaging" => lower.contains("/imaging/wsdl"),
        "media" => lower.contains("/media/wsdl"),
        _ => false,
    }
}

fn local_name(name: &[u8]) -> String {
    match name.split(|byte| *byte == b':').last() {
        Some(part) => String::from_utf8_lossy(part).to_string(),
        None => String::from_utf8_lossy(name).to_string(),
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::{
        build_set_imaging_settings_body, load_onvif_night_vision_cameras_from_env,
        parse_first_video_source_token, parse_service_endpoint, NightVisionMode,
    };
    use crate::config::AgentConfig;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::MutexGuard;

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

    #[test]
    fn loads_only_cameras_with_night_vision_mode_from_runtime_env() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "cameras": [
                {
                    "id": "cam-1",
                    "user": "alice",
                    "pass": "secret",
                    "onvif": {
                        "url": "http://10.20.30.40:3030/onvif/service",
                        "night_vision": {
                            "mode": "night"
                        }
                    }
                },
                {
                    "id": "cam-2",
                    "user": "bob",
                    "pass": "secret-2",
                    "onvif": {
                        "url": "http://10.20.30.41:4040/onvif/service"
                    }
                }
            ]
        });
        AgentConfig::apply_json_env_overrides(&cfg);

        let cameras =
            load_onvif_night_vision_cameras_from_env().expect("load night vision cameras");
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0].camera_id, "cam-1");
        assert_eq!(
            cameras[0].service_url,
            "http://10.20.30.40:3030/onvif/service"
        );
        assert_eq!(cameras[0].username, "alice");
        assert_eq!(cameras[0].password, "secret");
        assert_eq!(cameras[0].mode, NightVisionMode::Night);
    }

    #[test]
    fn parses_imaging_service_and_video_source_token() {
        let services_xml = r#"
        <tds:GetServicesResponse xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
          <tds:Service>
            <tds:Namespace>http://www.onvif.org/ver10/media/wsdl</tds:Namespace>
            <tds:XAddr>http://camera/onvif/media_service</tds:XAddr>
          </tds:Service>
          <tds:Service>
            <tds:Namespace>http://www.onvif.org/ver20/imaging/wsdl</tds:Namespace>
            <tds:XAddr>http://camera/onvif/imaging_service</tds:XAddr>
          </tds:Service>
        </tds:GetServicesResponse>
        "#;
        let profiles_xml = r#"
        <trt:GetProfilesResponse xmlns:trt="http://www.onvif.org/ver10/media/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <trt:Profiles token="MainProfile">
            <tt:VideoSourceConfiguration token="vsc1">
              <tt:SourceToken>video-source-1</tt:SourceToken>
            </tt:VideoSourceConfiguration>
          </trt:Profiles>
        </trt:GetProfilesResponse>
        "#;

        assert_eq!(
            parse_service_endpoint(services_xml, "imaging").as_deref(),
            Some("http://camera/onvif/imaging_service")
        );
        assert_eq!(
            parse_service_endpoint(services_xml, "media").as_deref(),
            Some("http://camera/onvif/media_service")
        );
        assert_eq!(
            parse_first_video_source_token(profiles_xml).as_deref(),
            Some("video-source-1")
        );
    }

    #[test]
    fn builds_set_imaging_settings_body_for_requested_mode() {
        let body = build_set_imaging_settings_body("video-source-1", NightVisionMode::Day);
        assert!(body.contains("<timg:VideoSourceToken>video-source-1</timg:VideoSourceToken>"));
        assert!(body.contains("<tt:IrCutFilter>Off</tt:IrCutFilter>"));
        assert!(body.contains("<timg:ForcePersistence>true</timg:ForcePersistence>"));
    }
}
