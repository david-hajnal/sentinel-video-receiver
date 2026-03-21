use anyhow::{anyhow, bail, Result};
use base64::Engine;
use chrono::Utc;
use quick_xml::events::Event as XmlEvent;
use quick_xml::Reader;
use rand::{rngs::OsRng, RngCore};
use reqwest::header::{HeaderMap, HeaderValue, CONNECTION, CONTENT_TYPE};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

use crate::config::AgentConfig;

const DEVICE_NAMESPACE: &str = "http://www.onvif.org/ver10/device/wsdl";
const MEDIA_NAMESPACE: &str = "http://www.onvif.org/ver10/media/wsdl";
const EVENTS_NAMESPACE: &str = "http://www.onvif.org/ver10/events/wsdl";
const PTZ_NAMESPACE: &str = "http://www.onvif.org/ver20/ptz/wsdl";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnvifProbeCameraConfig {
    pub camera_id: String,
    pub service_url: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct OnvifDeviceInfo {
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub firmware_version: Option<String>,
    pub serial_number: Option<String>,
    pub hardware_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OnvifServiceInfo {
    pub service: String,
    pub namespace: String,
    pub xaddr: Option<String>,
    pub capabilities_present: bool,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OnvifCapabilitiesSummary {
    pub media: bool,
    pub events: bool,
    pub ptz: bool,
    pub imaging: bool,
    pub device_io: bool,
    pub analytics: bool,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OnvifMediaProfile {
    pub token: Option<String>,
    pub name: Option<String>,
    pub video_source_token: Option<String>,
    pub video_encoder_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct OnvifVideoEncoderConfiguration {
    pub token: Option<String>,
    pub name: Option<String>,
    pub encoding: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality: Option<f64>,
    pub frame_rate_limit: Option<u32>,
    pub bitrate_limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct OnvifMediaSummary {
    pub profiles: Vec<OnvifMediaProfile>,
    pub encoder_configurations: Vec<OnvifVideoEncoderConfiguration>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OnvifEventsSummary {
    pub fixed_topic_set: Option<bool>,
    pub topic_namespace_locations: Vec<String>,
    pub topic_expression_dialects: Vec<String>,
    pub message_content_filter_dialects: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OnvifPtzSummary {
    pub node_count: usize,
    pub configuration_count: usize,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct OnvifServiceError {
    pub service: String,
    pub operation: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct OnvifProbeReport {
    pub device: OnvifDeviceInfo,
    pub services: Vec<OnvifServiceInfo>,
    pub capabilities: OnvifCapabilitiesSummary,
    pub media: Option<OnvifMediaSummary>,
    pub events: Option<OnvifEventsSummary>,
    pub ptz: Option<OnvifPtzSummary>,
    pub service_errors: Vec<OnvifServiceError>,
}

pub fn load_onvif_probe_cameras_from_env() -> Result<Vec<OnvifProbeCameraConfig>> {
    let mut cams = Vec::new();
    for i in 1..=4 {
        let prefix = format!("CAM{}_ONVIF_", i);
        let host = match AgentConfig::runtime_var(&format!("{prefix}HOST")) {
            Some(value) if !value.trim().is_empty() => value,
            _ => continue,
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
        cams.push(OnvifProbeCameraConfig {
            camera_id,
            service_url: format!("http://{}:{}/onvif/service", host, port),
            username,
            password,
        });
    }

    if !cams.is_empty() {
        return Ok(cams);
    }

    let host = match AgentConfig::runtime_var("ONVIF_HOST") {
        Some(value) if !value.trim().is_empty() => value,
        _ => return Ok(Vec::new()),
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
    Ok(vec![OnvifProbeCameraConfig {
        camera_id,
        service_url: format!("http://{}:{}/onvif/service", host, port),
        username,
        password,
    }])
}

pub async fn run_onvif_capability_probe(cam: &OnvifProbeCameraConfig) -> Result<OnvifProbeReport> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .pool_idle_timeout(Duration::from_secs(1))
        .pool_max_idle_per_host(0)
        .tcp_keepalive(Duration::from_secs(20))
        .build()?;

    let device_xml = call_operation(
        &client,
        &cam.service_url,
        &cam.username,
        &cam.password,
        DEVICE_NAMESPACE,
        "GetDeviceInformation",
        "<tds:GetDeviceInformation/>",
    )
    .await?;
    let services_xml = call_operation(
        &client,
        &cam.service_url,
        &cam.username,
        &cam.password,
        DEVICE_NAMESPACE,
        "GetServices",
        "<tds:GetServices><tds:IncludeCapability>true</tds:IncludeCapability></tds:GetServices>",
    )
    .await?;
    let capabilities_xml = call_operation(
        &client,
        &cam.service_url,
        &cam.username,
        &cam.password,
        DEVICE_NAMESPACE,
        "GetCapabilities",
        "<tds:GetCapabilities><tds:Category>All</tds:Category></tds:GetCapabilities>",
    )
    .await?;

    let services = parse_services(&services_xml);
    let capabilities = parse_capabilities(&capabilities_xml);
    let xaddrs = collect_service_xaddrs(&services, &cam.service_url);
    let mut report = OnvifProbeReport {
        device: parse_device_information(&device_xml),
        services,
        capabilities,
        media: None,
        events: None,
        ptz: None,
        service_errors: Vec::new(),
    };

    if report.capabilities.media || xaddrs.contains_key("media") {
        let media_url = xaddrs
            .get("media")
            .cloned()
            .unwrap_or_else(|| cam.service_url.clone());
        let profiles_xml = capture_optional(
            &mut report.service_errors,
            "media",
            "GetProfiles",
            call_operation(
                &client,
                &media_url,
                &cam.username,
                &cam.password,
                MEDIA_NAMESPACE,
                "GetProfiles",
                "<trt:GetProfiles/>",
            ),
        )
        .await;
        let encoders_xml = capture_optional(
            &mut report.service_errors,
            "media",
            "GetVideoEncoderConfigurations",
            call_operation(
                &client,
                &media_url,
                &cam.username,
                &cam.password,
                MEDIA_NAMESPACE,
                "GetVideoEncoderConfigurations",
                "<trt:GetVideoEncoderConfigurations/>",
            ),
        )
        .await;

        if profiles_xml.is_some() || encoders_xml.is_some() {
            report.media = Some(OnvifMediaSummary {
                profiles: profiles_xml
                    .as_deref()
                    .map(parse_profiles)
                    .unwrap_or_default(),
                encoder_configurations: encoders_xml
                    .as_deref()
                    .map(parse_video_encoder_configurations)
                    .unwrap_or_default(),
            });
        }
    }

    if report.capabilities.events || xaddrs.contains_key("events") {
        let events_url = xaddrs
            .get("events")
            .cloned()
            .unwrap_or_else(|| cam.service_url.clone());
        let events_xml = capture_optional(
            &mut report.service_errors,
            "events",
            "GetEventProperties",
            call_operation(
                &client,
                &events_url,
                &cam.username,
                &cam.password,
                EVENTS_NAMESPACE,
                "GetEventProperties",
                "<tev:GetEventProperties/>",
            ),
        )
        .await;
        if let Some(xml) = events_xml {
            report.events = Some(parse_event_properties(&xml));
        }
    }

    if report.capabilities.ptz || xaddrs.contains_key("ptz") {
        let ptz_url = xaddrs
            .get("ptz")
            .cloned()
            .unwrap_or_else(|| cam.service_url.clone());
        let nodes_xml = capture_optional(
            &mut report.service_errors,
            "ptz",
            "GetNodes",
            call_operation(
                &client,
                &ptz_url,
                &cam.username,
                &cam.password,
                PTZ_NAMESPACE,
                "GetNodes",
                "<tptz:GetNodes/>",
            ),
        )
        .await;
        let configs_xml = capture_optional(
            &mut report.service_errors,
            "ptz",
            "GetConfigurations",
            call_operation(
                &client,
                &ptz_url,
                &cam.username,
                &cam.password,
                PTZ_NAMESPACE,
                "GetConfigurations",
                "<tptz:GetConfigurations/>",
            ),
        )
        .await;
        if nodes_xml.is_some() || configs_xml.is_some() {
            report.ptz = Some(OnvifPtzSummary {
                node_count: nodes_xml
                    .as_deref()
                    .map(|xml| count_elements(xml, &["PTZNode"]))
                    .unwrap_or_default(),
                configuration_count: configs_xml
                    .as_deref()
                    .map(|xml| count_elements(xml, &["PTZConfiguration", "Configurations"]))
                    .unwrap_or_default(),
            });
        }
    }

    Ok(report)
}

async fn capture_optional(
    service_errors: &mut Vec<OnvifServiceError>,
    service: &str,
    operation: &str,
    future: impl std::future::Future<Output = Result<String>>,
) -> Option<String> {
    match future.await {
        Ok(xml) => Some(xml),
        Err(error) => {
            service_errors.push(OnvifServiceError {
                service: service.to_string(),
                operation: operation.to_string(),
                error: error.to_string(),
            });
            None
        }
    }
}

async fn call_operation(
    client: &reqwest::Client,
    service_url: &str,
    username: &str,
    password: &str,
    namespace: &str,
    operation: &str,
    body: &str,
) -> Result<String> {
    let action = format!("{namespace}/{operation}");
    let xml = soap_envelope(service_url, &action, &wsse_header(username, password), body);
    soap_post(client, service_url, &action, &xml).await
}

async fn soap_post(client: &reqwest::Client, url: &str, action: &str, xml: &str) -> Result<String> {
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
        .body(xml.to_string())
        .send()
        .await?;
    let status = response.status();
    let text = response
        .text()
        .await
        .unwrap_or_else(|error| format!("<<failed to read body: {error}>>"));
    if !status.is_success() {
        bail!("SOAP HTTP {}: {}", status, text);
    }
    Ok(text)
}

fn soap_envelope(to: &str, action: &str, security: &str, body: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope
  xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope"
  xmlns:wsa="http://www.w3.org/2005/08/addressing"
  xmlns:tds="{DEVICE_NAMESPACE}"
  xmlns:trt="{MEDIA_NAMESPACE}"
  xmlns:tev="{EVENTS_NAMESPACE}"
  xmlns:tptz="{PTZ_NAMESPACE}"
  xmlns:tt="http://www.onvif.org/ver10/schema">
  <SOAP-ENV:Header>
    <wsa:Action SOAP-ENV:mustUnderstand="1">{}</wsa:Action>
    <wsa:To SOAP-ENV:mustUnderstand="1">{}</wsa:To>
    {}
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    {}
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"#,
        xml_escape(action),
        xml_escape(to),
        security,
        body
    )
}

fn wsse_header(username: &str, password: &str) -> String {
    let mut nonce = [0u8; 20];
    OsRng.fill_bytes(&mut nonce);

    let created = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let digest = wsse_password_digest(&nonce, &created, password);
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

fn wsse_password_digest(nonce_raw: &[u8], created: &str, password: &str) -> String {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(nonce_raw);
    hasher.update(created.as_bytes());
    hasher.update(password.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn parse_device_information(xml: &str) -> OnvifDeviceInfo {
    OnvifDeviceInfo {
        manufacturer: extract_first_text(xml, "Manufacturer"),
        model: extract_first_text(xml, "Model"),
        firmware_version: extract_first_text(xml, "FirmwareVersion"),
        serial_number: extract_first_text(xml, "SerialNumber"),
        hardware_id: extract_first_text(xml, "HardwareId"),
    }
}

fn parse_services(xml: &str) -> Vec<OnvifServiceInfo> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut services = Vec::new();
    let mut current: Option<OnvifServiceInfo> = None;
    let mut pending_text: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) => {
                let name = local_name(event.name().as_ref());
                if name == "Service" {
                    current = Some(OnvifServiceInfo::default());
                    pending_text = None;
                } else if current.is_some() {
                    if name == "Namespace" || name == "XAddr" {
                        pending_text = Some(name);
                    } else if name == "Capabilities" {
                        if let Some(service) = current.as_mut() {
                            service.capabilities_present = true;
                        }
                    }
                }
            }
            Ok(XmlEvent::Empty(event)) => {
                if current.is_some() && local_name(event.name().as_ref()) == "Capabilities" {
                    if let Some(service) = current.as_mut() {
                        service.capabilities_present = true;
                    }
                }
            }
            Ok(XmlEvent::Text(text)) => {
                if let (Some(service), Some(field)) = (current.as_mut(), pending_text.as_deref()) {
                    if let Ok(value) = text.unescape() {
                        match field {
                            "Namespace" => {
                                service.namespace = value.to_string();
                                service.service = service_key_from_namespace(&service.namespace);
                            }
                            "XAddr" => {
                                let value = value.trim();
                                if !value.is_empty() {
                                    service.xaddr = Some(value.to_string());
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(XmlEvent::End(event)) => {
                let name = local_name(event.name().as_ref());
                if name == "Service" {
                    if let Some(service) = current.take() {
                        services.push(service);
                    }
                }
                pending_text = None;
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    services
}

fn collect_service_xaddrs(
    services: &[OnvifServiceInfo],
    fallback_device_service: &str,
) -> HashMap<String, String> {
    let mut xaddrs = HashMap::new();
    for service in services {
        let xaddr = service
            .xaddr
            .clone()
            .unwrap_or_else(|| fallback_device_service.to_string());
        xaddrs.insert(service.service.clone(), xaddr);
    }
    xaddrs
}

fn parse_capabilities(xml: &str) -> OnvifCapabilitiesSummary {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut summary = OnvifCapabilitiesSummary::default();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) | Ok(XmlEvent::Empty(event)) => {
                match local_name(event.name().as_ref()).as_str() {
                    "Media" => summary.media = true,
                    "Events" => summary.events = true,
                    "PTZ" => summary.ptz = true,
                    "Imaging" => summary.imaging = true,
                    "DeviceIO" => summary.device_io = true,
                    "Analytics" | "AnalyticsDevice" => summary.analytics = true,
                    _ => {}
                }
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    summary
}

fn parse_profiles(xml: &str) -> Vec<OnvifMediaProfile> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut profiles = Vec::new();
    let mut current: Option<OnvifMediaProfile> = None;
    let mut pending_text = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) => {
                let name = local_name(event.name().as_ref());
                match name.as_str() {
                    "Profiles" => {
                        current = Some(OnvifMediaProfile {
                            token: attr_value(&event, "token"),
                            ..Default::default()
                        });
                    }
                    "Name" if current.is_some() => pending_text = true,
                    "VideoSourceConfiguration" if current.is_some() => {
                        if let Some(profile) = current.as_mut() {
                            profile.video_source_token = attr_value(&event, "token");
                        }
                    }
                    "VideoEncoderConfiguration" if current.is_some() => {
                        if let Some(profile) = current.as_mut() {
                            profile.video_encoder_token = attr_value(&event, "token");
                        }
                    }
                    _ => {}
                }
            }
            Ok(XmlEvent::Empty(event)) => {
                let name = local_name(event.name().as_ref());
                if name == "VideoSourceConfiguration" {
                    if let Some(profile) = current.as_mut() {
                        profile.video_source_token = attr_value(&event, "token");
                    }
                } else if name == "VideoEncoderConfiguration" {
                    if let Some(profile) = current.as_mut() {
                        profile.video_encoder_token = attr_value(&event, "token");
                    }
                }
            }
            Ok(XmlEvent::Text(text)) => {
                if pending_text {
                    if let (Some(profile), Ok(value)) = (current.as_mut(), text.unescape()) {
                        profile.name = Some(value.to_string());
                    }
                }
            }
            Ok(XmlEvent::End(event)) => {
                let name = local_name(event.name().as_ref());
                if name == "Profiles" {
                    if let Some(profile) = current.take() {
                        profiles.push(profile);
                    }
                }
                pending_text = false;
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    profiles
}

fn parse_video_encoder_configurations(xml: &str) -> Vec<OnvifVideoEncoderConfiguration> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut configs = Vec::new();
    let mut current: Option<OnvifVideoEncoderConfiguration> = None;
    let mut pending_field: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) => {
                let name = local_name(event.name().as_ref());
                if name == "Configurations" || name == "VideoEncoderConfiguration" {
                    current = Some(OnvifVideoEncoderConfiguration {
                        token: attr_value(&event, "token"),
                        ..Default::default()
                    });
                } else if current.is_some() {
                    match name.as_str() {
                        "Name" | "Encoding" | "Quality" | "Width" | "Height" | "FrameRateLimit"
                        | "BitrateLimit" => pending_field = Some(name),
                        _ => {}
                    }
                }
            }
            Ok(XmlEvent::Text(text)) => {
                if let (Some(config), Some(field)) = (current.as_mut(), pending_field.as_deref()) {
                    if let Ok(value) = text.unescape() {
                        match field {
                            "Name" => config.name = Some(value.to_string()),
                            "Encoding" => config.encoding = Some(value.to_string()),
                            "Quality" => config.quality = value.parse::<f64>().ok(),
                            "Width" => config.width = value.parse::<u32>().ok(),
                            "Height" => config.height = value.parse::<u32>().ok(),
                            "FrameRateLimit" => {
                                config.frame_rate_limit = value.parse::<u32>().ok();
                            }
                            "BitrateLimit" => config.bitrate_limit = value.parse::<u32>().ok(),
                            _ => {}
                        }
                    }
                }
            }
            Ok(XmlEvent::End(event)) => {
                let name = local_name(event.name().as_ref());
                if name == "Configurations" || name == "VideoEncoderConfiguration" {
                    if let Some(config) = current.take() {
                        configs.push(config);
                    }
                }
                pending_field = None;
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    configs
}

fn parse_event_properties(xml: &str) -> OnvifEventsSummary {
    OnvifEventsSummary {
        fixed_topic_set: extract_first_text(xml, "FixedTopicSet")
            .map(|value| value.eq_ignore_ascii_case("true") || value == "1"),
        topic_namespace_locations: collect_texts(xml, &["TopicNamespaceLocation"]),
        topic_expression_dialects: collect_texts(xml, &["TopicExpressionDialect"]),
        message_content_filter_dialects: collect_texts(xml, &["MessageContentFilterDialect"]),
    }
}

fn count_elements(xml: &str, locals: &[&str]) -> usize {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut count = 0usize;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) | Ok(XmlEvent::Empty(event)) => {
                let local = local_name(event.name().as_ref());
                if locals.iter().any(|candidate| *candidate == local) {
                    count += 1;
                }
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    count
}

fn collect_texts(xml: &str, locals: &[&str]) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut current_match = false;
    let mut out = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) => {
                let local = local_name(event.name().as_ref());
                current_match = locals.iter().any(|candidate| *candidate == local);
            }
            Ok(XmlEvent::Text(text)) => {
                if current_match {
                    if let Ok(value) = text.unescape() {
                        push_unique(&mut out, value.trim().to_string());
                    }
                }
            }
            Ok(XmlEvent::End(_)) => current_match = false,
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    out
}

fn extract_first_text(xml: &str, wanted_local: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut in_wanted = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(event)) => {
                in_wanted = local_name(event.name().as_ref()) == wanted_local;
            }
            Ok(XmlEvent::Text(text)) => {
                if in_wanted {
                    return text.unescape().ok().map(|value| value.to_string());
                }
            }
            Ok(XmlEvent::End(_)) => in_wanted = false,
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    None
}

fn service_key_from_namespace(namespace: &str) -> String {
    let lower = namespace.to_ascii_lowercase();
    if lower.contains("/device/wsdl") {
        "device".to_string()
    } else if lower.contains("/media/wsdl") {
        "media".to_string()
    } else if lower.contains("/events/wsdl") {
        "events".to_string()
    } else if lower.contains("/ptz/wsdl") {
        "ptz".to_string()
    } else if lower.contains("/imaging/wsdl") {
        "imaging".to_string()
    } else if lower.contains("/deviceio/wsdl") {
        "device_io".to_string()
    } else if lower.contains("/analytics/wsdl") {
        "analytics".to_string()
    } else {
        namespace.to_string()
    }
}

fn attr_value(event: &quick_xml::events::BytesStart<'_>, wanted_local: &str) -> Option<String> {
    event.attributes().flatten().find_map(|attr| {
        if local_name(attr.key.as_ref()) == wanted_local {
            attr.unescape_value().ok().map(|value| value.to_string())
        } else {
            None
        }
    })
}

fn local_name(qname: &[u8]) -> String {
    match qname.split(|byte| *byte == b':').last() {
        Some(value) => String::from_utf8_lossy(value).to_string(),
        None => String::from_utf8_lossy(qname).to_string(),
    }
}

fn push_unique(out: &mut Vec<String>, value: String) {
    if !value.is_empty() && !out.iter().any(|existing| existing == &value) {
        out.push(value);
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
        collect_service_xaddrs, load_onvif_probe_cameras_from_env, parse_capabilities,
        parse_device_information, parse_event_properties, parse_profiles, parse_services,
        parse_video_encoder_configurations,
    };
    use crate::config::AgentConfig;
    use serde_json::json;

    #[test]
    fn parse_device_info_extracts_common_fields() {
        let xml = r#"
<Envelope>
  <Body>
    <GetDeviceInformationResponse>
      <Manufacturer>ACME</Manufacturer>
      <Model>Cam 9</Model>
      <FirmwareVersion>1.2.3</FirmwareVersion>
      <SerialNumber>abc123</SerialNumber>
      <HardwareId>rev-a</HardwareId>
    </GetDeviceInformationResponse>
  </Body>
</Envelope>
"#;

        let parsed = parse_device_information(xml);
        assert_eq!(parsed.manufacturer.as_deref(), Some("ACME"));
        assert_eq!(parsed.model.as_deref(), Some("Cam 9"));
        assert_eq!(parsed.firmware_version.as_deref(), Some("1.2.3"));
        assert_eq!(parsed.serial_number.as_deref(), Some("abc123"));
        assert_eq!(parsed.hardware_id.as_deref(), Some("rev-a"));
    }

    #[test]
    fn parse_services_tracks_namespaces_and_xaddrs() {
        let xml = r#"
<Envelope xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
  <Body>
    <tds:GetServicesResponse>
      <tds:Service>
        <tds:Namespace>http://www.onvif.org/ver10/device/wsdl</tds:Namespace>
        <tds:XAddr>http://cam/onvif/service</tds:XAddr>
        <tds:Capabilities/>
      </tds:Service>
      <tds:Service>
        <tds:Namespace>http://www.onvif.org/ver10/media/wsdl</tds:Namespace>
        <tds:XAddr>http://cam/onvif/media</tds:XAddr>
      </tds:Service>
    </tds:GetServicesResponse>
  </Body>
</Envelope>
"#;

        let services = parse_services(xml);
        assert_eq!(services.len(), 2);
        assert_eq!(services[0].service, "device");
        assert_eq!(
            services[0].xaddr.as_deref(),
            Some("http://cam/onvif/service")
        );
        assert!(services[0].capabilities_present);
        assert_eq!(services[1].service, "media");

        let xaddrs = collect_service_xaddrs(&services, "http://fallback/onvif/service");
        assert_eq!(
            xaddrs.get("media").map(String::as_str),
            Some("http://cam/onvif/media")
        );
    }

    #[test]
    fn parse_capabilities_marks_supported_services() {
        let xml = r#"
<Envelope>
  <Body>
    <GetCapabilitiesResponse>
      <Capabilities>
        <Media/>
        <Events/>
        <PTZ/>
        <AnalyticsDevice/>
      </Capabilities>
    </GetCapabilitiesResponse>
  </Body>
</Envelope>
"#;

        let caps = parse_capabilities(xml);
        assert!(caps.media);
        assert!(caps.events);
        assert!(caps.ptz);
        assert!(caps.analytics);
        assert!(!caps.device_io);
    }

    #[test]
    fn parse_media_sections_extract_profiles_and_encoder_configs() {
        let profiles_xml = r#"
<Envelope xmlns:trt="http://www.onvif.org/ver10/media/wsdl">
  <Body>
    <trt:GetProfilesResponse>
      <trt:Profiles token="profile1">
        <tt:Name xmlns:tt="http://www.onvif.org/ver10/schema">Main</tt:Name>
        <tt:VideoSourceConfiguration xmlns:tt="http://www.onvif.org/ver10/schema" token="src1"/>
        <tt:VideoEncoderConfiguration xmlns:tt="http://www.onvif.org/ver10/schema" token="enc1"/>
      </trt:Profiles>
    </trt:GetProfilesResponse>
  </Body>
</Envelope>
"#;
        let encoders_xml = r#"
<Envelope xmlns:trt="http://www.onvif.org/ver10/media/wsdl">
  <Body>
    <trt:GetVideoEncoderConfigurationsResponse>
      <trt:Configurations token="enc1">
        <tt:Name xmlns:tt="http://www.onvif.org/ver10/schema">MainEncoder</tt:Name>
        <tt:Encoding xmlns:tt="http://www.onvif.org/ver10/schema">H264</tt:Encoding>
        <tt:Resolution xmlns:tt="http://www.onvif.org/ver10/schema">
          <tt:Width>1920</tt:Width>
          <tt:Height>1080</tt:Height>
        </tt:Resolution>
        <tt:Quality xmlns:tt="http://www.onvif.org/ver10/schema">5</tt:Quality>
        <tt:RateControl xmlns:tt="http://www.onvif.org/ver10/schema">
          <tt:FrameRateLimit>15</tt:FrameRateLimit>
          <tt:BitrateLimit>2048</tt:BitrateLimit>
        </tt:RateControl>
      </trt:Configurations>
    </trt:GetVideoEncoderConfigurationsResponse>
  </Body>
</Envelope>
"#;

        let profiles = parse_profiles(profiles_xml);
        let encoders = parse_video_encoder_configurations(encoders_xml);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].token.as_deref(), Some("profile1"));
        assert_eq!(profiles[0].video_source_token.as_deref(), Some("src1"));
        assert_eq!(encoders.len(), 1);
        assert_eq!(encoders[0].encoding.as_deref(), Some("H264"));
        assert_eq!(encoders[0].width, Some(1920));
        assert_eq!(encoders[0].height, Some(1080));
        assert_eq!(encoders[0].frame_rate_limit, Some(15));
        assert_eq!(encoders[0].bitrate_limit, Some(2048));
    }

    #[test]
    fn parse_event_properties_collects_topics_and_dialects() {
        let xml = r#"
<Envelope>
  <Body>
    <GetEventPropertiesResponse>
      <FixedTopicSet>true</FixedTopicSet>
      <TopicNamespaceLocation>http://www.onvif.org/ver10/topics/topicns.xml</TopicNamespaceLocation>
      <TopicExpressionDialect>dialect-1</TopicExpressionDialect>
      <TopicExpressionDialect>dialect-1</TopicExpressionDialect>
      <MessageContentFilterDialect>xpath</MessageContentFilterDialect>
    </GetEventPropertiesResponse>
  </Body>
</Envelope>
"#;

        let parsed = parse_event_properties(xml);
        assert_eq!(parsed.fixed_topic_set, Some(true));
        assert_eq!(parsed.topic_namespace_locations.len(), 1);
        assert_eq!(parsed.topic_expression_dialects, vec!["dialect-1"]);
        assert_eq!(parsed.message_content_filter_dialects, vec!["xpath"]);
    }

    #[test]
    fn load_probe_cameras_reads_prefixed_and_global_runtime_values() {
        let guard = AgentConfig::runtime_test_lock()
            .lock()
            .expect("runtime test lock");
        AgentConfig::clear_runtime_overrides();

        AgentConfig::apply_json_env_overrides(&json!({
            "camera_id": "cam-global",
            "cameras": [
                {
                    "id": "cam-1",
                    "user": "probe-user",
                    "pass": "probe-pass",
                    "onvif": { "url": "http://10.0.0.10:8899/onvif/service" }
                }
            ]
        }));

        let cameras = load_onvif_probe_cameras_from_env().expect("load probe cameras");
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0].camera_id, "cam-1");
        assert_eq!(
            cameras[0].service_url,
            "http://10.0.0.10:8899/onvif/service"
        );
        assert_eq!(cameras[0].username, "probe-user");
        assert_eq!(cameras[0].password, "probe-pass");

        AgentConfig::clear_runtime_overrides();
        drop(guard);
    }
}
