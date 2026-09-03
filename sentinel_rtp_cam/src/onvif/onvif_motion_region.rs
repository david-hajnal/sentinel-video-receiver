use anyhow::{anyhow, bail, Result};
use base64::Engine;
use chrono::Utc;
use rand::{rngs::OsRng, RngCore};
use reqwest::header::{HeaderMap, HeaderValue, CONNECTION, CONTENT_TYPE};
use serde::Deserialize;
use std::io::Cursor;
use std::time::Duration;
use tracing::{info, warn};
use xmltree::{Element, XMLNode};

use crate::config::AgentConfig;

const DEVICE_GET_SERVICES_ACTION: &str = "http://www.onvif.org/ver10/device/wsdl/GetServices";
const MEDIA1_GET_PROFILES_ACTION: &str = "http://www.onvif.org/ver10/media/wsdl/GetProfiles";
const MEDIA2_GET_PROFILES_ACTION: &str = "http://www.onvif.org/ver20/media/wsdl/GetProfiles";
const ANALYTICS_GET_RULES_ACTION: &str = "http://www.onvif.org/ver20/analytics/wsdl/GetRules";
const ANALYTICS_GET_MODULES_ACTION: &str =
    "http://www.onvif.org/ver20/analytics/wsdl/GetAnalyticsModules";
const ANALYTICS_MODIFY_RULES_ACTION: &str =
    "http://www.onvif.org/ver20/analytics/wsdl/ModifyRules";
const ANALYTICS_MODIFY_MODULES_ACTION: &str =
    "http://www.onvif.org/ver20/analytics/wsdl/ModifyAnalyticsModules";

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MotionRegionPoint {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnvifMotionRegionCameraConfig {
    pub camera_id: String,
    pub service_url: String,
    pub username: String,
    pub password: String,
    pub enabled: bool,
    pub points: Vec<MotionRegionPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnvifMotionRegionSyncOutcome {
    pub camera_id: String,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum MediaFlavor {
    Media1,
    Media2,
}

#[derive(Debug, Clone)]
struct ServiceEndpoints {
    analytics_xaddr: String,
    media_xaddr: String,
    media_flavor: MediaFlavor,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum MotionRegionTargetKind {
    Rule,
    AnalyticsModule,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
struct MotionRegionTarget {
    config_token: String,
    kind: MotionRegionTargetKind,
    fragment: Element,
    target_name: Option<String>,
    target_type: Option<String>,
}

pub fn load_onvif_motion_region_cameras_from_env() -> Result<Vec<OnvifMotionRegionCameraConfig>> {
    let mut cameras = Vec::new();

    for i in 1..=4 {
        let prefix = format!("CAM{}_ONVIF_", i);
        let Some(host) = AgentConfig::runtime_var(&format!("{prefix}HOST")) else {
            continue;
        };
        if host.trim().is_empty() {
            continue;
        }
        let enabled = AgentConfig::runtime_bool(&format!("{prefix}MOTION_DETECTION_ENABLED"), false);
        let Some(points_raw) = AgentConfig::runtime_var(&format!("{prefix}MOTION_REGION_POINTS"))
        else {
            continue;
        };
        let points = parse_motion_region_points(&points_raw)?;
        let port = AgentConfig::runtime_var(&format!("{prefix}PORT"))
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(2020);
        let username = AgentConfig::runtime_var(&format!("{prefix}USER"))
            .ok_or_else(|| anyhow!("Missing CAM{}_ONVIF_USER", i))?;
        let password = AgentConfig::runtime_var(&format!("{prefix}PASS"))
            .ok_or_else(|| anyhow!("Missing CAM{}_ONVIF_PASS", i))?;
        let camera_id = AgentConfig::runtime_var(&format!("CAM{}_CAMERA_ID", i))
            .unwrap_or_else(|| format!("cam-{}", i));
        cameras.push(OnvifMotionRegionCameraConfig {
            camera_id,
            service_url: format!("http://{}:{}/onvif/service", host, port),
            username,
            password,
            enabled,
            points,
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
    let Some(points_raw) = AgentConfig::runtime_var("ONVIF_MOTION_REGION_POINTS") else {
        return Ok(Vec::new());
    };
    let points = parse_motion_region_points(&points_raw)?;
    let enabled = AgentConfig::runtime_bool("ONVIF_MOTION_DETECTION_ENABLED", false);
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

    Ok(vec![OnvifMotionRegionCameraConfig {
        camera_id,
        service_url: format!("http://{}:{}/onvif/service", host, port),
        username,
        password,
        enabled,
        points,
    }])
}

pub async fn run_onvif_motion_region_sync_once() -> Vec<OnvifMotionRegionSyncOutcome> {
    let cameras = match load_onvif_motion_region_cameras_from_env() {
        Ok(cameras) => cameras,
        Err(error) => {
            warn!(error = %error, "Failed to load ONVIF motion region cameras");
            return Vec::new();
        }
    };
    if cameras.is_empty() {
        return Vec::new();
    }

    let client = match reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .pool_idle_timeout(Duration::from_secs(1))
        .pool_max_idle_per_host(0)
        .tcp_keepalive(Duration::from_secs(20))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "Failed to build HTTP client for ONVIF sync");
            return Vec::new();
        }
    };

    let mut outcomes = Vec::with_capacity(cameras.len());
    for camera in &cameras {
        match apply_motion_region(&client, camera).await {
            Ok(()) => {
                info!(
                    camera_id = %camera.camera_id,
                    point_count = camera.points.len(),
                    enabled = camera.enabled,
                    "Applied ONVIF motion region"
                );
                outcomes.push(OnvifMotionRegionSyncOutcome {
                    camera_id: camera.camera_id.clone(),
                    status: "success".to_string(),
                    error: None,
                });
            }
            Err(error) => {
                warn!(
                    error = %error,
                    camera_id = %camera.camera_id,
                    point_count = camera.points.len(),
                    enabled = camera.enabled,
                    "Failed to apply ONVIF motion region"
                );
                let bounded_error: String = error.to_string().chars().take(500).collect();
                outcomes.push(OnvifMotionRegionSyncOutcome {
                    camera_id: camera.camera_id.clone(),
                    status: "failed".to_string(),
                    error: Some(bounded_error),
                });
            }
        }
    }

    outcomes
}

fn parse_motion_region_points(raw: &str) -> Result<Vec<MotionRegionPoint>> {
    let points: Vec<MotionRegionPoint> = serde_json::from_str(raw)?;
    Ok(points)
}

async fn apply_motion_region(
    client: &reqwest::Client,
    camera: &OnvifMotionRegionCameraConfig,
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
    let endpoints = discover_services(&services_xml, &camera.service_url);
    let config_token = fetch_video_analytics_config_token(
        client,
        &endpoints,
        &camera.username,
        &camera.password,
    )
    .await?;
    let rules_xml = get_rules(
        client,
        &endpoints.analytics_xaddr,
        &camera.username,
        &camera.password,
        &config_token,
    )
    .await?;
    let target = if let Some(target) = find_motion_region_rule_target(&rules_xml, &config_token)? {
        target
    } else {
        let modules_xml = get_analytics_modules(
            client,
            &endpoints.analytics_xaddr,
            &camera.username,
            &camera.password,
            &config_token,
        )
        .await?;
        find_motion_region_rule_target(&modules_xml, &config_token)?
            .ok_or_else(|| anyhow!("No ONVIF motion region polygon target found"))?
    };
    let body = build_modify_motion_region_body(&target, &camera.points)?;
    let action = match target.kind {
        MotionRegionTargetKind::Rule => ANALYTICS_MODIFY_RULES_ACTION,
        MotionRegionTargetKind::AnalyticsModule => ANALYTICS_MODIFY_MODULES_ACTION,
    };
    let _ = soap_call(
        client,
        &endpoints.analytics_xaddr,
        &camera.username,
        &camera.password,
        action,
        &body,
    )
    .await?;
    Ok(())
}

fn discover_services(xml: &str, service_url: &str) -> ServiceEndpoints {
    let root = Element::parse(Cursor::new(xml.as_bytes())).ok();
    let mut analytics_xaddr = None;
    let mut media_xaddr = None;
    let mut media_flavor = MediaFlavor::Media1;

    if let Some(root) = root.as_ref() {
        for service in descendants_named(root, "Service") {
            let namespace = first_child_text_by_local_name(service, "Namespace");
            let xaddr = first_child_text_by_local_name(service, "XAddr");
            let (Some(namespace), Some(xaddr)) = (namespace, xaddr) else {
                continue;
            };
            let ns = namespace.to_ascii_lowercase();
            if ns.contains("/analytics/wsdl") {
                analytics_xaddr = Some(xaddr.clone());
            }
            if ns.contains("/ver20/media/wsdl") {
                media_xaddr = Some(xaddr.clone());
                media_flavor = MediaFlavor::Media2;
            } else if media_xaddr.is_none() && ns.contains("/media/wsdl") {
                media_xaddr = Some(xaddr.clone());
                media_flavor = MediaFlavor::Media1;
            }
        }
    }

    ServiceEndpoints {
        analytics_xaddr: analytics_xaddr.unwrap_or_else(|| service_url.to_string()),
        media_xaddr: media_xaddr.unwrap_or_else(|| service_url.to_string()),
        media_flavor,
    }
}

async fn fetch_video_analytics_config_token(
    client: &reqwest::Client,
    endpoints: &ServiceEndpoints,
    username: &str,
    password: &str,
) -> Result<String> {
    let (action, body) = match endpoints.media_flavor {
        MediaFlavor::Media1 => (MEDIA1_GET_PROFILES_ACTION, "<trt:GetProfiles/>"),
        MediaFlavor::Media2 => (MEDIA2_GET_PROFILES_ACTION, "<tr2:GetProfiles/>"),
    };

    let xml = soap_call(
        client,
        &endpoints.media_xaddr,
        username,
        password,
        action,
        body,
    )
    .await?;

    parse_video_analytics_config_token(&xml)
}

async fn get_rules(
    client: &reqwest::Client,
    analytics_xaddr: &str,
    username: &str,
    password: &str,
    config_token: &str,
) -> Result<String> {
    let body = format!(
        r#"<tan:GetRules><tan:ConfigurationToken>{}</tan:ConfigurationToken></tan:GetRules>"#,
        xml_escape(config_token)
    );
    soap_call(
        client,
        analytics_xaddr,
        username,
        password,
        ANALYTICS_GET_RULES_ACTION,
        &body,
    )
    .await
}

async fn get_analytics_modules(
    client: &reqwest::Client,
    analytics_xaddr: &str,
    username: &str,
    password: &str,
    config_token: &str,
) -> Result<String> {
    let body = format!(
        r#"<tan:GetAnalyticsModules><tan:ConfigurationToken>{}</tan:ConfigurationToken></tan:GetAnalyticsModules>"#,
        xml_escape(config_token)
    );
    soap_call(
        client,
        analytics_xaddr,
        username,
        password,
        ANALYTICS_GET_MODULES_ACTION,
        &body,
    )
    .await
}

#[allow(dead_code)]
fn find_motion_region_rule_target(xml: &str, config_token: &str) -> Result<Option<MotionRegionTarget>> {
    let root = Element::parse(Cursor::new(xml.as_bytes()))?;
    for (tag, kind) in [
        ("Rule", MotionRegionTargetKind::Rule),
        ("AnalyticsModule", MotionRegionTargetKind::AnalyticsModule),
    ] {
        for child in descendants_named(&root, tag) {
            let Some(target_type) = child.attributes.get("Type").cloned() else {
                continue;
            };
            if !target_type.to_ascii_lowercase().contains("motionregion") {
                continue;
            }
            if extract_polygon_points(child).is_none() {
                continue;
            }
            return Ok(Some(MotionRegionTarget {
                config_token: config_token.to_string(),
                kind,
                fragment: child.clone(),
                target_name: child.attributes.get("Name").cloned(),
                target_type: Some(target_type),
            }));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_polygon_points(el: &Element) -> Option<Vec<MotionRegionPoint>> {
    let polygon = descendants_named(el, "Polygon").into_iter().next()?;
    let mut points = Vec::new();
    for child in &polygon.children {
        let XMLNode::Element(point) = child else {
            continue;
        };
        if local_name(&point.name) != "Point" {
            continue;
        }
        let x = point.attributes.get("x")?.parse::<f64>().ok()?;
        let y = point.attributes.get("y")?.parse::<f64>().ok()?;
        points.push(MotionRegionPoint { x, y });
    }
    if points.is_empty() {
        return None;
    }
    Some(points)
}

#[allow(dead_code)]
fn set_polygon_points(el: &mut Element, points: &[MotionRegionPoint]) -> bool {
    if local_name(&el.name) == "Polygon" {
        el.children.clear();
        for point in points {
            let mut point_el = Element::new("tt:Point");
            point_el
                .attributes
                .insert("x".to_string(), trim_float(point.x));
            point_el
                .attributes
                .insert("y".to_string(), trim_float(point.y));
            el.children.push(XMLNode::Element(point_el));
        }
        return true;
    }
    for child in &mut el.children {
        let XMLNode::Element(child_el) = child else {
            continue;
        };
        if set_polygon_points(child_el, points) {
            return true;
        }
    }
    false
}

#[allow(dead_code)]
fn build_modify_motion_region_body(target: &MotionRegionTarget, points: &[MotionRegionPoint]) -> Result<String> {
    let mut fragment = target.fragment.clone();
    if !set_polygon_points(&mut fragment, points) {
        return Err(anyhow!("Motion region polygon not found in ONVIF target"));
    }

    let mut buf = Vec::new();
    fragment.write(&mut buf)?;
    let fragment_xml = String::from_utf8(buf)?;

    match target.kind {
        MotionRegionTargetKind::Rule => Ok(format!(
            r#"<tan:ModifyRules><tan:ConfigurationToken>{}</tan:ConfigurationToken>{}</tan:ModifyRules>"#,
            xml_escape(&target.config_token),
            fragment_xml
        )),
        MotionRegionTargetKind::AnalyticsModule => Ok(format!(
            r#"<tan:ModifyAnalyticsModules><tan:ConfigurationToken>{}</tan:ConfigurationToken>{}</tan:ModifyAnalyticsModules>"#,
            xml_escape(&target.config_token),
            fragment_xml
        )),
    }
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
    if let Some(reason) = soap_fault_reason(&text) {
        bail!("SOAP fault: {}", reason);
    }
    Ok(text)
}

fn soap_fault_reason(xml: &str) -> Option<String> {
    let root = Element::parse(Cursor::new(xml.as_bytes())).ok()?;
    let fault = descendants_named(&root, "Fault").into_iter().next()?;

    if let Some(reason) = first_descendant_text_by_local_name(fault, "Text") {
        let trimmed = reason.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(reason) = first_descendant_text_by_local_name(fault, "Reason") {
        let trimmed = reason.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(code) = first_descendant_text_by_local_name(fault, "Value") {
        let trimmed = code.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    Some("unknown SOAP fault".to_string())
}

#[allow(dead_code)]
fn descendants_named<'a>(root: &'a Element, wanted_local: &str) -> Vec<&'a Element> {
    let mut out = Vec::new();
    collect_descendants_named(root, wanted_local, &mut out);
    out
}

fn first_child_text_by_local_name(el: &Element, wanted_local: &str) -> Option<String> {
    for child in &el.children {
        if let XMLNode::Element(child_el) = child {
            if local_name(&child_el.name) == wanted_local {
                let text = child_el
                    .children
                    .iter()
                    .filter_map(|n| match n {
                        XMLNode::Text(t) => Some(t.as_str()),
                        _ => None,
                    })
                    .collect::<String>();
                return Some(text.trim().to_string());
            }
        }
    }
    None
}

fn descendant_attr_by_local_name(el: &Element, wanted_local: &str, attr: &str) -> Option<String> {
    if local_name(&el.name) == wanted_local {
        if let Some(value) = el.attributes.get(attr) {
            return Some(value.clone());
        }
    }
    for child in &el.children {
        if let XMLNode::Element(child_el) = child {
            if let Some(value) = descendant_attr_by_local_name(child_el, wanted_local, attr) {
                return Some(value);
            }
        }
    }
    None
}

fn first_descendant_text_by_local_name(el: &Element, wanted_local: &str) -> Option<String> {
    if local_name(&el.name) == wanted_local {
        let text = el
            .children
            .iter()
            .filter_map(|n| match n {
                XMLNode::Text(t) => Some(t.as_str()),
                _ => None,
            })
            .collect::<String>();
        if !text.trim().is_empty() {
            return Some(text.trim().to_string());
        }
    }
    for child in &el.children {
        if let XMLNode::Element(child_el) = child {
            if let Some(value) = first_descendant_text_by_local_name(child_el, wanted_local) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_video_analytics_config_token(xml: &str) -> Result<String> {
    let root = Element::parse(Cursor::new(xml.as_bytes()))?;
    for profile in descendants_named(&root, "Profiles") {
        if let Some(token) =
            descendant_attr_by_local_name(profile, "VideoAnalyticsConfiguration", "token")
        {
            if !token.trim().is_empty() {
                return Ok(token);
            }
        }
    }
    if let Some(token) =
        descendant_attr_by_local_name(&root, "VideoAnalyticsConfiguration", "token")
    {
        if !token.trim().is_empty() {
            return Ok(token);
        }
    }

    bail!("GetProfiles returned no VideoAnalyticsConfiguration token")
}

#[allow(dead_code)]
fn collect_descendants_named<'a>(el: &'a Element, wanted_local: &str, out: &mut Vec<&'a Element>) {
    if local_name(&el.name) == wanted_local {
        out.push(el);
    }
    for child in &el.children {
        if let XMLNode::Element(child_el) = child {
            collect_descendants_named(child_el, wanted_local, out);
        }
    }
}

#[allow(dead_code)]
fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

#[allow(dead_code)]
fn trim_float(value: f64) -> String {
    let mut rendered = format!("{value:.6}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    rendered
}

#[allow(dead_code)]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
  xmlns:tr2="http://www.onvif.org/ver20/media/wsdl"
  xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl"
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

#[cfg(test)]
mod tests {
    use super::{
        apply_motion_region, build_modify_motion_region_body, discover_services,
        extract_polygon_points, find_motion_region_rule_target,
        load_onvif_motion_region_cameras_from_env, parse_video_analytics_config_token,
        run_onvif_motion_region_sync_once, soap_fault_reason, MotionRegionPoint,
        MotionRegionTargetKind, OnvifMotionRegionCameraConfig, OnvifMotionRegionSyncOutcome,
    };
    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        Router,
    };
    use crate::config::AgentConfig;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, MutexGuard};
    use tokio::net::TcpListener;

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
    fn loads_only_cameras_with_motion_region_points_from_runtime_env() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "cameras": [
                {
                    "id": "cam-1",
                    "user": "alice",
                    "pass": "secret",
                    "onvif": {
                        "url": "http://10.20.30.40:3030/onvif/service",
                        "motion_detection": {
                            "enabled": true,
                            "region": {
                                "points": [
                                    { "x": 0.1, "y": 0.1 },
                                    { "x": 0.8, "y": 0.1 },
                                    { "x": 0.8, "y": 0.7 }
                                ]
                            }
                        }
                    }
                },
                {
                    "id": "cam-2",
                    "user": "bob",
                    "pass": "secret-2",
                    "onvif": {
                        "url": "http://10.20.30.41:4040/onvif/service",
                        "motion_detection": {
                            "enabled": false
                        }
                    }
                }
            ]
        });
        AgentConfig::apply_json_env_overrides(&cfg);

        let cameras =
            load_onvif_motion_region_cameras_from_env().expect("load motion region cameras");
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0].camera_id, "cam-1");
        assert_eq!(
            cameras[0].service_url,
            "http://10.20.30.40:3030/onvif/service"
        );
        assert_eq!(cameras[0].username, "alice");
        assert_eq!(cameras[0].password, "secret");
        assert!(cameras[0].enabled);
        assert_eq!(cameras[0].points.len(), 3);
        assert_eq!(cameras[0].points[0].x, 0.1);
        assert_eq!(cameras[0].points[2].y, 0.7);
    }

    #[test]
    fn finds_motion_region_rule_target_and_reads_polygon_points() {
        let xml = r#"
        <tan:GetRulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tan:Rule Name="CellMotion" Type="tt:CellMotionEngine">
            <tt:Parameters>
              <tt:SimpleItem Name="Sensitivity" Value="80"/>
            </tt:Parameters>
          </tan:Rule>
          <tan:Rule Name="Region1" Type="tt:MotionRegionDetector">
            <tt:Parameters>
              <tt:ElementItem Name="RegionPolygon">
                <tt:Polygon>
                  <tt:Point x="0.10" y="0.10"/>
                  <tt:Point x="0.80" y="0.10"/>
                  <tt:Point x="0.80" y="0.70"/>
                </tt:Polygon>
              </tt:ElementItem>
            </tt:Parameters>
          </tan:Rule>
        </tan:GetRulesResponse>
        "#;

        let target = find_motion_region_rule_target(xml, "va-token")
            .expect("target search")
            .expect("motion region target");

        assert_eq!(target.target_name.as_deref(), Some("Region1"));
        assert_eq!(target.target_type.as_deref(), Some("tt:MotionRegionDetector"));
        let points = extract_polygon_points(&target.fragment).expect("polygon points");
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].x, 0.10);
        assert_eq!(points[2].y, 0.70);
    }

    #[test]
    fn builds_modify_rules_body_with_updated_polygon_points() {
        let xml = r#"
        <tan:GetRulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tan:Rule Name="Region1" Type="tt:MotionRegionDetector">
            <tt:Parameters>
              <tt:ElementItem Name="RegionPolygon">
                <tt:Polygon>
                  <tt:Point x="0.10" y="0.10"/>
                  <tt:Point x="0.80" y="0.10"/>
                  <tt:Point x="0.80" y="0.70"/>
                </tt:Polygon>
              </tt:ElementItem>
            </tt:Parameters>
          </tan:Rule>
        </tan:GetRulesResponse>
        "#;

        let target = find_motion_region_rule_target(xml, "va-token")
            .expect("target search")
            .expect("motion region target");
        let body = build_modify_motion_region_body(
            &target,
            &[
                MotionRegionPoint { x: 0.15, y: 0.20 },
                MotionRegionPoint { x: 0.75, y: 0.20 },
                MotionRegionPoint { x: 0.75, y: 0.65 },
                MotionRegionPoint { x: 0.15, y: 0.65 },
            ],
        )
        .expect("modify body");

        assert!(body.contains("<tan:ModifyRules>"));
        assert!(body.contains("<tan:ConfigurationToken>va-token</tan:ConfigurationToken>"));
        assert!(body.contains(r#"x="0.15""#));
        assert!(body.contains(r#"y="0.2""#));
        assert!(body.contains(r#"x="0.75""#));
        assert!(body.contains(r#"y="0.65""#));
    }

    #[test]
    fn discovers_media_and_analytics_service_endpoints() {
        let services_xml = r#"
        <tds:GetServicesResponse xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
          <tds:Service>
            <tds:Namespace>http://www.onvif.org/ver20/media/wsdl</tds:Namespace>
            <tds:XAddr>http://camera/onvif/media2</tds:XAddr>
          </tds:Service>
          <tds:Service>
            <tds:Namespace>http://www.onvif.org/ver20/analytics/wsdl</tds:Namespace>
            <tds:XAddr>http://camera/onvif/analytics</tds:XAddr>
          </tds:Service>
        </tds:GetServicesResponse>
        "#;

        let endpoints = discover_services(services_xml, "http://camera/onvif/service");
        assert_eq!(endpoints.media_xaddr, "http://camera/onvif/media2");
        assert_eq!(endpoints.analytics_xaddr, "http://camera/onvif/analytics");
    }

    #[test]
    fn parses_video_analytics_configuration_token_from_profiles() {
        let profiles_xml = r#"
        <tr2:GetProfilesResponse xmlns:tr2="http://www.onvif.org/ver20/media/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tr2:Profiles token="MainProfile">
            <tt:VideoAnalyticsConfiguration token="va-token"/>
          </tr2:Profiles>
        </tr2:GetProfilesResponse>
        "#;

        let token = parse_video_analytics_config_token(profiles_xml).expect("analytics token");
        assert_eq!(token, "va-token");
    }

    #[test]
    fn extracts_soap_12_fault_reason_text() {
        let xml = r#"
        <env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
          <env:Body>
            <env:Fault>
              <env:Code>
                <env:Value>env:Sender</env:Value>
              </env:Code>
              <env:Reason>
                <env:Text xml:lang="en">ModifyRules is not supported</env:Text>
              </env:Reason>
            </env:Fault>
          </env:Body>
        </env:Envelope>
        "#;

        let reason = soap_fault_reason(xml).expect("fault reason");
        assert_eq!(reason, "ModifyRules is not supported");
    }

    #[test]
    fn ignores_non_fault_soap_envelope() {
        let xml = r#"
        <env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
          <env:Body>
            <tan:ModifyRulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl"/>
          </env:Body>
        </env:Envelope>
        "#;

        assert_eq!(soap_fault_reason(xml), None);
    }

    #[test]
    fn finds_motion_region_analytics_module_target_and_reads_polygon_points() {
        let xml = r#"
        <tan:GetAnalyticsModulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tan:AnalyticsModule Name="RegionModule" Type="tt:MotionRegionDetector">
            <tt:Parameters>
              <tt:ElementItem Name="RegionPolygon">
                <tt:Polygon>
                  <tt:Point x="0.15" y="0.15"/>
                  <tt:Point x="0.75" y="0.15"/>
                  <tt:Point x="0.75" y="0.65"/>
                </tt:Polygon>
              </tt:ElementItem>
            </tt:Parameters>
          </tan:AnalyticsModule>
        </tan:GetAnalyticsModulesResponse>
        "#;

        let target = find_motion_region_rule_target(xml, "va-token")
            .expect("target search")
            .expect("motion region target");

        assert_eq!(target.kind, MotionRegionTargetKind::AnalyticsModule);
        assert_eq!(target.target_name.as_deref(), Some("RegionModule"));
        assert_eq!(target.target_type.as_deref(), Some("tt:MotionRegionDetector"));
        let points = extract_polygon_points(&target.fragment).expect("polygon points");
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].x, 0.15);
        assert_eq!(points[2].y, 0.65);
    }

    #[test]
    fn builds_modify_analytics_modules_body_with_updated_polygon_points() {
        let xml = r#"
        <tan:GetAnalyticsModulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tan:AnalyticsModule Name="RegionModule" Type="tt:MotionRegionDetector">
            <tt:Parameters>
              <tt:ElementItem Name="RegionPolygon">
                <tt:Polygon>
                  <tt:Point x="0.10" y="0.10"/>
                  <tt:Point x="0.80" y="0.10"/>
                  <tt:Point x="0.80" y="0.70"/>
                </tt:Polygon>
              </tt:ElementItem>
            </tt:Parameters>
          </tan:AnalyticsModule>
        </tan:GetAnalyticsModulesResponse>
        "#;

        let target = find_motion_region_rule_target(xml, "va-token")
            .expect("target search")
            .expect("motion region target");
        let body = build_modify_motion_region_body(
            &target,
            &[
                MotionRegionPoint { x: 0.2, y: 0.2 },
                MotionRegionPoint { x: 0.7, y: 0.2 },
                MotionRegionPoint { x: 0.7, y: 0.6 },
            ],
        )
        .expect("modify body");

        assert!(body.contains("<tan:ModifyAnalyticsModules>"));
        assert!(body.contains("<tan:ConfigurationToken>va-token</tan:ConfigurationToken>"));
        assert!(body.contains(r#"x="0.2""#));
        assert!(body.contains(r#"y="0.6""#));
    }

    #[tokio::test]
    async fn apply_motion_region_prefers_rules_when_available() {
        let state = Arc::new(MockOnvifState::rules());
        let Some(base_url) = spawn_mock_onvif_server(state.clone()).await else {
            eprintln!("skipping local ONVIF integration test: bind not permitted");
            return;
        };
        let client = reqwest::Client::builder().no_proxy().build().expect("client");
        let camera = OnvifMotionRegionCameraConfig {
            camera_id: "cam-1".to_string(),
            service_url: format!("{}/onvif/service", base_url),
            username: "alice".to_string(),
            password: "secret".to_string(),
            enabled: true,
            points: vec![
                MotionRegionPoint { x: 0.2, y: 0.2 },
                MotionRegionPoint { x: 0.7, y: 0.2 },
                MotionRegionPoint { x: 0.7, y: 0.6 },
            ],
        };

        apply_motion_region(&client, &camera)
            .await
            .expect("apply motion region");

        let requests = state.requests.lock().expect("requests");
        assert!(requests.iter().any(|r| r.contains("GetRules")));
        assert!(requests.iter().any(|r| r.contains("ModifyRules")));
        assert!(!requests.iter().any(|r| r.contains("GetAnalyticsModules")));
        assert!(!requests.iter().any(|r| r.contains("ModifyAnalyticsModules")));
    }

    #[tokio::test]
    async fn apply_motion_region_falls_back_to_analytics_modules() {
        let state = Arc::new(MockOnvifState::analytics_modules_only());
        let Some(base_url) = spawn_mock_onvif_server(state.clone()).await else {
            eprintln!("skipping local ONVIF integration test: bind not permitted");
            return;
        };
        let client = reqwest::Client::builder().no_proxy().build().expect("client");
        let camera = OnvifMotionRegionCameraConfig {
            camera_id: "cam-1".to_string(),
            service_url: format!("{}/onvif/service", base_url),
            username: "alice".to_string(),
            password: "secret".to_string(),
            enabled: true,
            points: vec![
                MotionRegionPoint { x: 0.2, y: 0.2 },
                MotionRegionPoint { x: 0.7, y: 0.2 },
                MotionRegionPoint { x: 0.7, y: 0.6 },
            ],
        };

        apply_motion_region(&client, &camera)
            .await
            .expect("apply motion region");

        let requests = state.requests.lock().expect("requests");
        assert!(requests.iter().any(|r| r.contains("GetRules")));
        assert!(requests.iter().any(|r| r.contains("GetAnalyticsModules")));
        assert!(requests.iter().any(|r| r.contains("ModifyAnalyticsModules")));
    }

    #[derive(Clone)]
    struct MockOnvifState {
        mode: MockOnvifMode,
        requests: Arc<Mutex<Vec<String>>>,
        base_url: Arc<Mutex<String>>,
    }

    #[derive(Clone, Copy)]
    enum MockOnvifMode {
        Rules,
        AnalyticsModulesOnly,
    }

    impl MockOnvifState {
        fn rules() -> Self {
            Self {
                mode: MockOnvifMode::Rules,
                requests: Arc::new(Mutex::new(Vec::new())),
                base_url: Arc::new(Mutex::new(String::new())),
            }
        }

        fn analytics_modules_only() -> Self {
            Self {
                mode: MockOnvifMode::AnalyticsModulesOnly,
                requests: Arc::new(Mutex::new(Vec::new())),
                base_url: Arc::new(Mutex::new(String::new())),
            }
        }
    }

    async fn spawn_mock_onvif_server(state: Arc<MockOnvifState>) -> Option<String> {
        let app = Router::new()
            .route("/onvif/service", post(mock_onvif_handler))
            .route("/onvif/media2", post(mock_onvif_handler))
            .route("/onvif/analytics", post(mock_onvif_handler))
            .with_state(state.clone());
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(error) => panic!("bind: {}", error),
        };
        let addr = listener.local_addr().expect("addr");
        *state.base_url.lock().expect("base url lock") = format!("http://{}", addr);
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        Some(format!("http://{}", addr))
    }

    async fn mock_onvif_handler(
        State(state): State<Arc<MockOnvifState>>,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, String) {
        let action = headers
            .get("SOAPAction")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        state
            .requests
            .lock()
            .expect("requests lock")
            .push(action.clone());
        let base_url = state.base_url.lock().expect("base url lock").clone();

        let response = match action.as_str() {
            "http://www.onvif.org/ver10/device/wsdl/GetServices" => {
                mock_get_services_response(&base_url)
            }
            "http://www.onvif.org/ver20/media/wsdl/GetProfiles" => mock_get_profiles_response(),
            "http://www.onvif.org/ver20/analytics/wsdl/GetRules" => match state.mode {
                MockOnvifMode::Rules => mock_get_rules_response(),
                MockOnvifMode::AnalyticsModulesOnly => mock_empty_rules_response(),
            },
            "http://www.onvif.org/ver20/analytics/wsdl/GetAnalyticsModules" => {
                mock_get_analytics_modules_response()
            }
            "http://www.onvif.org/ver20/analytics/wsdl/ModifyRules" => {
                assert!(body.contains(r#"x="0.2""#));
                assert!(body.contains("<tan:ModifyRules>"));
                mock_modify_rules_response()
            }
            "http://www.onvif.org/ver20/analytics/wsdl/ModifyAnalyticsModules" => {
                assert!(body.contains(r#"x="0.2""#));
                assert!(body.contains("<tan:ModifyAnalyticsModules>"));
                mock_modify_analytics_modules_response()
            }
            other => panic!("unexpected SOAP action: {}", other),
        };

        (StatusCode::OK, response)
    }

    fn mock_get_services_response(base_url: &str) -> String {
        format!(
            r#"
        <tds:GetServicesResponse xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
          <tds:Service>
            <tds:Namespace>http://www.onvif.org/ver20/media/wsdl</tds:Namespace>
            <tds:XAddr>{}/onvif/media2</tds:XAddr>
          </tds:Service>
          <tds:Service>
            <tds:Namespace>http://www.onvif.org/ver20/analytics/wsdl</tds:Namespace>
            <tds:XAddr>{}/onvif/analytics</tds:XAddr>
          </tds:Service>
        </tds:GetServicesResponse>
        "#,
            base_url, base_url
        )
    }

    fn mock_get_profiles_response() -> String {
        r#"
        <tr2:GetProfilesResponse xmlns:tr2="http://www.onvif.org/ver20/media/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tr2:Profiles token="MainProfile">
            <tt:VideoAnalyticsConfiguration token="va-token"/>
          </tr2:Profiles>
        </tr2:GetProfilesResponse>
        "#
        .to_string()
    }

    fn mock_get_rules_response() -> String {
        r#"
        <tan:GetRulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tan:Rule Name="Region1" Type="tt:MotionRegionDetector">
            <tt:Parameters>
              <tt:ElementItem Name="RegionPolygon">
                <tt:Polygon>
                  <tt:Point x="0.10" y="0.10"/>
                  <tt:Point x="0.80" y="0.10"/>
                  <tt:Point x="0.80" y="0.70"/>
                </tt:Polygon>
              </tt:ElementItem>
            </tt:Parameters>
          </tan:Rule>
        </tan:GetRulesResponse>
        "#
        .to_string()
    }

    fn mock_empty_rules_response() -> String {
        r#"
        <tan:GetRulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
        </tan:GetRulesResponse>
        "#
        .to_string()
    }

    fn mock_get_analytics_modules_response() -> String {
        r#"
        <tan:GetAnalyticsModulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
          <tan:AnalyticsModule Name="RegionModule" Type="tt:MotionRegionDetector">
            <tt:Parameters>
              <tt:ElementItem Name="RegionPolygon">
                <tt:Polygon>
                  <tt:Point x="0.10" y="0.10"/>
                  <tt:Point x="0.80" y="0.10"/>
                  <tt:Point x="0.80" y="0.70"/>
                </tt:Polygon>
              </tt:ElementItem>
            </tt:Parameters>
          </tan:AnalyticsModule>
        </tan:GetAnalyticsModulesResponse>
        "#
        .to_string()
    }

    fn mock_modify_rules_response() -> String {
        r#"<tan:ModifyRulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl"/>"#
            .to_string()
    }

    fn mock_modify_analytics_modules_response() -> String {
        r#"<tan:ModifyAnalyticsModulesResponse xmlns:tan="http://www.onvif.org/ver20/analytics/wsdl"/>"#
            .to_string()
    }



    #[tokio::test]
    async fn sync_once_returns_success_outcome_for_each_applied_camera() {
        let state = Arc::new(MockOnvifState::rules());
        let Some(base_url) = spawn_mock_onvif_server(state.clone()).await else {
            eprintln!("skipping local ONVIF integration test: bind not permitted");
            return;
        };

        let points_json = r#"[{"x":0.2,"y":0.2},{"x":0.7,"y":0.2},{"x":0.7,"y":0.6}]"#;

        let cfg = serde_json::json!({
            "cameras": [
                {
                    "id": "cam-sync-1",
                    "user": "alice",
                    "pass": "secret",
                    "onvif": {
                        "url": format!("{}/onvif/service", base_url),
                        "motion_detection": {
                            "enabled": true,
                            "region": { "points": serde_json::from_str::<Vec<serde_json::Value>>(points_json).unwrap() }
                        }
                    }
                },
                {
                    "id": "cam-sync-2",
                    "user": "bob",
                    "pass": "hunter2",
                    "onvif": {
                        "url": format!("{}/onvif/service", base_url),
                        "motion_detection": {
                            "enabled": true,
                            "region": { "points": serde_json::from_str::<Vec<serde_json::Value>>(points_json).unwrap() }
                        }
                    }
                }
            ]
        });
        crate::config::AgentConfig::apply_json_env_overrides(&cfg);

        let outcomes = run_onvif_motion_region_sync_once().await;
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].camera_id, "cam-sync-1");
        assert_eq!(outcomes[0].status, "success");
        assert!(outcomes[0].error.is_none());
        assert_eq!(outcomes[1].camera_id, "cam-sync-2");
        assert_eq!(outcomes[1].status, "success");
        assert!(outcomes[1].error.is_none());
    }

    #[tokio::test]
    async fn sync_once_returns_failure_and_continues_with_later_camera() {
        let state = Arc::new(MockOnvifState::rules());
        let Some(base_url) = spawn_mock_onvif_server(state.clone()).await else {
            eprintln!("skipping local ONVIF integration test: bind not permitted");
            return;
        };

        let points_json = r#"[{"x":0.2,"y":0.2},{"x":0.7,"y":0.2},{"x":0.7,"y":0.6}]"#;

        let cfg = serde_json::json!({
            "cameras": [
                {
                    "id": "cam-bad",
                    "user": "alice",
                    "pass": "secret",
                    "onvif": {
                        "url": "http://127.0.0.1:1/onvif/service",
                        "motion_detection": {
                            "enabled": true,
                            "region": { "points": serde_json::from_str::<Vec<serde_json::Value>>(points_json).unwrap() }
                        }
                    }
                },
                {
                    "id": "cam-good",
                    "user": "bob",
                    "pass": "hunter2",
                    "onvif": {
                        "url": format!("{}/onvif/service", base_url),
                        "motion_detection": {
                            "enabled": true,
                            "region": { "points": serde_json::from_str::<Vec<serde_json::Value>>(points_json).unwrap() }
                        }
                    }
                }
            ]
        });
        crate::config::AgentConfig::apply_json_env_overrides(&cfg);

        let outcomes = run_onvif_motion_region_sync_once().await;
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].camera_id, "cam-bad");
        assert_eq!(outcomes[0].status, "failed");
        assert!(outcomes[0].error.is_some());
        assert_eq!(outcomes[1].camera_id, "cam-good");
        assert_eq!(outcomes[1].status, "success");
        assert!(outcomes[1].error.is_none());
    }

    #[test]
    fn sync_outcome_does_not_include_credentials_or_polygon_payload() {
        let outcome = OnvifMotionRegionSyncOutcome {
            camera_id: "cam-1".to_string(),
            status: "success".to_string(),
            error: None,
        };

        let serialized = format!("{:?}", outcome);
        assert!(!serialized.contains("alice"));
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("password"));
        assert!(!serialized.contains("username"));
        assert!(!serialized.contains("0.1"));
        assert!(!serialized.contains("polygon"));
        assert!(!serialized.contains("points"));
    }
}
