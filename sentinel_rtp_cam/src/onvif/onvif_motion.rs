use anyhow::{anyhow, bail, Result};
use base64::Engine;
use chrono::Utc;
use quick_xml::events::Event as XmlEvent;
use quick_xml::Reader;
use rand::{rngs::OsRng, RngCore};
use reqwest::header::{HeaderMap, HeaderValue, CONNECTION, CONTENT_TYPE};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

use crate::config::AgentConfig;
use crate::event::{Event, EventBus, MotionEvent, MotionStateBus};

// --- Dialect / Actions (match camera expectations) ---
const TOPIC_DIALECT_CONCRETE_SET: &str =
    "http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet";

const ACTION_CREATE_PULLPOINT_REQ: &str =
    "http://www.onvif.org/ver10/events/wsdl/EventPortType/CreatePullPointSubscriptionRequest";
const ACTION_PULLMESSAGES_REQ: &str =
    "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription/PullMessagesRequest";
const ACTION_RENEW_REQ: &str =
    "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription/RenewRequest";

// Defaults (env can override)
const DEFAULT_SUB_TERMINATION: &str = "PT10M";
const DEFAULT_RENEW_EVERY_SECS: u64 = 300;
const DEFAULT_PULL_TIMEOUT: &str = "PT30S";
const DEFAULT_PULL_LIMIT: u32 = 10;
const DEFAULT_RESUBSCRIBE_AFTER_ERRORS: u32 = 3;
const DEFAULT_MIN_POLL_GAP_MS: u64 = 800;

// ---------- tiny env helpers ----------
fn env_bool(key: &str) -> bool {
    AgentConfig::runtime_bool(key, false)
}
fn env_u64(key: &str, default: u64) -> u64 {
    AgentConfig::runtime_u64(key, default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    AgentConfig::runtime_var(key)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_string(key: &str, default: &str) -> String {
    AgentConfig::runtime_var(key).unwrap_or_else(|| default.to_string())
}

#[derive(Clone)]
struct OnvifSettings {
    sub_termination: String,
    renew_every_secs: u64,
    pull_timeout: String,
    pull_limit: u32,
    resub_after: u32,
    min_poll_gap: Duration,
}

#[derive(Clone)]
struct OnvifCameraConfig {
    camera_id: String,
    host: String,
    port: u16,
    user: String,
    pass: String,
}

fn debug_enabled() -> bool {
    env_bool("ONVIF_DEBUG")
}
fn dump_enabled() -> bool {
    env_bool("ONVIF_DUMP_XML")
}

fn ensure_dump_dir() {
    if dump_enabled() {
        let _ = std::fs::create_dir_all("onvif_dump");
    }
}

fn dump_xml(tag: &str, xml: &str) {
    if !dump_enabled() {
        return;
    }
    ensure_dump_dir();
    let ts = Utc::now().format("%Y%m%d_%H%M%S%.3f");
    let filename = format!("onvif_dump/{}_{}.xml", ts, tag);
    let _ = std::fs::write(&filename, xml);
    if debug_enabled() {
        debug!(file = %filename, "Dumped ONVIF XML");
    }
}

fn log_debug(msg: impl AsRef<str>) {
    if debug_enabled() {
        debug!("{}", msg.as_ref());
    }
}

// ---------- WSSE ----------
fn wsse_password_digest(nonce_raw: &[u8], created: &str, password: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(nonce_raw);
    h.update(created.as_bytes());
    h.update(password.as_bytes());
    let out = h.finalize();
    base64::engine::general_purpose::STANDARD.encode(out)
}

fn wsse_header(username: &str, password: &str) -> String {
    let mut nonce = [0u8; 20];
    OsRng.fill_bytes(&mut nonce);

    let created = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let digest = wsse_password_digest(&nonce, &created, password);
    let nonce_b64 = base64::engine::general_purpose::STANDARD.encode(nonce);

    format!(
        r#"
<wsse:Security SOAP-ENV:mustUnderstand="1"
  xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
  xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
  <wsse:UsernameToken>
    <wsse:Username>{}</wsse:Username>
    <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{}</wsse:Password>
    <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{}</wsse:Nonce>
    <wsu:Created>{}</wsu:Created>
  </wsse:UsernameToken>
</wsse:Security>
"#,
        xml_escape(username),
        xml_escape(&digest),
        xml_escape(&nonce_b64),
        xml_escape(&created)
    )
}

// ---------- SOAP envelope (SOAP 1.2 + WS-A 2005/08) ----------
fn soap_envelope(
    to: &str,
    action: &str,
    security: &str,
    body: &str,
    extra_headers_xml: &str,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope
  xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope"
  xmlns:wsa="http://www.w3.org/2005/08/addressing"
  xmlns:tev="http://www.onvif.org/ver10/events/wsdl"
  xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2"
  xmlns:tns1="http://www.onvif.org/ver10/topics">
  <SOAP-ENV:Header>
    <wsa:Action SOAP-ENV:mustUnderstand="1">{}</wsa:Action>
    <wsa:To SOAP-ENV:mustUnderstand="1">{}</wsa:To>
    {}
    {}
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    {}
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"#,
        xml_escape(action),
        xml_escape(to),
        security,
        extra_headers_xml,
        body
    )
}

// ---------- Bodies ----------
fn create_pullpoint_body_motion_only(sub_termination: &str) -> String {
    format!(
        r#"<tev:CreatePullPointSubscription>
  <tev:Filter>
    <wsnt:TopicExpression Dialect="{}">tns1:RuleEngine/CellMotionDetector/Motion</wsnt:TopicExpression>
  </tev:Filter>
  <tev:InitialTerminationTime>{}</tev:InitialTerminationTime>
</tev:CreatePullPointSubscription>"#,
        TOPIC_DIALECT_CONCRETE_SET,
        xml_escape(sub_termination)
    )
}

fn pull_messages_body(timeout: &str, limit: u32) -> String {
    format!(
        r#"<tev:PullMessages>
  <tev:Timeout>{}</tev:Timeout>
  <tev:MessageLimit>{}</tev:MessageLimit>
</tev:PullMessages>"#,
        xml_escape(timeout),
        limit
    )
}

fn renew_body(termination: &str) -> String {
    format!(
        r#"<wsnt:Renew xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2">
  <wsnt:TerminationTime>{}</wsnt:TerminationTime>
</wsnt:Renew>"#,
        xml_escape(termination),
    )
}

fn subscription_id_header(sub_id: &Option<String>) -> String {
    if let Some(id) = sub_id {
        format!(
            r#"<SubscriptionId xmlns="http://www.onvif.org/ver10/events/wsdl">{}</SubscriptionId>"#,
            xml_escape(id)
        )
    } else {
        "".to_string()
    }
}

// ---------- HTTP SOAP POST ----------
async fn soap_post(
    client: &reqwest::Client,
    url: &str,
    action: &str,
    xml: &str,
    dump_tag: &str,
) -> Result<String> {
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

    // Many embedded devices behave better with explicit close on these ephemeral ports.
    headers.insert(CONNECTION, HeaderValue::from_static("close"));

    if dump_enabled() {
        dump_xml(&format!("{}_req", dump_tag), xml);
    }

    let resp = client
        .post(url)
        .headers(headers)
        .body(xml.to_string())
        .send()
        .await;
    let resp = match resp {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    let status = resp.status();
    let text = resp
        .text()
        .await
        .unwrap_or_else(|e| format!("<<failed to read body: {e}>>"));

    if !status.is_success() {
        if dump_enabled() {
            dump_xml(&format!("{}_http_error_resp", dump_tag), &text);
        }
        bail!("SOAP HTTP {}: {}", status, text);
    }

    if dump_enabled() {
        dump_xml(&format!("{}_resp", dump_tag), &text);
    }

    Ok(text)
}

// ---------- XML parsing ----------
fn extract_first_text(xml: &str, wanted_local: &str) -> Option<String> {
    let mut r = Reader::from_str(xml);
    r.trim_text(true);

    let mut buf = Vec::new();
    let mut in_wanted = false;

    loop {
        match r.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(e)) => {
                in_wanted = local_name(e.name().as_ref()) == wanted_local;
            }
            Ok(XmlEvent::Text(t)) => {
                if in_wanted {
                    return t.unescape().ok().map(|c| c.to_string());
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

fn extract_subscription_id(xml: &str) -> Option<String> {
    extract_first_text(xml, "SubscriptionId")
}

fn extract_subscription_address(xml: &str) -> Option<String> {
    // VIGI response: <tev:SubscriptionReference><wsa5:Address>...</wsa5:Address></...>
    extract_first_text(xml, "Address")
}

fn extract_motion_events(xml: &str) -> Vec<(Option<String>, bool)> {
    let mut r = Reader::from_str(xml);
    r.trim_text(true);

    let mut buf = Vec::new();
    let mut current_rule: Option<String> = None;
    let mut out = Vec::new();

    loop {
        match r.read_event_into(&mut buf) {
            Ok(XmlEvent::Empty(e)) | Ok(XmlEvent::Start(e)) => {
                if local_name(e.name().as_ref()) == "SimpleItem" {
                    let mut name_attr: Option<String> = None;
                    let mut value_attr: Option<String> = None;

                    for a in e.attributes().flatten() {
                        let k = local_name(a.key.as_ref());
                        if k == "Name" {
                            if let Ok(v) = a.unescape_value() {
                                name_attr = Some(v.to_string());
                            }
                        } else if k == "Value" {
                            if let Ok(v) = a.unescape_value() {
                                value_attr = Some(v.to_string());
                            }
                        }
                    }

                    match (name_attr.as_deref(), value_attr.as_deref()) {
                        (Some("Rule"), Some(v)) => current_rule = Some(v.to_string()),
                        (Some("IsMotion"), Some(v)) => {
                            let is_motion = v.eq_ignore_ascii_case("true") || v == "1";
                            out.push((current_rule.clone(), is_motion));
                        }
                        _ => {}
                    }
                }
            }
            Ok(XmlEvent::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    out
}

fn local_name(qname: &[u8]) -> String {
    match qname.split(|&b| b == b':').last() {
        Some(t) => String::from_utf8_lossy(t).to_string(),
        None => String::from_utf8_lossy(qname).to_string(),
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

// ---------- Subscription ops ----------
async fn create_subscription(
    client: &reqwest::Client,
    onvif_service: &str,
    user: &str,
    pass: &str,
    sub_termination: &str,
) -> Result<(String, Option<String>)> {
    let sec = wsse_header(user, pass);
    let body = create_pullpoint_body_motion_only(sub_termination);
    let req_xml = soap_envelope(onvif_service, ACTION_CREATE_PULLPOINT_REQ, &sec, &body, "");

    let resp_xml = soap_post(
        client,
        onvif_service,
        ACTION_CREATE_PULLPOINT_REQ,
        &req_xml,
        "createpullpoint",
    )
    .await?;

    let sub_addr = extract_subscription_address(&resp_xml).ok_or_else(|| {
        anyhow!("No SubscriptionReference/Address found in CreatePullPointSubscription response")
    })?;

    let sub_id = extract_subscription_id(&resp_xml);

    Ok((sub_addr, sub_id))
}

async fn renew_subscription(
    client: &reqwest::Client,
    sub_addr: &str,
    sub_id: &Option<String>,
    user: &str,
    pass: &str,
    sub_termination: &str,
) -> Result<()> {
    let sec = wsse_header(user, pass);
    let body = renew_body(sub_termination);
    let extra_hdr = subscription_id_header(sub_id);
    let req_xml = soap_envelope(sub_addr, ACTION_RENEW_REQ, &sec, &body, &extra_hdr);

    let _ = soap_post(client, sub_addr, ACTION_RENEW_REQ, &req_xml, "renew").await?;
    Ok(())
}

// ---------- Public entrypoint used by app.rs ----------
pub async fn run_onvif_motion_poller(bus: EventBus, motion_state: MotionStateBus) -> Result<()> {
    let cameras = load_onvif_cameras_from_env()?;
    let settings = OnvifSettings {
        sub_termination: env_string("ONVIF_SUB_TERMINATION", DEFAULT_SUB_TERMINATION),
        renew_every_secs: env_u64("ONVIF_RENEW_EVERY_SECS", DEFAULT_RENEW_EVERY_SECS),
        pull_timeout: env_string("ONVIF_PULL_TIMEOUT", DEFAULT_PULL_TIMEOUT),
        pull_limit: env_u32("ONVIF_PULL_LIMIT", DEFAULT_PULL_LIMIT),
        resub_after: env_u32(
            "ONVIF_RESUBSCRIBE_AFTER_ERRORS",
            DEFAULT_RESUBSCRIBE_AFTER_ERRORS,
        ),
        min_poll_gap: Duration::from_millis(env_u64("ONVIF_MIN_POLL_GAP_MS", DEFAULT_MIN_POLL_GAP_MS)),
    };

    let mut handles = Vec::new();
    for cam in cameras {
        let bus = bus.clone();
        let motion_state = motion_state.clone();
        let settings = settings.clone();
        handles.push(tokio::spawn(async move {
            run_onvif_motion_poller_for_camera(bus, motion_state, cam, settings).await
        }));
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                warn!(error = %e, "ONVIF motion poller ended");
            }
            Err(e) => {
                warn!(error = %e, "ONVIF motion poller task join failed");
            }
        }
    }

    Ok(())
}

fn load_onvif_cameras_from_env() -> Result<Vec<OnvifCameraConfig>> {
    let mut cams = Vec::new();
    for i in 1..=4 {
        let prefix = format!("CAM{}_ONVIF_", i);
        let host = match AgentConfig::runtime_var(&format!("{prefix}HOST")) {
            Some(v) if !v.trim().is_empty() => v,
            _ => continue,
        };
        let port: u16 = AgentConfig::runtime_var(&format!("{prefix}PORT"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(2020);
        let user = AgentConfig::runtime_var(&format!("{prefix}USER"))
            .ok_or_else(|| anyhow!("Missing CAM{}_ONVIF_USER", i))?;
        let pass = AgentConfig::runtime_var(&format!("{prefix}PASS"))
            .ok_or_else(|| anyhow!("Missing CAM{}_ONVIF_PASS", i))?;
        let camera_id =
            AgentConfig::runtime_var(&format!("CAM{}_CAMERA_ID", i)).unwrap_or_else(|| {
                format!("cam-{}", i)
            });
        cams.push(OnvifCameraConfig {
            camera_id,
            host,
            port,
            user,
            pass,
        });
    }

    if !cams.is_empty() {
        return Ok(cams);
    }

    let host = env_string("ONVIF_HOST", "192.168.1.187");
    let port: u16 = AgentConfig::runtime_var("ONVIF_PORT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(2020);
    let user = AgentConfig::runtime_var("ONVIF_USER").ok_or_else(|| anyhow!("Missing ONVIF_USER"))?;
    let pass = AgentConfig::runtime_var("ONVIF_PASS").ok_or_else(|| anyhow!("Missing ONVIF_PASS"))?;
    let camera_id = AgentConfig::runtime_var("CAMERA_ID")
        .or_else(|| AgentConfig::runtime_var("CAM1_CAMERA_ID"))
        .or_else(|| AgentConfig::runtime_var("ONVIF_HOST"))
        .unwrap_or_else(|| "unknown-camera".to_string());
    Ok(vec![OnvifCameraConfig {
        camera_id,
        host,
        port,
        user,
        pass,
    }])
}

async fn run_onvif_motion_poller_for_camera(
    bus: EventBus,
    motion_state: MotionStateBus,
    cam: OnvifCameraConfig,
    settings: OnvifSettings,
) -> Result<()> {
    let onvif_service = format!("http://{}:{}/onvif/service", cam.host, cam.port);

    info!(
        service = %onvif_service,
        camera_id = %cam.camera_id,
        debug = debug_enabled(),
        dump = dump_enabled(),
        min_poll_gap_ms = settings.min_poll_gap.as_millis(),
        pull_timeout = %settings.pull_timeout,
        pull_limit = settings.pull_limit,
        "ONVIF motion poller starting"
    );

    // NOTE:
    // - keep overall timeout > PullMessages timeout
    // - disable idle pooling to avoid reusing half-dead connections on embedded devices
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(40))
        .pool_idle_timeout(Duration::from_secs(1))
        .pool_max_idle_per_host(0)
        .tcp_keepalive(Duration::from_secs(20))
        .build()?;

    // Track last motion state per rule -> emit edges only
    let mut state: HashMap<String, bool> = HashMap::new();

    // Track event_id per rule (ULID generated on motion start, reused for motion end)
    let mut event_ids: HashMap<String, String> = HashMap::new();

    // Create subscription
    let (mut sub_addr, mut sub_id) = create_subscription(
        &client,
        &onvif_service,
        &cam.user,
        &cam.pass,
        &settings.sub_termination,
    )
    .await?;

    info!(
        address = %sub_addr,
        subscription_id = ?sub_id,
        camera_id = %cam.camera_id,
        "PullPoint subscription created, starting event polling"
    );

    let mut renew_tick = interval(Duration::from_secs(settings.renew_every_secs));
    let mut consecutive_errors: u32 = 0;

    let mut last_pull_done: Option<Instant> = None;
    let mut pull_attempt: u64 = 0;

    loop {
        tokio::select! {
            _ = renew_tick.tick() => {
                if let Err(e) = renew_subscription(&client, &sub_addr, &sub_id, &cam.user, &cam.pass, &settings.sub_termination).await {
                    warn!(error = %e, camera_id = %cam.camera_id, "Subscription renewal failed, recreating PullPoint");
                    (sub_addr, sub_id) = create_subscription(&client, &onvif_service, &cam.user, &cam.pass, &settings.sub_termination).await?;
                    info!(
                        address = %sub_addr,
                        subscription_id = ?sub_id,
                        camera_id = %cam.camera_id,
                        "PullPoint subscription recreated"
                    );
                    consecutive_errors = 0;
                    // reset pull pacing
                    last_pull_done = None;
                }
            }

            // PullMessages path
            _ = async {} => {
                // Min gap between polls to avoid hammering the camera when responses return fast (motion storms).
                if let Some(t) = last_pull_done {
                    let elapsed = t.elapsed();
                    if elapsed < settings.min_poll_gap {
                        sleep(settings.min_poll_gap - elapsed).await;
                    }
                }

                pull_attempt += 1;

                let sec = wsse_header(&cam.user, &cam.pass);
                let body = pull_messages_body(&settings.pull_timeout, settings.pull_limit);
                let extra_hdr = subscription_id_header(&sub_id);
                let req_xml = soap_envelope(&sub_addr, ACTION_PULLMESSAGES_REQ, &sec, &body, &extra_hdr);

                let dump_tag = format!("pull_attempt_{}", pull_attempt);
                log_debug(format!("➡️ PullMessages POST {}", sub_addr));

                match soap_post(&client, &sub_addr, ACTION_PULLMESSAGES_REQ, &req_xml, &dump_tag).await {
                    Ok(pm_xml) => {
                        consecutive_errors = 0;
                        last_pull_done = Some(Instant::now());

                        for (rule_opt, is_motion) in extract_motion_events(&pm_xml) {
                            let rule = rule_opt.unwrap_or_else(|| "<unknown-rule>".to_string());
                            let prev = state.get(&rule).copied().unwrap_or(false);
                            if prev == is_motion {
                                continue;
                            }
                            state.insert(rule.clone(), is_motion);

                            // Generate or reuse event_id
                            let event_id = if is_motion {
                                // Motion start: generate new ULID
                                let id = ulid::Ulid::new().to_string();
                                event_ids.insert(rule.clone(), id.clone());
                                id
                            } else {
                                // Motion end: reuse existing event_id or generate new one
                                event_ids.remove(&rule).unwrap_or_else(|| ulid::Ulid::new().to_string())
                            };

                            // Update motion state (for recorder) with metadata
                            let metadata = if is_motion {
                                Some(crate::event::MotionMetadata {
                                    camera_id: cam.camera_id.clone(),
                                    event_id: event_id.clone(),
                                })
                            } else {
                                None
                            };
                            motion_state.set(rule.clone(), is_motion, metadata);

                            // Publish edge event (for logging)
                            let ev = MotionEvent {
                                rule,
                                active: is_motion,
                                ts: Utc::now(),
                                camera_id: cam.camera_id.clone(),
                                event_id,
                            };
                            bus.publish(Event::Motion(ev)).await;
                        }
                    }
                    Err(e) => {
                        last_pull_done = Some(Instant::now());

                        // Classify connect/refused as "subscription endpoint is gone" -> recreate quickly
                        let mut is_connectish = false;
                        if let Some(re) = e.downcast_ref::<reqwest::Error>() {
                            if re.is_connect() {
                                is_connectish = true;
                            }
                        }
                        let msg = e.to_string();
                        if msg.contains("Connection refused") || msg.contains("os error 61") {
                            is_connectish = true;
                        }

                        consecutive_errors += 1;
                        warn!(
                            error = %e,
                            consecutive = consecutive_errors,
                            threshold = settings.resub_after,
                            "PullMessages error"
                        );

                        if is_connectish || consecutive_errors >= settings.resub_after {
                            warn!("Subscription endpoint dropped, recreating PullPoint");
                            (sub_addr, sub_id) = create_subscription(
                                &client,
                                &onvif_service,
                                &cam.user,
                                &cam.pass,
                                &settings.sub_termination,
                            )
                            .await?;
                            info!(
                                address = %sub_addr,
                                subscription_id = ?sub_id,
                                camera_id = %cam.camera_id,
                                "PullPoint subscription recreated"
                            );
                            consecutive_errors = 0;
                            // slight backoff to avoid immediate hammering after resubscribe
                            sleep(Duration::from_millis(300)).await;
                        } else {
                            sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        debug_enabled, dump_enabled, env_string, env_u32, env_u64, load_onvif_cameras_from_env,
        DEFAULT_MIN_POLL_GAP_MS, DEFAULT_PULL_LIMIT, DEFAULT_PULL_TIMEOUT,
        DEFAULT_RENEW_EVERY_SECS, DEFAULT_RESUBSCRIBE_AFTER_ERRORS, DEFAULT_SUB_TERMINATION,
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
    fn onvif_json_config_drives_motion_camera_used_by_clip_recorder() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "cameras": [
                {
                    "id": "cam-from-json",
                    "user": "alice",
                    "pass": "secret",
                    "onvif": {
                        "url": "http://10.20.30.40:3030/onvif/service"
                    }
                }
            ]
        });
        AgentConfig::apply_json_env_overrides(&cfg);

        // The resolved camera config feeds motion metadata consumed by ClipRecorder.
        let cameras = load_onvif_cameras_from_env().expect("load onvif cameras from env");
        assert_eq!(cameras.len(), 1);
        let cam = &cameras[0];
        assert_eq!(cam.camera_id, "cam-from-json");
        assert_eq!(cam.host, "10.20.30.40");
        assert_eq!(cam.port, 3030);
        assert_eq!(cam.user, "alice");
        assert_eq!(cam.pass, "secret");
    }

    #[test]
    fn onvif_json_config_applies_extended_fields_and_keeps_nulls_unset() {
        let _guard = EnvGuard::new();

        let cfg = json!({
            "cameras": [
                {
                    "id": "cam-001",
                    "user": "alice",
                    "pass": "secret",
                    "onvif": {
                        "after_sub_delay_ms": 4000,
                        "connrefused_backoff_ms": 250,
                        "connrefused_retries": 20,
                        "debug": null,
                        "dump_xml": null,
                        "min_poll_gap_ms": null,
                        "pull_limit": null,
                        "pull_timeout": null,
                        "renew_every_secs": null,
                        "resubscribe_after_errors": null,
                        "sub_termination": null,
                        "url": "http://192.168.1.187:2020/onvif/service"
                    }
                }
            ]
        });
        AgentConfig::apply_json_env_overrides(&cfg);

        assert_eq!(
            AgentConfig::runtime_var("CAM1_ONVIF_AFTER_SUB_DELAY_MS").as_deref(),
            Some("4000")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM1_ONVIF_CONNREFUSED_BACKOFF_MS").as_deref(),
            Some("250")
        );
        assert_eq!(
            AgentConfig::runtime_var("CAM1_ONVIF_CONNREFUSED_RETRIES").as_deref(),
            Some("20")
        );

        assert_eq!(
            AgentConfig::runtime_var("ONVIF_AFTER_SUB_DELAY_MS").as_deref(),
            Some("4000")
        );
        assert_eq!(
            AgentConfig::runtime_var("ONVIF_CONNREFUSED_BACKOFF_MS").as_deref(),
            Some("250")
        );
        assert_eq!(
            AgentConfig::runtime_var("ONVIF_CONNREFUSED_RETRIES").as_deref(),
            Some("20")
        );

        assert!(AgentConfig::runtime_var("CAM1_ONVIF_DEBUG").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_DUMP_XML").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_MIN_POLL_GAP_MS").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_PULL_LIMIT").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_PULL_TIMEOUT").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_RENEW_EVERY_SECS").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_RESUBSCRIBE_AFTER_ERRORS").is_none());
        assert!(AgentConfig::runtime_var("CAM1_ONVIF_SUB_TERMINATION").is_none());

        assert!(AgentConfig::runtime_var("ONVIF_DEBUG").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_DUMP_XML").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_MIN_POLL_GAP_MS").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_PULL_LIMIT").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_PULL_TIMEOUT").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_RENEW_EVERY_SECS").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_RESUBSCRIBE_AFTER_ERRORS").is_none());
        assert!(AgentConfig::runtime_var("ONVIF_SUB_TERMINATION").is_none());

        assert!(!debug_enabled());
        assert!(!dump_enabled());
        assert_eq!(
            env_u64("ONVIF_MIN_POLL_GAP_MS", DEFAULT_MIN_POLL_GAP_MS),
            DEFAULT_MIN_POLL_GAP_MS
        );
        assert_eq!(
            env_u32("ONVIF_PULL_LIMIT", DEFAULT_PULL_LIMIT),
            DEFAULT_PULL_LIMIT
        );
        assert_eq!(
            env_string("ONVIF_PULL_TIMEOUT", DEFAULT_PULL_TIMEOUT),
            DEFAULT_PULL_TIMEOUT
        );
        assert_eq!(
            env_u64("ONVIF_RENEW_EVERY_SECS", DEFAULT_RENEW_EVERY_SECS),
            DEFAULT_RENEW_EVERY_SECS
        );
        assert_eq!(
            env_u32("ONVIF_RESUBSCRIBE_AFTER_ERRORS", DEFAULT_RESUBSCRIBE_AFTER_ERRORS),
            DEFAULT_RESUBSCRIBE_AFTER_ERRORS
        );
        assert_eq!(
            env_string("ONVIF_SUB_TERMINATION", DEFAULT_SUB_TERMINATION),
            DEFAULT_SUB_TERMINATION
        );

        let cameras = load_onvif_cameras_from_env().expect("load onvif cameras from env");
        assert_eq!(cameras.len(), 1);
        let cam = &cameras[0];
        assert_eq!(cam.host, "192.168.1.187");
        assert_eq!(cam.port, 2020);
        assert_eq!(cam.user, "alice");
        assert_eq!(cam.pass, "secret");
        assert_eq!(cam.camera_id, "cam-001");
    }
}
