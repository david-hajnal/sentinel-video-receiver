use anyhow::{anyhow, bail, Result};
use base64::Engine;
use chrono::Utc;
use quick_xml::events::Event;
use quick_xml::Reader;
use rand::{rngs::OsRng, RngCore};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use std::time::Duration;
use tokio::time::{interval, sleep};

const TOPIC_DIALECT_CONCRETE_SET: &str =
    "http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet";

// ONVIF WS-Addressing actions (commonly accepted by cameras)
const ACTION_CREATE_PULLPOINT_REQ: &str =
    "http://www.onvif.org/ver10/events/wsdl/EventPortType/CreatePullPointSubscriptionRequest";
const ACTION_PULLMESSAGES_REQ: &str =
    "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription/PullMessagesRequest";
const ACTION_RENEW_REQ: &str =
    "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription/RenewRequest";

// Tune these as needed
const SUB_TERMINATION: &str = "PT10M"; // ask for 10 minutes
const RENEW_EVERY_SECS: u64 = 300; // renew every 5 minutes
const PULL_TIMEOUT: &str = "PT30S"; // long poll 30s
const PULL_LIMIT: u32 = 10;
const RESUBSCRIBE_AFTER_ERRORS: u32 = 3; // recreate after N consecutive errors

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

fn create_pullpoint_body_motion_only() -> String {
    format!(
        r#"<tev:CreatePullPointSubscription>
  <tev:Filter>
    <wsnt:TopicExpression Dialect="{}">tns1:RuleEngine/CellMotionDetector/Motion</wsnt:TopicExpression>
  </tev:Filter>
  <tev:InitialTerminationTime>{}</tev:InitialTerminationTime>
</tev:CreatePullPointSubscription>"#,
        TOPIC_DIALECT_CONCRETE_SET, SUB_TERMINATION
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

/// Extract first text content of an element by LOCAL NAME (ignores namespace prefixes).
fn extract_first_text(xml: &str, wanted_local: &str) -> Option<String> {
    let mut r = Reader::from_str(xml);
    r.trim_text(true);
    let mut buf = Vec::new();
    let mut in_wanted = false;

    loop {
        match r.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                in_wanted = name == wanted_local;
            }
            Ok(Event::Text(t)) => {
                if in_wanted {
                    return t.unescape().ok().map(|c| c.to_string());
                }
            }
            Ok(Event::End(_)) => {
                in_wanted = false;
            }
            Ok(Event::Eof) => break,
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
    extract_first_text(xml, "Address")
}

fn extract_motion_events(xml: &str) -> Vec<(Option<String>, bool)> {
    let mut r = Reader::from_str(xml);
    r.trim_text(true);
    let mut buf = Vec::new();

    let mut current_rule: Option<String> = None;
    let mut events_out = Vec::new();

    loop {
        match r.read_event_into(&mut buf) {
            Ok(Event::Empty(e)) | Ok(Event::Start(e)) => {
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
                            events_out.push((current_rule.clone(), is_motion));
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    events_out
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

    let resp = client
        .post(url)
        .headers(headers)
        .body(xml.to_string())
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        bail!("SOAP HTTP {}: {}", status, text);
    }
    Ok(text)
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

async fn create_subscription(
    client: &reqwest::Client,
    onvif_service: &str,
    user: &str,
    pass: &str,
) -> Result<(String, Option<String>)> {
    let sec = wsse_header(user, pass);
    let body = create_pullpoint_body_motion_only();
    let req_xml = soap_envelope(onvif_service, ACTION_CREATE_PULLPOINT_REQ, &sec, &body, "");

    let resp_xml = soap_post(client, onvif_service, ACTION_CREATE_PULLPOINT_REQ, &req_xml).await?;
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
) -> Result<()> {
    let sec = wsse_header(user, pass);
    let body = renew_body(SUB_TERMINATION);
    let extra_hdr = subscription_id_header(sub_id);
    let req_xml = soap_envelope(sub_addr, ACTION_RENEW_REQ, &sec, &body, &extra_hdr);
    let _resp = soap_post(client, sub_addr, ACTION_RENEW_REQ, &req_xml).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    if dotenvy::dotenv().is_err() {
        dotenvy::from_filename("../.env").ok();
    }

    // Use env vars so special chars are safe.
    let host = std::env::var("ONVIF_HOST").unwrap_or_else(|_| "192.168.1.187".to_string());
    let port: u16 = std::env::var("ONVIF_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2020);
    let user = std::env::var("ONVIF_USER").map_err(|_| anyhow!("Missing ONVIF_USER"))?;
    let pass = std::env::var("ONVIF_PASS").map_err(|_| anyhow!("Missing ONVIF_PASS"))?;

    // Your GetServices output shows XAddr = http://<ip>:<port>/onvif/service for events too.
    let onvif_service = format!("http://{}:{}/onvif/service", host, port);

    // Make HTTP client a bit less "sticky" for embedded devices.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(5))
        .tcp_keepalive(Duration::from_secs(20))
        .build()?;

    // Create initial subscription
    let (mut sub_addr, mut sub_id) =
        create_subscription(&client, &onvif_service, &user, &pass).await?;
    println!("✅ PullPoint created");
    println!("   Subscription Address: {}", sub_addr);
    if let Some(id) = &sub_id {
        println!("   SubscriptionId: {}", id);
    }

    let mut renew_tick = interval(Duration::from_secs(RENEW_EVERY_SECS));
    let mut consecutive_errors: u32 = 0;

    loop {
        tokio::select! {
            _ = renew_tick.tick() => {
                if let Err(e) = renew_subscription(&client, &sub_addr, &sub_id, &user, &pass).await {
                    eprintln!("⚠ Renew error: {:#}", e);
                    eprintln!("⚠ Recreating PullPoint subscription...");
                    (sub_addr, sub_id) = create_subscription(&client, &onvif_service, &user, &pass).await?;
                    println!("✅ PullPoint re-created");
                    println!("   Subscription Address: {}", sub_addr);
                    if let Some(id) = &sub_id {
                        println!("   SubscriptionId: {}", id);
                    }
                    consecutive_errors = 0;
                }
            }

            // PullMessages loop body
            _ = async {} => {
                let sec = wsse_header(&user, &pass);
                let body = pull_messages_body(PULL_TIMEOUT, PULL_LIMIT);
                let extra_hdr = subscription_id_header(&sub_id);

                let req_xml = soap_envelope(&sub_addr, ACTION_PULLMESSAGES_REQ, &sec, &body, &extra_hdr);

                match soap_post(&client, &sub_addr, ACTION_PULLMESSAGES_REQ, &req_xml).await {
                    Ok(pm_xml) => {
                        consecutive_errors = 0;
                        for (rule, is_motion) in extract_motion_events(&pm_xml) {
                            let rule = rule.unwrap_or_else(|| "<unknown-rule>".to_string());
                            println!("📣 Motion event: rule={} is_motion={}", rule, is_motion);
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        eprintln!("⚠ PullMessages error: {:#}", e);

                        if consecutive_errors >= RESUBSCRIBE_AFTER_ERRORS {
                            eprintln!("⚠ Too many errors; recreating PullPoint subscription...");
                            (sub_addr, sub_id) = create_subscription(&client, &onvif_service, &user, &pass).await?;
                            println!("✅ PullPoint re-created");
                            println!("   Subscription Address: {}", sub_addr);
                            if let Some(id) = &sub_id {
                                println!("   SubscriptionId: {}", id);
                            }
                            consecutive_errors = 0;
                        } else {
                            sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }
}
