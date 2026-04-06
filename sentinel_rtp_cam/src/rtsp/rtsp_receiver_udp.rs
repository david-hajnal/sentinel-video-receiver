use anyhow::{bail, Result};
use base64::Engine;
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::AgentConfig;
use crate::core::h264_depacketize::H264Depacketizer;
use crate::core::h264_sync::H264SyncGate;
use crate::core::rtp::RtpPacket;
use crate::core::sdp::parse_sdp_video_track;
use crate::core::video::{annexb_from_raw_nal, VideoNal};
use crate::rtsp::rtsp::RtspClient;

#[derive(Clone, Debug)]
pub struct UdpReceiverConfig {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub rtp_port: u16,
    pub rtcp_port: u16,
    /// Optional RTSP Basic auth
    pub user: String,
    pub pass: String,
}

fn unquote(mut s: String) -> String {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            s = s[1..bytes.len() - 1].to_string();
        }
    }
    s
}

impl UdpReceiverConfig {
    pub fn from_env() -> Self {
        let host = AgentConfig::runtime_var("UDP_RTSP_HOST")
            .or_else(|| AgentConfig::runtime_var("RTSP_HOST"))
            .unwrap_or_else(|| "192.168.1.187".to_string());

        let port: u16 = AgentConfig::runtime_var("UDP_RTSP_PORT")
            .or_else(|| AgentConfig::runtime_var("RTSP_PORT"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(554);

        let path = AgentConfig::runtime_var("UDP_RTSP_PATH")
            .or_else(|| AgentConfig::runtime_var("RTSP_PATH"))
            .unwrap_or_else(|| "/stream2".to_string());

        let rtp_port: u16 = AgentConfig::runtime_var("UDP_RTP_PORT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(5004);

        let rtcp_port: u16 = AgentConfig::runtime_var("UDP_RTCP_PORT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(5005);

        // Prefer UDP_RTSP_USER/PASS, fall back to RTSP_USER/PASS
        let user = AgentConfig::runtime_var("UDP_RTSP_USER")
            .or_else(|| AgentConfig::runtime_var("RTSP_USER"))
            .unwrap_or_else(|| "".to_string());
        let pass = AgentConfig::runtime_var("UDP_RTSP_PASS")
            .or_else(|| AgentConfig::runtime_var("RTSP_PASS"))
            .unwrap_or_else(|| "".to_string());

        Self {
            host: unquote(host),
            port,
            path: unquote(path),
            rtp_port,
            rtcp_port,
            user: unquote(user),
            pass: unquote(pass),
        }
    }

    pub fn rtsp_url(&self) -> String {
        let path = if self.path.starts_with('/') {
            self.path.clone()
        } else {
            format!("/{}", self.path)
        };
        format!("rtsp://{}:{}{}", self.host, self.port, path)
    }

    fn has_auth(&self) -> bool {
        !self.user.is_empty()
    }
}

fn basic_auth_value(user: &str, pass: &str) -> String {
    let token = format!("{user}:{pass}");
    let b64 = base64::engine::general_purpose::STANDARD.encode(token.as_bytes());
    format!("Basic {b64}")
}

fn header_value<'a>(headers: &'a [(String, String)], key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.as_str())
}

/// Receives RTP/H264 over UDP and emits Annex-B NALs + RTP timestamps.
pub async fn run_udp_receiver(
    cfg: UdpReceiverConfig,
    nal_tx: mpsc::Sender<VideoNal>,
    cancel: CancellationToken,
) -> Result<()> {
    let rtsp_url = cfg.rtsp_url();

    info!(
        host = %cfg.host,
        port = cfg.port,
        url = %rtsp_url,
        auth = if cfg.has_auth() { "basic" } else { "none" },
        "Starting RTSP UDP receiver"
    );

    let authz = if cfg.has_auth() {
        Some(basic_auth_value(&cfg.user, &cfg.pass))
    } else {
        None
    };

    // RTSP control connection (TCP)
    debug!(host = %cfg.host, port = cfg.port, "Connecting to RTSP server");
    let mut c = RtspClient::connect(&cfg.host, cfg.port).await?;

    // Build common headers for RTSP requests
    let mut common_headers: Vec<(&str, &str)> = Vec::new();
    if let Some(ref a) = authz {
        common_headers.push(("Authorization", a.as_str()));
    }

    // OPTIONS
    let r = c
        .request("OPTIONS", &rtsp_url, &common_headers, None)
        .await?;
    if r.status != 200 {
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            debug!(auth_header = %wa, "RTSP authentication required");
        }
        bail!("OPTIONS failed: {}", r.status);
    }

    // DESCRIBE
    let mut describe_headers = common_headers.clone();
    describe_headers.push(("Accept", "application/sdp"));
    let r = c
        .request("DESCRIBE", &rtsp_url, &describe_headers, None)
        .await?;
    if r.status != 200 {
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            debug!(auth_header = %wa, "RTSP authentication required");
        }
        bail!("DESCRIBE failed: {}", r.status);
    }

    let sdp = String::from_utf8_lossy(&r.body);
    let track = parse_sdp_video_track(&sdp)?;

    let setup_url = if track.control.starts_with("rtsp://") {
        track.control.clone()
    } else {
        format!(
            "{}/{}",
            rtsp_url.trim_end_matches('/'),
            track.control.trim_start_matches('/')
        )
    };

    // SETUP (UDP)
    let transport = format!(
        "RTP/AVP;unicast;client_port={}-{};mode=play",
        cfg.rtp_port, cfg.rtcp_port
    );
    let mut setup_headers = common_headers.clone();
    setup_headers.push(("Transport", &transport));
    let r = c.request("SETUP", &setup_url, &setup_headers, None).await?;
    if r.status != 200 {
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            debug!(auth_header = %wa, "RTSP authentication required");
        }
        bail!("SETUP failed: {}", r.status);
    }
    c.set_session_from(&r);

    // PLAY
    let mut play_headers = common_headers.clone();
    play_headers.push(("Range", "npt=0.000-"));
    let r = c.request("PLAY", &rtsp_url, &play_headers, None).await?;
    if r.status != 200 {
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            debug!(auth_header = %wa, "RTSP authentication required");
        }
        bail!("PLAY failed: {}", r.status);
    }

    // Bind UDP socket for RTP
    let sock = UdpSocket::bind(("0.0.0.0", cfg.rtp_port)).await?;
    info!(port = cfg.rtp_port, "RTP receiving on UDP socket");

    let mut dep = H264Depacketizer::new();
    let mut gate = H264SyncGate::new(true);
    if let (Some(sps), Some(pps)) = (track.sprop_sps, track.sprop_pps) {
        gate.set_sprop_param_sets(annexb_from_raw_nal(&sps), annexb_from_raw_nal(&pps));
    }
    let mut buf = vec![0u8; 8192];

    // continuity
    let mut expected_seq: Option<u16> = None;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // best-effort TEARDOWN
                let _ = c.request("TEARDOWN", &rtsp_url, &common_headers, None).await;
                break;
            }
            r = sock.recv_from(&mut buf) => {
                let (n, _src) = r?;
                let pkt = match RtpPacket::parse(&buf[..n]) {
                    Ok(p) => p,
                    Err(_) => {
                        dep.reset();
                        gate.reset();
                        expected_seq = None;
                        continue;
                    }
                };

                // Drop on gap/out-of-order for now
                if let Some(exp) = expected_seq {
                    if pkt.sequence_number != exp {
                        dep.reset();
                        gate.reset();
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        continue;
                    }
                }
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));

                let marker = pkt.marker;
                let rtp_ts = pkt.timestamp;

                let nals = match dep.push_rtp_payload(pkt.payload) {
                    Ok(v) => v,
                    Err(_) => {
                        dep.reset();
                        gate.reset();
                        expected_seq = None;
                        continue;
                    }
                };

                for nal in nals {
                    for out in gate.push_nal(nal) {
                        let _ = nal_tx.try_send(VideoNal { data: out, rtp_ts, marker });
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
                .unwrap_or_else(|poison| poison.into_inner());
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
    fn unquote_strips_single_and_double_quotes() {
        assert_eq!(unquote("\"value\"".to_string()), "value");
        assert_eq!(unquote("'value'".to_string()), "value");
        assert_eq!(unquote("value".to_string()), "value");
    }

    #[test]
    fn rtsp_url_normalizes_path_prefix() {
        let cfg_no_slash = UdpReceiverConfig {
            host: "cam.local".to_string(),
            port: 8554,
            path: "stream2".to_string(),
            rtp_port: 5004,
            rtcp_port: 5005,
            user: "".to_string(),
            pass: "".to_string(),
        };
        assert_eq!(cfg_no_slash.rtsp_url(), "rtsp://cam.local:8554/stream2");

        let cfg_with_slash = UdpReceiverConfig {
            path: "/stream2".to_string(),
            ..cfg_no_slash
        };
        assert_eq!(cfg_with_slash.rtsp_url(), "rtsp://cam.local:8554/stream2");
    }

    #[test]
    fn has_auth_depends_on_username_presence() {
        let mut cfg = UdpReceiverConfig {
            host: "cam.local".to_string(),
            port: 554,
            path: "/stream2".to_string(),
            rtp_port: 5004,
            rtcp_port: 5005,
            user: "".to_string(),
            pass: "pass".to_string(),
        };
        assert!(!cfg.has_auth());

        cfg.user = "user".to_string();
        assert!(cfg.has_auth());
    }

    #[test]
    fn header_value_matches_keys_case_insensitively() {
        let headers = vec![("TrAnSpOrT".to_string(), "value".to_string())];
        assert_eq!(header_value(&headers, "transport"), Some("value"));
        assert_eq!(header_value(&headers, "TRANSPORT"), Some("value"));
    }

    #[test]
    fn from_env_prefers_udp_specific_values_and_unquotes_strings() {
        let _guard = EnvGuard::new();
        AgentConfig::runtime_restore(HashMap::from([
            ("UDP_RTSP_HOST".to_string(), "\"udp-host.local\"".to_string()),
            ("UDP_RTSP_PORT".to_string(), "8554".to_string()),
            ("UDP_RTSP_PATH".to_string(), "'stream-main'".to_string()),
            ("UDP_RTP_PORT".to_string(), "6000".to_string()),
            ("UDP_RTCP_PORT".to_string(), "6001".to_string()),
            ("UDP_RTSP_USER".to_string(), "\"udp-user\"".to_string()),
            ("UDP_RTSP_PASS".to_string(), "'udp-pass'".to_string()),
            ("RTSP_HOST".to_string(), "fallback.local".to_string()),
            ("RTSP_USER".to_string(), "fallback-user".to_string()),
            ("RTSP_PASS".to_string(), "fallback-pass".to_string()),
        ]));

        let cfg = UdpReceiverConfig::from_env();
        assert_eq!(cfg.host, "udp-host.local");
        assert_eq!(cfg.port, 8554);
        assert_eq!(cfg.path, "stream-main");
        assert_eq!(cfg.rtp_port, 6000);
        assert_eq!(cfg.rtcp_port, 6001);
        assert_eq!(cfg.user, "udp-user");
        assert_eq!(cfg.pass, "udp-pass");
    }

    #[test]
    fn from_env_falls_back_to_general_rtsp_values() {
        let _guard = EnvGuard::new();
        AgentConfig::apply_json_env_overrides(&json!({
            "cameras": [{
                "user": "general-user",
                "pass": "general-pass",
                "rtsp": {
                    "url": "rtsp://general.local/general-stream"
                }
            }]
        }));

        let cfg = UdpReceiverConfig::from_env();
        assert_eq!(cfg.host, "general.local");
        assert_eq!(cfg.port, 554);
        assert_eq!(cfg.path, "/general-stream");
        assert_eq!(cfg.user, "general-user");
        assert_eq!(cfg.pass, "general-pass");
    }
}
