use anyhow::{bail, Result};
use base64::Engine;
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::h264_depacketize::H264Depacketizer;
use crate::rtp::RtpPacket;
use crate::rtsp::RtspClient;
use crate::sdp::parse_sdp_video_track;
use crate::video::{nal_type_from_annexb, VideoNal};

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
        let host = std::env::var("UDP_RTSP_HOST")
            .or_else(|_| std::env::var("RTSP_HOST"))
            .unwrap_or_else(|_| "192.168.1.187".to_string());

        let port: u16 = std::env::var("UDP_RTSP_PORT")
            .or_else(|_| std::env::var("RTSP_PORT"))
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(554);

        let path = std::env::var("UDP_RTSP_PATH")
            .or_else(|_| std::env::var("RTSP_PATH"))
            .unwrap_or_else(|_| "/stream2".to_string());

        let rtp_port: u16 = std::env::var("UDP_RTP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5004);

        let rtcp_port: u16 = std::env::var("UDP_RTCP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5005);

        // Prefer UDP_RTSP_USER/PASS, fall back to RTSP_USER/PASS
        let user = std::env::var("UDP_RTSP_USER")
            .or_else(|_| std::env::var("RTSP_USER"))
            .unwrap_or_else(|_| "".to_string());
        let pass = std::env::var("UDP_RTSP_PASS")
            .or_else(|_| std::env::var("RTSP_PASS"))
            .unwrap_or_else(|_| "".to_string());

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

    eprintln!(
        "🎥 RTSP(UDP): starting host={} port={} url={} auth={}",
        cfg.host,
        cfg.port,
        rtsp_url,
        if cfg.has_auth() { "basic" } else { "none" }
    );

    let authz = if cfg.has_auth() {
        Some(basic_auth_value(&cfg.user, &cfg.pass))
    } else {
        None
    };

    // RTSP control connection (TCP)
    eprintln!("🎥 RTSP TcpStream::connect to {}:{}", cfg.host, cfg.port);
    let mut c = RtspClient::connect(&cfg.host, cfg.port).await?;

    // Build common headers for RTSP requests
    let mut common_headers: Vec<(&str, &str)> = Vec::new();
    if let Some(ref a) = authz {
        common_headers.push(("Authorization", a.as_str()));
    }

    // OPTIONS
    let r = c.request("OPTIONS", &rtsp_url, &common_headers, None).await?;
    if r.status != 200 {
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            eprintln!("RTSP WWW-Authenticate: {wa}");
        }
        bail!("OPTIONS failed: {}", r.status);
    }

    // DESCRIBE
    let mut describe_headers = common_headers.clone();
    describe_headers.push(("Accept", "application/sdp"));
    let r = c.request("DESCRIBE", &rtsp_url, &describe_headers, None).await?;
    if r.status != 200 {
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            eprintln!("RTSP WWW-Authenticate: {wa}");
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
            eprintln!("RTSP WWW-Authenticate: {wa}");
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
            eprintln!("RTSP WWW-Authenticate: {wa}");
        }
        bail!("PLAY failed: {}", r.status);
    }

    // Bind UDP socket for RTP
    let sock = UdpSocket::bind(("0.0.0.0", cfg.rtp_port)).await?;
    println!("🎥 RTP: receiving on udp://0.0.0.0:{}", cfg.rtp_port);

    let mut dep = H264Depacketizer::new();
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
                        expected_seq = None;
                        continue;
                    }
                };

                // Drop on gap/out-of-order for now
                if let Some(exp) = expected_seq {
                    if pkt.sequence_number != exp {
                        dep.reset();
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        continue;
                    }
                }
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));

                let marker = pkt.marker;
                let rtp_ts = pkt.timestamp;

                let nals = match dep.push_rtp_payload(pkt.payload) {
                    Ok(v) => v,
                    Err(_) => { dep.reset(); expected_seq = None; continue; }
                };

                for nal in nals {
                    if nal_type_from_annexb(&nal).is_none() {
                        dep.reset();
                        expected_seq = None;
                        break;
                    }
                    let _ = nal_tx.try_send(VideoNal { data: nal, rtp_ts, marker });
                }
            }
        }
    }

    Ok(())
}
