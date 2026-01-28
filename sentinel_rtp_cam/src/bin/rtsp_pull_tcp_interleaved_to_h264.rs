use anyhow::{anyhow, bail, Result};
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info};

use sentinel_rtp_cam::core::h264_depacketize::H264Depacketizer;
use sentinel_rtp_cam::core::h264_sync::H264SyncGate;
use sentinel_rtp_cam::rtsp::interleaved::{read_interleaved_frame, InterleavedFrame};
use sentinel_rtp_cam::core::rtp::RtpPacket;
use sentinel_rtp_cam::rtsp::rtsp::RtspClient;
use sentinel_rtp_cam::core::sdp::parse_sdp_video_track;
use sentinel_rtp_cam::core::video::annexb_from_raw_nal;

use base64::Engine;

fn header_value<'a>(headers: &'a [(String, String)], key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.as_str())
}

fn parse_interleaved_channels(transport: &str) -> Option<(u8, u8)> {
    for part in transport.split(';') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("interleaved=") {
            let (a, b) = v.split_once('-')?;
            let rtp: u8 = a.parse().ok()?;
            let rtcp: u8 = b.parse().ok()?;
            return Some((rtp, rtcp));
        }
    }
    None
}

fn basic_auth_value(user: &str, pass: &str) -> String {
    let token = format!("{user}:{pass}");
    let b64 = base64::engine::general_purpose::STANDARD.encode(token.as_bytes());
    format!("Basic {b64}")
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load .env file if it exists
    dotenvy::dotenv().ok();

    // Read configuration from environment variables
    let host = std::env::var("TCP_RTSP_HOST").unwrap_or_else(|_| "192.168.1.187".to_string());
    let port: u16 = std::env::var("TCP_RTSP_PORT")
        .unwrap_or_else(|_| "554".to_string())
        .parse()
        .map_err(|_| anyhow!("Invalid TCP_RTSP_PORT"))?;
    let path = std::env::var("TCP_RTSP_PATH").unwrap_or_else(|_| "/stream1".to_string());
    let rtsp_url = format!("rtsp://{}:{}{}", host, port, path);

    // Read raw credentials from environment (no URL encoding issues).
    let user = std::env::var("RTSP_USER").map_err(|_| anyhow!("Missing RTSP_USER env var"))?;
    let pass = std::env::var("RTSP_PASS").map_err(|_| anyhow!("Missing RTSP_PASS env var"))?;
    let authz = basic_auth_value(&user, &pass);

    let mut c = RtspClient::connect(&host, port).await?;

    // OPTIONS
    let r = c
        .request("OPTIONS", &rtsp_url, &[("Authorization", &authz)], None)
        .await?;
    debug!(status = r.status, "OPTIONS response");
    if let Some(p) = header_value(&r.headers, "Public") {
        debug!(methods = %p, "Supported RTSP methods");
    }
    if r.status != 200 {
        bail!("OPTIONS failed: {}", r.status);
    }

    // DESCRIBE
    let r = c
        .request(
            "DESCRIBE",
            &rtsp_url,
            &[("Accept", "application/sdp"), ("Authorization", &authz)],
            None,
        )
        .await?;
    if r.status != 200 {
        error!(status = r.status, "DESCRIBE failed");
        if let Some(wa) = header_value(&r.headers, "WWW-Authenticate") {
            debug!(auth_header = %wa, "Authentication challenge");
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

    // SETUP
    let transport = "RTP/AVP/TCP;unicast;interleaved=0-1;mode=play";
    let r = c
        .request(
            "SETUP",
            &setup_url,
            &[("Transport", transport), ("Authorization", &authz)],
            None,
        )
        .await?;
    if r.status != 200 {
        bail!("SETUP failed: {}", r.status);
    }
    c.set_session_from(&r);

    let transport_resp = header_value(&r.headers, "Transport")
        .ok_or_else(|| anyhow!("SETUP response missing Transport header"))?;
    let (rtp_chan, rtcp_chan) = parse_interleaved_channels(transport_resp)
        .ok_or_else(|| anyhow!("SETUP Transport missing interleaved=..-..: {transport_resp}"))?;
    info!(
        rtp_channel = rtp_chan,
        rtcp_channel = rtcp_chan,
        "Negotiated interleaved channels"
    );

    // PLAY
    let r = c
        .request(
            "PLAY",
            &rtsp_url,
            &[("Range", "npt=0.000-"), ("Authorization", &authz)],
            None,
        )
        .await?;
    if r.status != 200 {
        error!(status = r.status, "PLAY failed");
        for (k, v) in &r.headers {
            debug!(header = %k, value = %v, "Response header");
        }
        bail!("PLAY failed: {}", r.status);
    }

    info!("Reading RTP/RTCP from RTSP TCP connection (interleaved mode)");

    let output_file =
        std::env::var("TCP_OUTPUT_FILE").unwrap_or_else(|_| "tcp_out.h264".to_string());
    let mut out = tokio::fs::File::create(&output_file).await?;
    let mut dep = H264Depacketizer::new();
    let mut gate = H264SyncGate::new(true);
    if let (Some(sps), Some(pps)) = (track.sprop_sps, track.sprop_pps) {
        gate.set_sprop_param_sets(annexb_from_raw_nal(&sps), annexb_from_raw_nal(&pps));
    }

    let mut expected_seq: Option<u16> = None;

    loop {
        match read_interleaved_frame(&mut c, rtp_chan, rtcp_chan).await? {
            InterleavedFrame::Rtp(bytes) => {
                let pkt = match RtpPacket::parse(&bytes) {
                    Ok(p) => p,
                    Err(_) => {
                        dep.reset();
                        gate.reset();
                        expected_seq = None;
                        continue;
                    }
                };

                if let Some(exp) = expected_seq {
                    if pkt.sequence_number != exp {
                        dep.reset();
                        gate.reset();
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        continue;
                    }
                }
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));

                for nal in dep.push_rtp_payload(pkt.payload)? {
                    for out_nal in gate.push_nal(nal) {
                        out.write_all(&out_nal).await?;
                    }
                }
            }
            InterleavedFrame::Rtcp(_bytes) => {}
            InterleavedFrame::Unknown(_ch, _bytes) => {}
        }
    }
}
