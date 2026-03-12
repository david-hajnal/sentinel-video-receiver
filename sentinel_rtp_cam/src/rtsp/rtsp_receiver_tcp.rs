use anyhow::{anyhow, bail, Result};
use tokio::sync::mpsc;
use tracing::debug;

use crate::config::AgentConfig;
use crate::core::h264_depacketize::H264Depacketizer;
use crate::core::h264_sync::H264SyncGate;
use crate::core::rtp::RtpPacket;
use crate::core::sdp::parse_sdp_video_track;
use crate::core::video::annexb_from_raw_nal;
use crate::rtsp::interleaved::{read_interleaved_frame, InterleavedFrame};
use crate::rtsp::rtsp::RtspClient;

use base64::Engine;

#[derive(Debug, Clone)]
pub struct TcpInterleavedReceiverConfig {
    /// Example: "rtsp://192.168.1.187:554/stream2" (NO credentials in URL)
    pub rtsp_url: String,
    pub host: String,
    pub port: u16,
    /// Username/password used for RTSP Basic auth
    pub user: String,
    pub pass: String,
    /// Range header for PLAY (optional)
    pub play_range: String,
    /// Requested interleaved channels (camera may respond with different)
    pub interleaved_request: String,
    /// If true, enforces SPS+PPS then IDR before outputting any NALs.
    pub require_idr_sync: bool,
}

impl TcpInterleavedReceiverConfig {
    pub fn from_env_defaults() -> Result<Self> {
        let rtsp_url =
            AgentConfig::runtime_var("RTSP_URL").ok_or_else(|| anyhow!("Missing RTSP_URL"))?;
        let host = AgentConfig::runtime_var("RTSP_HOST").ok_or_else(|| anyhow!("Missing RTSP_HOST"))?;
        let port: u16 = AgentConfig::runtime_var("RTSP_PORT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(554);

        let user = AgentConfig::runtime_var("RTSP_USER").ok_or_else(|| anyhow!("Missing RTSP_USER"))?;
        let pass = AgentConfig::runtime_var("RTSP_PASS").ok_or_else(|| anyhow!("Missing RTSP_PASS"))?;

        Ok(Self {
            rtsp_url,
            host,
            port,
            user,
            pass,
            play_range: "npt=0.000-".to_string(),
            interleaved_request: "RTP/AVP/TCP;unicast;interleaved=0-1;mode=play".to_string(),
            require_idr_sync: true,
        })
    }
}

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

/// Runs a TCP-interleaved RTSP session and pushes Annex-B H264 NAL units into `nal_tx`.
pub async fn run_tcp_interleaved_receiver(
    cfg: TcpInterleavedReceiverConfig,
    nal_tx: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    let authz = basic_auth_value(&cfg.user, &cfg.pass);

    let mut c = RtspClient::connect(&cfg.host, cfg.port).await?;

    // OPTIONS
    let r = c
        .request("OPTIONS", &cfg.rtsp_url, &[("Authorization", &authz)], None)
        .await?;
    if r.status != 200 {
        bail!("OPTIONS failed: {}", r.status);
    }

    // DESCRIBE
    let r = c
        .request(
            "DESCRIBE",
            &cfg.rtsp_url,
            &[("Accept", "application/sdp"), ("Authorization", &authz)],
            None,
        )
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
            cfg.rtsp_url.trim_end_matches('/'),
            track.control.trim_start_matches('/')
        )
    };

    // SETUP
    let r = c
        .request(
            "SETUP",
            &setup_url,
            &[
                ("Transport", &cfg.interleaved_request),
                ("Authorization", &authz),
            ],
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

    // PLAY
    let r = c
        .request(
            "PLAY",
            &cfg.rtsp_url,
            &[("Range", &cfg.play_range), ("Authorization", &authz)],
            None,
        )
        .await?;
    if r.status != 200 {
        bail!("PLAY failed: {}", r.status);
    }

    // Depacketizer + sync gate
    let mut dep = H264Depacketizer::new();
    let mut gate = H264SyncGate::new(cfg.require_idr_sync);
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

                // continuity check (wrap-safe)
                if let Some(exp) = expected_seq {
                    if pkt.sequence_number != exp {
                        dep.reset();
                        gate.reset();
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        continue;
                    }
                }
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));

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
                        let _ = nal_tx.try_send(out);
                    }
                }
            }

            InterleavedFrame::Rtcp(_bytes) => {
                // ignore for now
            }

            InterleavedFrame::Unknown(_ch, _bytes) => {
                // ignore unknown channels safely
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_interleaved_channels_extracts_rtp_and_rtcp_channels() {
        let transport = "RTP/AVP/TCP;unicast;interleaved=2-3;mode=play";
        assert_eq!(parse_interleaved_channels(transport), Some((2, 3)));
    }

    #[test]
    fn parse_interleaved_channels_returns_none_for_invalid_values() {
        assert_eq!(
            parse_interleaved_channels("RTP/AVP/TCP;unicast;interleaved=abc-def"),
            None
        );
        assert_eq!(
            parse_interleaved_channels("RTP/AVP/TCP;unicast;mode=play"),
            None
        );
    }

    #[test]
    fn header_value_matches_keys_case_insensitively() {
        let headers = vec![("TrAnSpOrT".to_string(), "value".to_string())];
        assert_eq!(header_value(&headers, "transport"), Some("value"));
        assert_eq!(header_value(&headers, "TRANSPORT"), Some("value"));
    }

    #[test]
    fn basic_auth_value_builds_expected_header() {
        assert_eq!(basic_auth_value("user", "pass"), "Basic dXNlcjpwYXNz");
    }
}
