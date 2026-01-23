use anyhow::{anyhow, bail, Result};
use tokio::sync::mpsc;
use tracing::debug;

use crate::core::h264_depacketize::H264Depacketizer;
use crate::core::rtp::RtpPacket;
use crate::core::sdp::parse_sdp_video_track;
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
            std::env::var("RTSP_URL").map_err(|_| anyhow!("Missing RTSP_URL env var"))?;
        let host = std::env::var("RTSP_HOST").map_err(|_| anyhow!("Missing RTSP_HOST env var"))?;
        let port: u16 = std::env::var("RTSP_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(554);

        let user = std::env::var("RTSP_USER").map_err(|_| anyhow!("Missing RTSP_USER env var"))?;
        let pass = std::env::var("RTSP_PASS").map_err(|_| anyhow!("Missing RTSP_PASS env var"))?;

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

fn nal_type_from_annexb(nal: &[u8]) -> Option<u8> {
    if nal.len() < 5 || nal[0..4] != [0, 0, 0, 1] {
        return None;
    }
    Some(nal[4] & 0x1F)
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
    let mut last_sps: Option<Vec<u8>> = None;
    let mut last_pps: Option<Vec<u8>> = None;
    let mut synced = !cfg.require_idr_sync;

    let mut expected_seq: Option<u16> = None;

    loop {
        match read_interleaved_frame(&mut c, rtp_chan, rtcp_chan).await? {
            InterleavedFrame::Rtp(bytes) => {
                let pkt = match RtpPacket::parse(&bytes) {
                    Ok(p) => p,
                    Err(_) => {
                        dep.reset();
                        synced = !cfg.require_idr_sync;
                        expected_seq = None;
                        continue;
                    }
                };

                // continuity check (wrap-safe)
                if let Some(exp) = expected_seq {
                    if pkt.sequence_number != exp {
                        dep.reset();
                        synced = !cfg.require_idr_sync;
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        continue;
                    }
                }
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));

                let nals = match dep.push_rtp_payload(pkt.payload) {
                    Ok(v) => v,
                    Err(_) => {
                        dep.reset();
                        synced = !cfg.require_idr_sync;
                        expected_seq = None;
                        continue;
                    }
                };

                for nal in nals {
                    let Some(nt) = nal_type_from_annexb(&nal) else {
                        dep.reset();
                        synced = !cfg.require_idr_sync;
                        expected_seq = None;
                        continue;
                    };

                    // cache SPS/PPS
                    match nt {
                        7 => {
                            last_sps = Some(nal.clone());
                        }
                        8 => {
                            last_pps = Some(nal.clone());
                        }
                        _ => {}
                    }

                    // If sync is required, wait for SPS+PPS then IDR
                    if !synced {
                        if nt == 5 && last_sps.is_some() && last_pps.is_some() {
                            // Prepend SPS/PPS before first IDR (helps MP4/decoders)
                            let _ = nal_tx.try_send(last_sps.as_ref().unwrap().clone());
                            let _ = nal_tx.try_send(last_pps.as_ref().unwrap().clone());
                            let _ = nal_tx.try_send(nal);
                            synced = true;
                        }
                        continue;
                    }

                    // Synced: prepend SPS/PPS before each IDR for robustness
                    if nt == 5 {
                        if let (Some(sps), Some(pps)) = (last_sps.as_ref(), last_pps.as_ref()) {
                            let _ = nal_tx.try_send(sps.clone());
                            let _ = nal_tx.try_send(pps.clone());
                        }
                    }

                    // Forward NAL downstream (recorder / pipeline)
                    let _ = nal_tx.try_send(nal);
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
