use anyhow::{bail, Result};
use tokio::{io::AsyncWriteExt, net::UdpSocket};
use tracing::{info, warn};

use sentinel_rtp_cam::core::h264_depacketize::H264Depacketizer;
use sentinel_rtp_cam::core::h264_sync::H264SyncGate;
use sentinel_rtp_cam::core::rtp::RtpPacket;
use sentinel_rtp_cam::rtsp::rtsp::RtspClient;
use sentinel_rtp_cam::core::sdp::parse_sdp_video_track;
use sentinel_rtp_cam::core::video::annexb_from_raw_nal;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load environment variables from .env file
    if dotenvy::dotenv().is_err() {
        dotenvy::from_filename("../.env").ok();
    }

    // Read configuration from environment variables
    let host = std::env::var("UDP_RTSP_HOST").unwrap_or_else(|_| "192.168.1.187".to_string());
    let port: u16 = std::env::var("UDP_RTSP_PORT")
        .unwrap_or_else(|_| "8554".to_string())
        .parse()
        .unwrap_or(8554);
    let path = std::env::var("UDP_RTSP_PATH").unwrap_or_else(|_| "/cam".to_string());
    let rtsp_url = format!("rtsp://{}:{}{}", host, port, path);

    let mut c = RtspClient::connect(&host, port).await?;

    let r = c.request("OPTIONS", &rtsp_url, &[], None).await?;
    if r.status != 200 {
        bail!("OPTIONS failed: {}", r.status);
    }

    let r = c
        .request(
            "DESCRIBE",
            &rtsp_url,
            &[("Accept", "application/sdp")],
            None,
        )
        .await?;
    if r.status != 200 {
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

    let rtp_port: u16 = std::env::var("UDP_RTP_PORT")
        .unwrap_or_else(|_| "5004".to_string())
        .parse()
        .unwrap_or(5004);
    let rtcp_port: u16 = std::env::var("UDP_RTCP_PORT")
        .unwrap_or_else(|_| "5005".to_string())
        .parse()
        .unwrap_or(5005);

    let transport = format!(
        "RTP/AVP;unicast;client_port={}-{};mode=play",
        rtp_port, rtcp_port
    );
    let r = c
        .request("SETUP", &setup_url, &[("Transport", &transport)], None)
        .await?;
    if r.status != 200 {
        bail!("SETUP failed: {}", r.status);
    }
    c.set_session_from(&r);

    let r = c
        .request("PLAY", &rtsp_url, &[("Range", "npt=0.000-")], None)
        .await?;
    if r.status != 200 {
        bail!("PLAY failed: {}", r.status);
    }

    let sock = UdpSocket::bind(("0.0.0.0", rtp_port)).await?;
    info!(port = rtp_port, "Receiving RTP on UDP socket");

    let output_file =
        std::env::var("UDP_OUTPUT_FILE").unwrap_or_else(|_| "udp_out.h264".to_string());
    let mut out = tokio::fs::File::create(&output_file).await?;
    let mut dep = H264Depacketizer::new();
    let mut gate = H264SyncGate::new(true);
    if let (Some(sps), Some(pps)) = (track.sprop_sps, track.sprop_pps) {
        gate.set_sprop_param_sets(annexb_from_raw_nal(&sps), annexb_from_raw_nal(&pps));
    }

    // Bigger buffer reduces chances of truncation if packets are larger than expected.
    let mut buf = vec![0u8; 8192];

    // --- RTP integrity state ---
    let mut expected_seq: Option<u16> = None;

    loop {
        let (n, _) = sock.recv_from(&mut buf).await?;
        let pkt = match RtpPacket::parse(&buf[..n]) {
            Ok(p) => p,
            Err(_) => {
                // If we can't parse an RTP packet, treat as stream discontinuity.
                warn!("Failed to parse RTP packet, resetting depacketizer");
                dep.reset();
                gate.reset();
                expected_seq = None;
                continue;
            }
        };

        // RTP sequence continuity check (wrap-safe)
        if let Some(exp) = expected_seq {
            if pkt.sequence_number != exp {
                // Gap/out-of-order: drop this packet and resync at next IDR.
                warn!(
                    expected = exp,
                    received = pkt.sequence_number,
                    "RTP sequence discontinuity, resyncing"
                );
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
}
