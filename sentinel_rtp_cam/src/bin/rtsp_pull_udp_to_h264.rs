use anyhow::{bail, Result};
use tokio::{io::AsyncWriteExt, net::UdpSocket};

use sentinel_rtp_cam::h264_depacketize::H264Depacketizer;
use sentinel_rtp_cam::rtp::RtpPacket;
use sentinel_rtp_cam::rtsp::RtspClient;
use sentinel_rtp_cam::sdp::parse_sdp_video_track;

fn nal_type_from_annexb(nal: &[u8]) -> Option<u8> {
    // Expect Annex-B: 00 00 00 01 <nal_header> ...
    if nal.len() < 5 {
        return None;
    }
    if nal[0..4] != [0, 0, 0, 1] {
        return None;
    }
    Some(nal[4] & 0x1F)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if it exists
    dotenvy::dotenv().ok();

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
        .request("DESCRIBE", &rtsp_url, &[("Accept", "application/sdp")], None)
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
    println!("UDP mode: receiving RTP on udp://0.0.0.0:{rtp_port}");

    let output_file = std::env::var("UDP_OUTPUT_FILE").unwrap_or_else(|_| "udp_out.h264".to_string());
    let mut out = tokio::fs::File::create(&output_file).await?;
    let mut dep = H264Depacketizer::new();

    // Bigger buffer reduces chances of truncation if packets are larger than expected.
    let mut buf = vec![0u8; 8192];

    // --- Sync gate state ---
    let mut last_sps: Option<Vec<u8>> = None;
    let mut last_pps: Option<Vec<u8>> = None;
    let mut synced = false;

    // --- RTP integrity state ---
    let mut expected_seq: Option<u16> = None;

    loop {
        let (n, _) = sock.recv_from(&mut buf).await?;
        let pkt = match RtpPacket::parse(&buf[..n]) {
            Ok(p) => p,
            Err(_) => {
                // If we can't parse an RTP packet, treat as stream discontinuity.
                println!("can't parse an RTP packet");
                dep.reset();
                synced = false;
                expected_seq = None;
                continue;
            }
        };

        // RTP sequence continuity check (wrap-safe)
        if let Some(exp) = expected_seq {
            if pkt.sequence_number != exp {
                // Gap/out-of-order: drop this packet and resync at next IDR.
                println!("Gap/out-of-order: drop this packet and resync at next IDR.");
                dep.reset();
                synced = false;
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                continue;
            }
        }
        expected_seq = Some(pkt.sequence_number.wrapping_add(1));

        for nal in dep.push_rtp_payload(pkt.payload)? {
            let Some(nt) = nal_type_from_annexb(&nal) else {
                // If depacketizer ever outputs something unexpected, resync hard.
                println!("Resync hard");
                dep.reset();
                synced = false;
                expected_seq = None;
                continue;
            };

            // Cache SPS/PPS; don't write them immediately (we'll prepend at IDR).
            match nt {
                7 => {
                    println!("Cache SPS");
                    last_sps = Some(nal);
                    continue;
                }
                8 => {
                    println!("Cache PPS");
                    last_pps = Some(nal);
                    continue;
                }
                _ => {}
            }

            // Not synced yet: drop everything until SPS+PPS and then an IDR.
            if !synced {
                println!("Not synced yet");
                if nt == 5 && last_sps.is_some() && last_pps.is_some() {
                    out.write_all(last_sps.as_ref().unwrap()).await?;
                    out.write_all(last_pps.as_ref().unwrap()).await?;
                    out.write_all(&nal).await?;
                    synced = true;
                }
                continue;
            }

            // Synced: prepend SPS/PPS before every IDR for robustness.
            if nt == 5 {
                println!("Synced: prepend SPS/PPS before every IDR for robustness.");
                if let (Some(sps), Some(pps)) = (last_sps.as_ref(), last_pps.as_ref()) {
                    out.write_all(sps).await?;
                    out.write_all(pps).await?;
                }
            }

            out.write_all(&nal).await?;
        }
    }
}
