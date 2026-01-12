use anyhow::{bail, Result};
use tokio::io::AsyncWriteExt;

use sentinel_rtp_cam::h264_depacketize::H264Depacketizer;
use sentinel_rtp_cam::interleaved::{read_interleaved_frame, InterleavedFrame};
use sentinel_rtp_cam::rtp::RtpPacket;
use sentinel_rtp_cam::rtsp::RtspClient;
use sentinel_rtp_cam::sdp::parse_sdp_video_track;

fn header_value<'a>(headers: &'a [(String, String)], key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.as_str())
}

fn parse_interleaved_channels(transport: &str) -> Option<(u8, u8)> {
    // Example: "RTP/AVP/TCP;unicast;interleaved=0-1;mode=play"
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
    let rtsp_url = "rtsp://admin:Ratatata4@@192.168.1.187:8554/stream1";
    let host = "192.168.1.187";
    let port = 8554;

    let mut c = RtspClient::connect(host, port).await?;

    let r = c.request("OPTIONS", rtsp_url, &[], None).await?;
    println!("OPTIONS status: {}", r.status);
    for (k, v) in &r.headers {
        if k.eq_ignore_ascii_case("Public") {
            println!("Public: {v}");
        }
    }
    if r.status != 200 {
        bail!("OPTIONS failed: {}", r.status);
    }

    let r = c
        .request("DESCRIBE", rtsp_url, &[("Accept", "application/sdp")], None)
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

    // Interleaved channels 0 (RTP) and 1 (RTCP)
    let transport = "RTP/AVP/TCP;unicast;interleaved=0-1;mode=play";
    let r = c
        .request("SETUP", &setup_url, &[("Transport", transport)], None)
        .await?;
    if r.status != 200 {
        bail!("SETUP failed: {}", r.status);
    }
    c.set_session_from(&r);

    let transport_resp = header_value(&r.headers, "Transport")
        .ok_or_else(|| anyhow::anyhow!("SETUP response missing Transport header"))?;

    let (rtp_chan, rtcp_chan) = parse_interleaved_channels(transport_resp).ok_or_else(|| {
        anyhow::anyhow!("SETUP Transport missing interleaved=..-..: {transport_resp}")
    })?;
    println!("Negotiated interleaved channels: RTP={rtp_chan}, RTCP={rtcp_chan}");

    let r = c
        .request("PLAY", rtsp_url, &[("Range", "npt=0.000-")], None)
        .await?;
    if r.status != 200 {
        eprintln!("PLAY failed: {}", r.status);
        for (k, v) in &r.headers {
            eprintln!("  {}: {}", k, v);
        }
        bail!("PLAY failed: {}", r.status);
    }

    println!("TCP interleaved mode: reading RTP/RTCP from RTSP TCP connection");

    let mut out = tokio::fs::File::create("tcp_out.h264").await?;
    let mut dep = H264Depacketizer::new();

    // --- Sync gate state ---
    let mut last_sps: Option<Vec<u8>> = None;
    let mut last_pps: Option<Vec<u8>> = None;
    let mut synced = false;

    // --- RTP integrity state ---
    let mut expected_seq: Option<u16> = None;

    loop {
        match read_interleaved_frame(&mut c, rtp_chan, rtcp_chan).await? {
            InterleavedFrame::Rtp(bytes) => {
                let pkt = match RtpPacket::parse(&bytes) {
                    Ok(p) => p,
                    Err(_) => {
                        dep.reset();
                        synced = false;
                        expected_seq = None;
                        continue;
                    }
                };

                // RTP sequence continuity check (wrap-safe)
                if let Some(exp) = expected_seq {
                    if pkt.sequence_number != exp {
                        dep.reset();
                        synced = false;
                        expected_seq = Some(pkt.sequence_number.wrapping_add(1));
                        continue;
                    }
                }
                expected_seq = Some(pkt.sequence_number.wrapping_add(1));

                for nal in dep.push_rtp_payload(pkt.payload)? {
                    let Some(nt) = nal_type_from_annexb(&nal) else {
                        // If depacketizer ever outputs something unexpected, resync hard
                        dep.reset();
                        synced = false;
                        expected_seq = None;
                        continue;
                    };

                    // Cache SPS/PPS; don't write them immediately (we'll prepend at IDR)
                    match nt {
                        7 => {
                            last_sps = Some(nal);
                            continue;
                        }
                        8 => {
                            last_pps = Some(nal);
                            continue;
                        }
                        _ => {}
                    }

                    // Not synced: wait for SPS+PPS and then an IDR.
                    if !synced {
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
                        if let (Some(sps), Some(pps)) = (last_sps.as_ref(), last_pps.as_ref()) {
                            out.write_all(sps).await?;
                            out.write_all(pps).await?;
                        }
                    }

                    out.write_all(&nal).await?;
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
