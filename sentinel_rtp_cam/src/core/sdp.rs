use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct SdpVideoTrack {
    pub payload_type: u8,
    pub clock_rate: u32,
    pub control: String,
}

pub fn parse_sdp_video_track(sdp: &str) -> Result<SdpVideoTrack> {
    let mut in_video = false;
    let mut pt: Option<u8> = None;
    let mut clock: Option<u32> = None;
    let mut control: Option<String> = None;

    for line in sdp.lines().map(|l| l.trim()) {
        if line.starts_with("m=video ") {
            in_video = true;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 {
                bail!("Bad m=video line");
            }
            pt = Some(parts[3].parse()?);
        } else if line.starts_with("m=") {
            in_video = false;
        }

        if !in_video {
            continue;
        }

        if let Some(p) = pt {
            if line.starts_with(&format!("a=rtpmap:{p}")) {
                if let Some((_, rhs)) = line.split_once(' ') {
                    if let Some((codec, rate)) = rhs.split_once('/') {
                        if codec.eq_ignore_ascii_case("H264") {
                            clock = Some(rate.parse()?);
                        }
                    }
                }
            }
            if line.starts_with("a=control:") {
                control = Some(line["a=control:".len()..].to_string());
            }
        }
    }

    Ok(SdpVideoTrack {
        payload_type: pt.ok_or_else(|| anyhow::anyhow!("No video PT found"))?,
        clock_rate: clock.unwrap_or(90000),
        control: control.ok_or_else(|| anyhow::anyhow!("No control found"))?,
    })
}
