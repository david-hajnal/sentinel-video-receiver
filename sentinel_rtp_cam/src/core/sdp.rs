use anyhow::{bail, Result};
use base64::Engine;

#[derive(Debug, Clone)]
pub struct SdpVideoTrack {
    pub payload_type: u8,
    pub clock_rate: u32,
    pub codec_name: Option<String>,
    pub control: String,
    pub sprop_sps: Option<Vec<u8>>, // raw SPS (no Annex-B start code)
    pub sprop_pps: Option<Vec<u8>>, // raw PPS (no Annex-B start code)
}

pub fn parse_sdp_video_track(sdp: &str) -> Result<SdpVideoTrack> {
    let mut in_video = false;
    let mut pt: Option<u8> = None;
    let mut clock: Option<u32> = None;
    let mut codec_name: Option<String> = None;
    let mut control: Option<String> = None;
    let mut sprop_sps: Option<Vec<u8>> = None;
    let mut sprop_pps: Option<Vec<u8>> = None;

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
                        codec_name = Some(codec.trim().to_string());
                        clock = Some(rate.parse()?);
                    }
                }
            }
            if line.starts_with(&format!("a=fmtp:{p}")) {
                if let Some((_, rhs)) = line.split_once(' ') {
                    for part in rhs.split(';').map(|v| v.trim()) {
                        if let Some(v) = part.strip_prefix("sprop-parameter-sets=") {
                            let mut it = v.split(',');
                            let sps_b64 = it.next().unwrap_or("");
                            let pps_b64 = it.next().unwrap_or("");
                            if !sps_b64.is_empty() && !pps_b64.is_empty() {
                                let decode = |s: &str| {
                                    base64::engine::general_purpose::STANDARD
                                        .decode(s.as_bytes())
                                        .ok()
                                };
                                sprop_sps = decode(sps_b64);
                                sprop_pps = decode(pps_b64);
                            }
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
        codec_name,
        control: control.ok_or_else(|| anyhow::anyhow!("No control found"))?,
        sprop_sps,
        sprop_pps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sprop_parameter_sets() {
        let sps = vec![0x67, 0x42, 0x00, 0x1F];
        let pps = vec![0x68, 0xCE, 0x06];
        let sps_b64 = base64::engine::general_purpose::STANDARD.encode(&sps);
        let pps_b64 = base64::engine::general_purpose::STANDARD.encode(&pps);

        let sdp = format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=No Name\r\n\
             t=0 0\r\n\
             m=video 0 RTP/AVP 96\r\n\
             a=rtpmap:96 H264/90000\r\n\
             a=fmtp:96 packetization-mode=1; sprop-parameter-sets={},{}\r\n\
             a=control:trackID=0\r\n",
            sps_b64, pps_b64
        );

        let track = parse_sdp_video_track(&sdp).unwrap();
        assert_eq!(track.codec_name.as_deref(), Some("H264"));
        assert_eq!(track.sprop_sps.unwrap(), sps);
        assert_eq!(track.sprop_pps.unwrap(), pps);
    }

    #[test]
    fn parses_non_h264_codec_name() {
        let sdp = "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=No Name\r\n\
             t=0 0\r\n\
             m=video 0 RTP/AVP 96\r\n\
             a=rtpmap:96 H265/90000\r\n\
             a=control:trackID=0\r\n";

        let track = parse_sdp_video_track(sdp).unwrap();
        assert_eq!(track.codec_name.as_deref(), Some("H265"));
        assert_eq!(track.clock_rate, 90000);
    }

    #[test]
    fn missing_rtpmap_keeps_codec_unknown_with_default_clock() {
        let sdp = "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=No Name\r\n\
             t=0 0\r\n\
             m=video 0 RTP/AVP 96\r\n\
             a=control:trackID=0\r\n";

        let track = parse_sdp_video_track(sdp).unwrap();
        assert_eq!(track.codec_name, None);
        assert_eq!(track.clock_rate, 90000);
    }
}
