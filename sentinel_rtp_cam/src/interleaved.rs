use crate::rtsp::RtspClient;
use anyhow::{bail, Result};

#[derive(Debug)]
pub enum InterleavedFrame {
    Rtp(Vec<u8>),
    Rtcp(Vec<u8>),
    Unknown(u8, Vec<u8>),
}

pub async fn read_interleaved_frame(
    c: &mut RtspClient,
    rtp_channel: u8,
    rtcp_channel: u8,
) -> Result<InterleavedFrame> {
    let b = c.read_exact_bytes(1).await?;
    if b[0] != 0x24 {
        bail!("Expected '$' (0x24), got 0x{:02x}", b[0]);
    }

    let hdr = c.read_exact_bytes(3).await?;
    let channel = hdr[0];
    let len = u16::from_be_bytes([hdr[1], hdr[2]]) as usize;

    let payload = c.read_exact_bytes(len).await?;

    Ok(if channel == rtp_channel {
        InterleavedFrame::Rtp(payload)
    } else if channel == rtcp_channel {
        InterleavedFrame::Rtcp(payload)
    } else {
        InterleavedFrame::Unknown(channel, payload)
    })
}
