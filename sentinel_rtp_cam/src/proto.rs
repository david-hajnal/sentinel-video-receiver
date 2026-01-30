use anyhow::{anyhow, bail, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAGIC: u32 = 0x534E5450; // "SNTP"
pub const VERSION: u16 = 1;
pub const MAX_LEN: usize = 256 * 1024;

pub const HELLO: u16 = 1;
pub const RTP: u16 = 2;
pub const GAP: u16 = 3;
pub const MOTION: u16 = 4;
pub const PING: u16 = 5;
pub const PONG: u16 = 6;

#[derive(Debug, Clone)]
pub struct Msg {
    pub msg_type: u16,
    pub stream_id: u32,
    pub payload: Vec<u8>,
}

pub async fn write_msg<W: AsyncWrite + Unpin>(w: &mut W, msg: &Msg) -> Result<()> {
    if msg.payload.len() > MAX_LEN {
        bail!("payload too large: {}", msg.payload.len());
    }

    let mut header = [0u8; 16];
    header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    header[4..6].copy_from_slice(&VERSION.to_be_bytes());
    header[6..8].copy_from_slice(&msg.msg_type.to_be_bytes());
    header[8..12].copy_from_slice(&msg.stream_id.to_be_bytes());
    header[12..16].copy_from_slice(&(msg.payload.len() as u32).to_be_bytes());

    w.write_all(&header).await?;
    if !msg.payload.is_empty() {
        w.write_all(&msg.payload).await?;
    }
    Ok(())
}

pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Msg> {
    let mut header = [0u8; 16];
    r.read_exact(&mut header).await?;

    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != MAGIC {
        bail!("bad magic: {:#x}", magic);
    }
    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != VERSION {
        bail!("bad version: {}", version);
    }

    let msg_type = u16::from_be_bytes([header[6], header[7]]);
    let stream_id = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    let len = u32::from_be_bytes([header[12], header[13], header[14], header[15]]) as usize;
    if len > MAX_LEN {
        bail!("payload too large: {}", len);
    }

    let mut payload = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut payload).await?;
    }

    Ok(Msg {
        msg_type,
        stream_id,
        payload,
    })
}

pub fn encode_gap(last_seq: u16, new_seq: u16) -> Vec<u8> {
    let mut out = [0u8; 4];
    out[0..2].copy_from_slice(&last_seq.to_be_bytes());
    out[2..4].copy_from_slice(&new_seq.to_be_bytes());
    out.to_vec()
}

pub fn decode_gap(buf: &[u8]) -> Result<(u16, u16)> {
    if buf.len() != 4 {
        return Err(anyhow!("gap payload len != 4"));
    }
    let last = u16::from_be_bytes([buf[0], buf[1]]);
    let new = u16::from_be_bytes([buf[2], buf[3]]);
    Ok((last, new))
}
