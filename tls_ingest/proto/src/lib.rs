use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_PAYLOAD: usize = 2 * 1024 * 1024;

#[repr(u8)]
pub enum RecordType {
    Hello = 1,
    Rtp = 2,
    Event = 3,
    Ping = 4,
    Close = 5,
    HelloOk = 6,
    Error = 7,
    Gap = 8,
}

pub struct Record {
    pub record_type: u8,
    pub flags: u8,
    pub payload: Vec<u8>,
}

pub async fn read_record<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Record> {
    let mut header = [0u8; 6];
    reader.read_exact(&mut header).await?;
    let record_type = header[0];
    let flags = header[1];
    let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    if len > MAX_PAYLOAD {
        return Err(anyhow!("record payload too large: {}", len));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        reader.read_exact(&mut payload).await?;
    }
    Ok(Record {
        record_type,
        flags,
        payload,
    })
}

pub async fn write_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    record_type: u8,
    flags: u8,
    payload: &[u8],
) -> Result<()> {
    if payload.len() > MAX_PAYLOAD {
        return Err(anyhow!("record payload too large: {}", payload.len()));
    }
    let len = payload.len() as u32;
    let mut header = [0u8; 6];
    header[0] = record_type;
    header[1] = flags;
    header[2..6].copy_from_slice(&len.to_be_bytes());
    writer.write_all(&header).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    writer.flush().await?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Hello {
    pub agent_id: String,
    pub token: String,
    pub streams: Vec<String>,
    pub timestamp_unix: i64,
    pub nonce: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloOk {
    pub session_id: String,
    pub max_payload: u32,
    pub ping_interval_sec: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventPayload {
    pub stream_id: String,
    pub event_type: String,
    pub state: String,
    pub event_ts_unix_ms: i64,
    pub confidence: f64,
}
