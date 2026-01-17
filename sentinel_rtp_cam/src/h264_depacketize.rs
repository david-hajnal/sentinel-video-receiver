use anyhow::{bail, Result};

const ANNEXB_START: [u8; 4] = [0, 0, 0, 1];

#[derive(Default)]
pub struct H264Depacketizer {
    fu_active: bool,
    fu_buf: Vec<u8>,
}

impl H264Depacketizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.fu_active = false;
        self.fu_buf.clear();
    }

    pub fn push_rtp_payload(&mut self, payload: &[u8]) -> Result<Vec<Vec<u8>>> {
        if payload.is_empty() {
            return Ok(vec![]);
        }
        let nal_type = payload[0] & 0x1F;

        match nal_type {
            1..=23 => {
                let mut out = Vec::with_capacity(ANNEXB_START.len() + payload.len());
                out.extend_from_slice(&ANNEXB_START);
                out.extend_from_slice(payload);
                Ok(vec![out])
            }
            24 => self.handle_stap_a(payload), // ✅ add this
            28 => self.handle_fu_a(payload),
            _ => bail!("Unsupported H.264 RTP NAL type: {}", nal_type),
        }
    }

    fn handle_stap_a(&mut self, payload: &[u8]) -> Result<Vec<Vec<u8>>> {
        // payload[0] is the STAP-A indicator, skip it
        let mut i = 1usize;
        let mut out = Vec::new();

        while i + 2 <= payload.len() {
            let nal_len = u16::from_be_bytes([payload[i], payload[i + 1]]) as usize;
            i += 2;

            if nal_len == 0 {
                bail!("STAP-A contains zero-length NAL");
            }
            if i + nal_len > payload.len() {
                bail!(
                    "STAP-A NAL length {} exceeds remaining payload {}",
                    nal_len,
                    payload.len().saturating_sub(i)
                );
            }

            let nal = &payload[i..i + nal_len];
            i += nal_len;

            let mut annexb = Vec::with_capacity(ANNEXB_START.len() + nal.len());
            annexb.extend_from_slice(&ANNEXB_START);
            annexb.extend_from_slice(nal);
            out.push(annexb);
        }

        // If there are trailing bytes, that’s malformed (or padding); be strict for now.
        if i != payload.len() {
            bail!("STAP-A has trailing bytes: {} leftover", payload.len() - i);
        }

        Ok(out)
    }

    fn handle_fu_a(&mut self, payload: &[u8]) -> Result<Vec<Vec<u8>>> {
        if payload.len() < 2 {
            bail!("FU-A too short");
        }

        let fu_indicator = payload[0];
        let fu_header = payload[1];
        let start = (fu_header & 0x80) != 0;
        let end = (fu_header & 0x40) != 0;
        let nal_type = fu_header & 0x1F;

        let forbidden_and_nri = fu_indicator & 0xE0;
        let reconstructed_nal_header = forbidden_and_nri | nal_type;
        let fu_payload = &payload[2..];

        if start {
            self.fu_active = true;
            self.fu_buf.clear();
            self.fu_buf.extend_from_slice(&ANNEXB_START);
            self.fu_buf.push(reconstructed_nal_header);
            self.fu_buf.extend_from_slice(fu_payload);
            return Ok(vec![]);
        }

        if !self.fu_active {
            bail!("FU-A continuation without start");
        }

        self.fu_buf.extend_from_slice(fu_payload);

        if end {
            self.fu_active = false;
            let completed = std::mem::take(&mut self.fu_buf);
            return Ok(vec![completed]);
        }

        Ok(vec![])
    }
}
