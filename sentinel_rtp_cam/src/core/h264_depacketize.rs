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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_nal_packet() {
        let mut dep = H264Depacketizer::new();
        let payload = vec![0x65, 0xAA, 0xBB]; // NAL type 5 (IDR)
        let out = dep.push_rtp_payload(&payload).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][0..4], &ANNEXB_START);
        assert_eq!(out[0][4], 0x65);
    }

    #[test]
    fn stap_a_unpacking() {
        let mut dep = H264Depacketizer::new();
        let nal1 = vec![0x67, 0x11, 0x22];
        let nal2 = vec![0x68, 0x33];
        let mut payload = vec![24u8]; // STAP-A
        payload.extend_from_slice(&(nal1.len() as u16).to_be_bytes());
        payload.extend_from_slice(&nal1);
        payload.extend_from_slice(&(nal2.len() as u16).to_be_bytes());
        payload.extend_from_slice(&nal2);

        let out = dep.push_rtp_payload(&payload).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(&out[0][0..4], &ANNEXB_START);
        assert_eq!(&out[0][4..], &nal1);
        assert_eq!(&out[1][4..], &nal2);
    }

    #[test]
    fn fu_a_reassembly() {
        let mut dep = H264Depacketizer::new();
        let fu_indicator = 0x7C; // F=0, NRI=3, Type=28
        let fu_header_start = 0x85; // S=1, E=0, Type=5
        let fu_header_end = 0x45; // S=0, E=1, Type=5

        let p1 = vec![fu_indicator, fu_header_start, 0xAA, 0xBB];
        let p2 = vec![fu_indicator, fu_header_end, 0xCC];

        assert!(dep.push_rtp_payload(&p1).unwrap().is_empty());
        let out = dep.push_rtp_payload(&p2).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0][0..4], &ANNEXB_START);
        assert_eq!(out[0][4], 0x65); // reconstructed IDR header
        assert_eq!(&out[0][5..], &[0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn fu_a_continuation_without_start_is_error() {
        let mut dep = H264Depacketizer::new();
        let fu_indicator = 0x7C;
        let fu_header_mid = 0x05; // S=0, E=0, Type=5
        let p = vec![fu_indicator, fu_header_mid, 0xAA];
        assert!(dep.push_rtp_payload(&p).is_err());
    }
}
