use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct RtpPacket<'a> {
    pub version: u8,
    pub padding: bool,
    pub extension: bool,
    pub csrc_count: u8,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub payload: &'a [u8],
}

impl<'a> RtpPacket<'a> {
    pub fn parse(buf: &'a [u8]) -> Result<Self> {
        if buf.len() < 12 {
            bail!("RTP packet too short: {}", buf.len());
        }

        let b0 = buf[0];
        let b1 = buf[1];

        let version = b0 >> 6;
        if version != 2 {
            bail!("Unsupported RTP version: {}", version);
        }

        let padding = (b0 & 0x20) != 0;
        let extension = (b0 & 0x10) != 0;
        let csrc_count = b0 & 0x0F;

        let marker = (b1 & 0x80) != 0;
        let payload_type = b1 & 0x7F;

        let sequence_number = u16::from_be_bytes([buf[2], buf[3]]);
        let timestamp = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let ssrc = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);

        // Start after fixed header
        let mut off = 12usize;

        // Skip CSRC list if present
        let csrc_bytes = (csrc_count as usize) * 4;
        if buf.len() < off + csrc_bytes {
            bail!("RTP packet too short for CSRC list");
        }
        off += csrc_bytes;

        // Skip header extension if present
        if extension {
            // extension header = 16-bit id + 16-bit length (in 32-bit words)
            if buf.len() < off + 4 {
                bail!("RTP packet too short for extension header");
            }
            let ext_len_words = u16::from_be_bytes([buf[off + 2], buf[off + 3]]) as usize;
            off += 4;

            let ext_bytes = ext_len_words * 4;
            if buf.len() < off + ext_bytes {
                bail!("RTP packet too short for extension data");
            }
            off += ext_bytes;
        }

        // Compute payload end (strip padding if present)
        let mut end = buf.len();
        if padding {
            if end == 0 {
                bail!("RTP padding flag set on empty packet");
            }
            let pad_len = buf[end - 1] as usize;
            if pad_len == 0 || pad_len > end.saturating_sub(off) {
                bail!("Invalid RTP padding length: {}", pad_len);
            }
            end -= pad_len;
        }

        if off > end {
            bail!("RTP payload offset beyond end (off={}, end={})", off, end);
        }

        let payload = &buf[off..end];

        Ok(Self {
            version,
            padding,
            extension,
            csrc_count,
            marker,
            payload_type,
            sequence_number,
            timestamp,
            ssrc,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RtpPacket;

    #[test]
    fn parse_supports_csrc_extension_and_padding() {
        let packet = [
            0xB1, 0xE0, 0x12, 0x34, 0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22,
            0x33, 0x44, 0xBE, 0xDE, 0x00, 0x01, 0xDE, 0xAD, 0xBE, 0xEF, 0x09, 0x08, 0x07, 0x00,
            0x00, 0x00, 0x04,
        ];

        let parsed = RtpPacket::parse(&packet).unwrap();

        assert_eq!(parsed.version, 2);
        assert!(parsed.padding);
        assert!(parsed.extension);
        assert_eq!(parsed.csrc_count, 1);
        assert!(parsed.marker);
        assert_eq!(parsed.payload_type, 96);
        assert_eq!(parsed.sequence_number, 0x1234);
        assert_eq!(parsed.timestamp, 0x0102_0304);
        assert_eq!(parsed.ssrc, 0xAABB_CCDD);
        assert_eq!(parsed.payload, &[0x09, 0x08, 0x07]);
    }

    #[test]
    fn parse_rejects_invalid_rtp_version() {
        let packet = [0x00, 0x60, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1];
        let err = RtpPacket::parse(&packet).unwrap_err();
        assert!(err.to_string().contains("Unsupported RTP version"));
    }

    #[test]
    fn parse_rejects_invalid_padding_length() {
        let packet = [0xA0, 0x60, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0xAA, 0xBB, 0x04];
        let err = RtpPacket::parse(&packet).unwrap_err();
        assert!(err.to_string().contains("Invalid RTP padding length"));
    }
}
