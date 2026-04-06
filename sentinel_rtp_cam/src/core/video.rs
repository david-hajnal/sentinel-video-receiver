#[derive(Debug, Clone)]
pub struct VideoNal {
    pub data: Vec<u8>, // Annex-B NAL (00 00 00 01 ...)
    pub rtp_ts: u32,   // RTP timestamp (90k clock for H264)
    pub marker: bool,  // RTP marker bit (often end of access unit)
}

pub fn annexb_from_raw_nal(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + raw.len());
    out.extend_from_slice(&[0, 0, 0, 1]);
    out.extend_from_slice(raw);
    out
}

pub fn nal_type_from_annexb(nal: &[u8]) -> Option<u8> {
    if nal.len() < 5 {
        return None;
    }
    if &nal[0..4] != [0, 0, 0, 1].as_slice() {
        return None;
    }
    Some(nal[4] & 0x1F)
}

#[cfg(test)]
mod tests {
    use super::{annexb_from_raw_nal, nal_type_from_annexb};

    #[test]
    fn annexb_from_raw_nal_prefixes_start_code() {
        let raw = [0x65, 0x88, 0x84];
        assert_eq!(annexb_from_raw_nal(&raw), vec![0, 0, 0, 1, 0x65, 0x88, 0x84]);
    }

    #[test]
    fn nal_type_from_annexb_extracts_type_for_valid_annexb() {
        assert_eq!(nal_type_from_annexb(&[0, 0, 0, 1, 0x67, 0x64]), Some(7));
    }

    #[test]
    fn nal_type_from_annexb_rejects_truncated_or_non_annexb_input() {
        assert_eq!(nal_type_from_annexb(&[0, 0, 0, 1]), None);
        assert_eq!(nal_type_from_annexb(&[0, 0, 1, 0x65, 0x88]), None);
    }
}
