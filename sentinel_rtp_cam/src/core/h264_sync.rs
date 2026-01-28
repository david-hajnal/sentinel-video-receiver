use crate::core::video::nal_type_from_annexb;

#[derive(Debug, Clone)]
pub struct H264SyncGate {
    require_idr_sync: bool,
    synced: bool,
    last_sps: Option<Vec<u8>>,
    last_pps: Option<Vec<u8>>,
}

impl H264SyncGate {
    pub fn new(require_idr_sync: bool) -> Self {
        Self {
            require_idr_sync,
            synced: !require_idr_sync,
            last_sps: None,
            last_pps: None,
        }
    }

    pub fn reset(&mut self) {
        self.synced = !self.require_idr_sync;
    }

    pub fn set_sprop_param_sets(&mut self, sps: Vec<u8>, pps: Vec<u8>) {
        self.last_sps = Some(sps);
        self.last_pps = Some(pps);
    }

    pub fn push_nal(&mut self, nal: Vec<u8>) -> Vec<Vec<u8>> {
        let Some(nt) = nal_type_from_annexb(&nal) else {
            return vec![];
        };

        match nt {
            7 => {
                self.last_sps = Some(nal);
                return vec![];
            }
            8 => {
                self.last_pps = Some(nal);
                return vec![];
            }
            _ => {}
        }

        if !self.synced {
            if nt == 5 {
                if let (Some(sps), Some(pps)) = (self.last_sps.as_ref(), self.last_pps.as_ref()) {
                    self.synced = true;
                    return vec![sps.clone(), pps.clone(), nal];
                }
            }
            return vec![];
        }

        if nt == 5 {
            let mut out = Vec::with_capacity(3);
            if let (Some(sps), Some(pps)) = (self.last_sps.as_ref(), self.last_pps.as_ref()) {
                out.push(sps.clone());
                out.push(pps.clone());
            }
            out.push(nal);
            return out;
        }

        vec![nal]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn annexb_nal(nal_type: u8) -> Vec<u8> {
        vec![0, 0, 0, 1, nal_type & 0x1F]
    }

    #[test]
    fn gate_waits_for_sps_pps_and_idr() {
        let mut gate = H264SyncGate::new(true);

        assert!(gate.push_nal(annexb_nal(1)).is_empty());
        assert!(gate.push_nal(annexb_nal(7)).is_empty());
        assert!(gate.push_nal(annexb_nal(8)).is_empty());

        let out = gate.push_nal(annexb_nal(5));
        assert_eq!(out.len(), 3);
        assert_eq!(out[0][4] & 0x1F, 7);
        assert_eq!(out[1][4] & 0x1F, 8);
        assert_eq!(out[2][4] & 0x1F, 5);
    }

    #[test]
    fn gate_resets_on_discontinuity() {
        let mut gate = H264SyncGate::new(true);
        gate.set_sprop_param_sets(annexb_nal(7), annexb_nal(8));

        let out = gate.push_nal(annexb_nal(5));
        assert_eq!(out.len(), 3);

        gate.reset();
        assert!(gate.push_nal(annexb_nal(1)).is_empty());
    }
}
