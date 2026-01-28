//! Zero-copy RTP packet parsing
//!
//! This module provides efficient RTP packet parsing using `bytes::Bytes` for zero-copy operations.
//! Implements RFC 3550 (RTP: A Transport Protocol for Real-Time Applications).
//!
//! ## Example
//!
//! ```no_run
//! use sentinel_rtp_cam::core::rtp_refactored::RtpPacket;
//! use bytes::Bytes;
//!
//! let data = Bytes::from_static(&[
//!     0x80, 0x60, 0x00, 0x01, // V=2, P=0, X=0, CC=0, M=0, PT=96, seq=1
//!     0x00, 0x00, 0x00, 0x00, // timestamp=0
//!     0x12, 0x34, 0x56, 0x78, // SSRC
//!     // payload follows...
//! ]);
//!
//! let packet = RtpPacket::parse(data)?;
//! assert_eq!(packet.version(), 2);
//! assert_eq!(packet.payload_type(), 96);
//! # Ok::<(), sentinel_rtp_cam::Error>(())
//! ```

use crate::utils::error::{Result, RtpError};
use bytes::Bytes;

/// Parsed RTP packet with zero-copy payload reference
///
/// Uses `Bytes` for efficient payload handling without unnecessary copying.
/// Lifetime-free design enables easier integration with async code.
#[derive(Debug, Clone)]
pub struct RtpPacket {
    /// Full packet data (includes header + payload)
    data: Bytes,
    /// Offset where payload starts
    payload_offset: usize,
    /// Length of payload (excluding padding)
    payload_len: usize,
    /// RTP sequence number
    sequence_number: u16,
    /// RTP timestamp (90kHz for H.264)
    timestamp: u32,
    /// Synchronization source identifier
    ssrc: u32,
    /// Marker bit (often indicates end of frame)
    marker: bool,
    /// Payload type
    payload_type: u8,
}

impl RtpPacket {
    /// Parse an RTP packet from bytes with zero-copy payload extraction
    ///
    /// # Errors
    ///
    /// Returns `RtpError::PacketTooShort` if less than 12 bytes.
    /// Returns `RtpError::UnsupportedVersion` if not RTP version 2.
    /// Returns `RtpError::InvalidCsrcList` if CSRC list exceeds packet.
    /// Returns `RtpError::InvalidExtension` if extension header is malformed.
    /// Returns `RtpError::InvalidPadding` if padding value is invalid.
    pub fn parse(data: Bytes) -> Result<Self> {
        if data.len() < 12 {
            return Err(RtpError::PacketTooShort(data.len()).into());
        }

        let b0 = data[0];
        let b1 = data[1];

        let version = b0 >> 6;
        if version != 2 {
            return Err(RtpError::UnsupportedVersion(version).into());
        }

        let padding = (b0 & 0x20) != 0;
        let extension = (b0 & 0x10) != 0;
        let csrc_count = b0 & 0x0F;

        let marker = (b1 & 0x80) != 0;
        let payload_type = b1 & 0x7F;

        let sequence_number = u16::from_be_bytes([data[2], data[3]]);
        let timestamp = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let ssrc = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

        // Calculate payload offset by skipping fixed header + CSRC + extension
        let mut offset = 12usize;

        // Skip CSRC list (4 bytes per entry)
        let csrc_bytes = (csrc_count as usize) * 4;
        if data.len() < offset + csrc_bytes {
            return Err(RtpError::InvalidCsrcList.into());
        }
        offset += csrc_bytes;

        // Skip header extension if present
        if extension {
            if data.len() < offset + 4 {
                return Err(RtpError::InvalidExtension.into());
            }
            // Extension: 16-bit profile + 16-bit length (in 32-bit words)
            let ext_len_words = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
            offset += 4; // extension header

            let ext_bytes = ext_len_words * 4;
            if data.len() < offset + ext_bytes {
                return Err(RtpError::InvalidExtension.into());
            }
            offset += ext_bytes;
        }

        // Calculate payload length (strip padding if present)
        let mut payload_end = data.len();
        if padding {
            if payload_end == 0 {
                return Err(RtpError::InvalidPadding(0).into());
            }
            let pad_len = data[payload_end - 1] as usize;
            if pad_len == 0 || pad_len > payload_end.saturating_sub(offset) {
                return Err(RtpError::InvalidPadding(pad_len).into());
            }
            payload_end -= pad_len;
        }

        if offset > payload_end {
            return Err(RtpError::PacketTooShort(data.len()).into());
        }

        let payload_len = payload_end - offset;

        Ok(Self {
            data,
            payload_offset: offset,
            payload_len,
            sequence_number,
            timestamp,
            ssrc,
            marker,
            payload_type,
        })
    }

    /// RTP version (always 2)
    #[inline]
    pub fn version(&self) -> u8 {
        2
    }

    /// Marker bit (typically indicates frame boundary)
    #[inline]
    pub fn marker(&self) -> bool {
        self.marker
    }

    /// Payload type
    #[inline]
    pub fn payload_type(&self) -> u8 {
        self.payload_type
    }

    /// Sequence number (wraps at 65535)
    #[inline]
    pub fn sequence_number(&self) -> u16 {
        self.sequence_number
    }

    /// RTP timestamp (90kHz clock for H.264)
    #[inline]
    pub fn timestamp(&self) -> u32 {
        self.timestamp
    }

    /// Synchronization source identifier
    #[inline]
    pub fn ssrc(&self) -> u32 {
        self.ssrc
    }

    /// Zero-copy access to payload bytes
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.data[self.payload_offset..self.payload_offset + self.payload_len]
    }

    /// Clone payload bytes (for ownership transfer)
    pub fn payload_bytes(&self) -> Bytes {
        self.data
            .slice(self.payload_offset..self.payload_offset + self.payload_len)
    }

    /// Check if this sequence follows expected (handles wrapping)
    pub fn follows(&self, expected: u16) -> bool {
        self.sequence_number == expected
    }

    /// Calculate next expected sequence number (with wrapping)
    #[inline]
    pub fn next_sequence(&self) -> u16 {
        self.sequence_number.wrapping_add(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_rtp_packet() {
        let data = Bytes::from_static(&[
            0x80, 0x60, 0x00, 0x01, // V=2, PT=96, seq=1
            0x00, 0x00, 0x00, 0x00, // timestamp=0
            0x12, 0x34, 0x56, 0x78, // SSRC
            0xAA, 0xBB, 0xCC, 0xDD, // payload
        ]);

        let pkt = RtpPacket::parse(data).unwrap();
        assert_eq!(pkt.version(), 2);
        assert_eq!(pkt.payload_type(), 96);
        assert_eq!(pkt.sequence_number(), 1);
        assert_eq!(pkt.timestamp(), 0);
        assert_eq!(pkt.ssrc(), 0x12345678);
        assert_eq!(pkt.payload(), &[0xAA, 0xBB, 0xCC, 0xDD]);
    }

    #[test]
    fn test_rtp_packet_too_short() {
        let data = Bytes::from_static(&[0x80, 0x60]);
        assert!(matches!(
            RtpPacket::parse(data),
            Err(crate::Error::Rtp(RtpError::PacketTooShort(_)))
        ));
    }

    #[test]
    fn test_marker_bit() {
        let data = Bytes::from_static(&[
            0x80, 0xE0, 0x00, 0x01, // V=2, M=1, PT=96
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]);

        let pkt = RtpPacket::parse(data).unwrap();
        assert!(pkt.marker());
    }

    #[test]
    fn test_sequence_wrapping() {
        let pkt = RtpPacket {
            data: Bytes::from_static(&[0u8; 12]),
            payload_offset: 12,
            payload_len: 0,
            sequence_number: 65535,
            timestamp: 0,
            ssrc: 0,
            marker: false,
            payload_type: 96,
        };
        assert_eq!(pkt.next_sequence(), 0);
    }
}
