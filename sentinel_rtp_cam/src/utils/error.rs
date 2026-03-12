//! Comprehensive error types for the sentinel RTSP library.
//!
//! Using `thiserror` provides better type safety, clearer error messages,
//! and enables matching on specific error variants for recovery logic.

use std::io;

/// Main result type used throughout the library
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error enum encompassing all error categories
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// RTSP protocol errors
    #[error("RTSP error: {0}")]
    Rtsp(#[from] RtspError),

    /// RTP protocol errors
    #[error("RTP error: {0}")]
    Rtp(#[from] RtpError),

    /// SDP parsing errors
    #[error("SDP error: {0}")]
    Sdp(#[from] SdpError),

    /// H264 depacketization errors
    #[error("H264 error: {0}")]
    H264(#[from] H264Error),

    /// ONVIF/SOAP errors
    #[error("ONVIF error: {0}")]
    Onvif(#[from] OnvifError),

    /// Network I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// HTTP client errors (for ONVIF)
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Generic error for exceptional cases
    #[error("Internal error: {0}")]
    Internal(String),
}

// ============================================================================
// RTSP Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum RtspError {
    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Invalid status code: {0}")]
    InvalidStatus(u16),

    #[error("Missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("Headers too large (> {0} bytes)")]
    HeadersTooLarge(usize),

    #[error("Invalid status line: {0}")]
    InvalidStatusLine(String),

    #[error("Body length mismatch: expected {expected}, got {actual}")]
    BodyLengthMismatch { expected: usize, actual: usize },

    #[error("Authentication required (status {0})")]
    AuthRequired(u16),

    #[error("Server returned error: {status} - {message}")]
    ServerError { status: u16, message: String },

    #[error("Unsupported transport: {0}")]
    UnsupportedTransport(String),

    #[error("Interleaved frame error: expected marker 0x24, got {0:#x}")]
    InvalidInterleavedMarker(u8),

    #[error("Interleaved channel mismatch: expected {expected}, got {actual}")]
    ChannelMismatch { expected: u8, actual: u8 },
}

// ============================================================================
// RTP Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum RtpError {
    #[error("Packet too short: {0} bytes (minimum 12)")]
    PacketTooShort(usize),

    #[error("Unsupported RTP version: {0}")]
    UnsupportedVersion(u8),

    #[error("CSRC list exceeds packet bounds")]
    InvalidCsrcList,

    #[error("Extension header exceeds packet bounds")]
    InvalidExtension,

    #[error("Invalid padding: {0} bytes")]
    InvalidPadding(usize),

    #[error("Sequence number discontinuity: expected {expected}, got {actual}")]
    SequenceDiscontinuity { expected: u16, actual: u16 },

    #[error("Payload type mismatch: expected {expected}, got {actual}")]
    PayloadTypeMismatch { expected: u8, actual: u8 },
}

// ============================================================================
// SDP Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum SdpError {
    #[error("No video track found in SDP")]
    NoVideoTrack,

    #[error("No payload type found")]
    NoPayloadType,

    #[error("No control attribute found")]
    NoControlAttribute,

    #[error("Invalid media line: {0}")]
    InvalidMediaLine(String),

    #[error("Invalid rtpmap: {0}")]
    InvalidRtpmap(String),

    #[error("Unsupported codec: {0}")]
    UnsupportedCodec(String),

    #[error("Missing required attribute: {0}")]
    MissingAttribute(&'static str),
}

// ============================================================================
// H264 Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum H264Error {
    #[error("Unsupported NAL type: {0}")]
    UnsupportedNalType(u8),

    #[error("FU-A continuation without start")]
    FuAWithoutStart,

    #[error("Payload too short: {0} bytes")]
    PayloadTooShort(usize),

    #[error("STAP-A NAL length exceeds bounds: {nal_len} > {remaining}")]
    StapAOutOfBounds { nal_len: usize, remaining: usize },

    #[error("STAP-A contains zero-length NAL")]
    StapAZeroLength,

    #[error("STAP-A has trailing bytes: {0}")]
    StapATrailingBytes(usize),

    #[error("Invalid Annex-B NAL: missing start code")]
    InvalidAnnexB,

    #[error("Missing parameter set: {0}")]
    MissingParameterSet(&'static str),
}

// ============================================================================
// ONVIF Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum OnvifError {
    #[error("SOAP fault: {code} - {message}")]
    SoapFault { code: String, message: String },

    #[error("HTTP {status}: {body}")]
    HttpError { status: u16, body: String },

    #[error("XML parsing error: {0}")]
    XmlParse(String),

    #[error("Missing required XML element: {0}")]
    MissingElement(&'static str),

    #[error("Authentication failed")]
    AuthenticationFailed,

    #[error("Subscription address not found in response")]
    NoSubscriptionAddress,

    #[error("Invalid WS-Addressing header: {0}")]
    InvalidWsAddressing(String),

    #[error("Connection refused to subscription endpoint: {0}")]
    SubscriptionEndpointUnreachable(String),

    #[error("Exceeded retry limit: {0}")]
    RetryLimitExceeded(String),
}

// ============================================================================
// Utility conversions
// ============================================================================

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Internal(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Internal(s.to_string())
    }
}

// Allow anyhow errors to be converted (for gradual migration)
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_maps_to_internal_error() {
        let err = Error::from("plain message");
        match err {
            Error::Internal(msg) => assert_eq!(msg, "plain message"),
            _ => panic!("expected Error::Internal"),
        }
    }

    #[test]
    fn from_string_maps_to_internal_error() {
        let err = Error::from("owned message".to_string());
        match err {
            Error::Internal(msg) => assert_eq!(msg, "owned message"),
            _ => panic!("expected Error::Internal"),
        }
    }

    #[test]
    fn from_anyhow_maps_to_internal_error_with_message() {
        let err = Error::from(anyhow::anyhow!("anyhow message"));
        match err {
            Error::Internal(msg) => assert!(msg.contains("anyhow message")),
            _ => panic!("expected Error::Internal"),
        }
    }
}
