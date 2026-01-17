//! # Sentinel Video Receiver
//!
//! A Rust library for receiving and processing RTSP/RTP video streams with ONVIF motion detection.
//!
//! ## Architecture
//!
//! This library provides:
//! - **RTSP Client**: RFC 2326 compliant RTSP client with TCP interleaved and UDP transport
//! - **RTP Depacketization**: Zero-copy RTP packet parsing and H.264 depacketization
//! - **ONVIF Integration**: WS-Security authenticated SOAP client for motion detection events
//! - **Event-driven Recording**: Motion-triggered video clip recording with pre/post-roll
//!
//! ## Example
//!
//! ```no_run
//! use sentinel_rtp_cam::rtsp_receiver_udp::{run_udp_receiver, UdpReceiverConfig};
//! use sentinel_rtp_cam::event_bus::EventBus;
//! use tokio::sync::mpsc;
//! use tokio_util::sync::CancellationToken;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let cfg = UdpReceiverConfig::from_env();
//!     let (tx, mut rx) = mpsc::channel(1024);
//!     let cancel = CancellationToken::new();
//!     
//!     // Receive video NALs
//!     tokio::spawn(async move {
//!         run_udp_receiver(cfg, tx, cancel).await
//!     });
//!     
//!     // Process NALs
//!     while let Some(nal) = rx.recv().await {
//!         // Handle video data
//!     }
//!     Ok(())
//! }
//! ```

// Core error types (using thiserror for type-safe error handling)
pub mod error;

// Protocol implementation
pub mod rtp;
pub mod rtsp;
pub mod sdp;
pub mod interleaved;

// Video codec support
pub mod h264_depacketize;
pub mod video;

// ONVIF integration
pub mod onvif_motion;
pub mod retry_helper;

// Event system
pub mod event_bus;
pub mod eventlogger;

// High-level receivers
pub mod rtsp_receiver_tcp;
pub mod rtsp_receiver_udp;

// Recording
pub mod clip_recorder;

// Re-export commonly used types
pub use error::{Error, Result};
pub use event_bus::{Event, EventBus, MotionEvent};
pub use video::VideoNal;