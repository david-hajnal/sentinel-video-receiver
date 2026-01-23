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
//! use sentinel_rtp_cam::rtsp::{run_udp_receiver, UdpReceiverConfig};
//! use sentinel_rtp_cam::event::EventBus;
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

// ============================================================================
// Module Organization
// ============================================================================

// Configuration
pub mod config;

// Core protocol & codec implementation
pub mod core;

// RTSP streaming
pub mod rtsp;

// ONVIF motion detection
pub mod onvif;

// Event system
pub mod event;

// Server integration
pub mod server;

// Utilities
pub mod utils;

// ============================================================================
// Public API Re-exports
// ============================================================================

// Configuration
pub use config::AgentConfig;

// Core types
pub use core::{ClipMeta, ClipRecorder, ClipRecorderConfig, VideoNal};

// RTSP
pub use rtsp::{run_udp_receiver, UdpReceiverConfig};

// ONVIF
pub use onvif::run_onvif_motion_poller;

// Events
pub use event::{Event, EventBus, MotionEvent, MotionMetadata, MotionState, MotionStateBus};

// Server
pub use server::{
    retry_forever, run_clip_meta_poster, run_heartbeat_poster, run_motion_event_poster,
    run_sse_config_listener,
};

// Utils
pub use utils::{run_disk_cleanup, DiskCleanupConfig, Error, Result};
