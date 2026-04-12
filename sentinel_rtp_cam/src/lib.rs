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

// Pi <-> server protocol + uplink
pub mod agent_uplink;
pub mod proto;
pub mod server_pipeline;

// RTSP streaming
pub mod rtsp;

// ONVIF motion detection
pub mod onvif;

// Event system
pub mod event;

// Server integration
pub mod server;
pub mod live;

// Utilities
pub mod utils;

// Forward agent helpers
pub mod forward_agent;
pub mod motion_event_latch;

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
pub use onvif::{
    load_onvif_probe_cameras_from_env, run_onvif_capability_probe, run_onvif_motion_poller,
    OnvifProbeCameraConfig, OnvifProbeReport,
};

// Events
pub use event::{Event, EventBus, MotionEvent, MotionMetadata, MotionState, MotionStateBus};

// Server
pub use server::{
    retry_forever, run_agent_heartbeat_poster, CameraHeartbeatTarget, OnvifProbeManager,
    OnvifProbeSummary,
};

// Utils
pub use utils::{run_disk_cleanup, DiskCleanupConfig, Error, Result};
