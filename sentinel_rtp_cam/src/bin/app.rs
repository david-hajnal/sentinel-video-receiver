use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, error};

use sentinel_rtp_cam::clip_recorder::{ClipRecorder, ClipRecorderConfig};
use sentinel_rtp_cam::event_bus::{Event, EventBus};
use sentinel_rtp_cam::onvif_motion::run_onvif_motion_poller;
use sentinel_rtp_cam::rtsp_receiver_udp::{run_udp_receiver, UdpReceiverConfig};
use sentinel_rtp_cam::video::VideoNal;

fn spawn_logger(mut rx: mpsc::Receiver<Event>) {
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let Event::Motion(m) = ev;
            if m.active {
                info!(rule = %m.rule, timestamp = %m.ts, "Motion detected");
            } else {
                info!(rule = %m.rule, timestamp = %m.ts, "Motion ended");
            }
        }
    });
}

fn arg_flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    // .env support
    if dotenvy::dotenv().is_err() {
        dotenvy::from_filename("../.env").ok();
    }

    let onvif_only = arg_flag("--onvif-only");

    let cancel = CancellationToken::new();

    // Event bus for motion events
    let bus = EventBus::new(128);

    // Logger subscriber
    spawn_logger(bus.subscribe().await);

    // NAL channel from receiver to recorder
    let (nal_tx, video_nal_rx) = mpsc::channel::<VideoNal>(2048);
    let (h264_tx, h264_rx) = mpsc::channel::<Vec<u8>>(2048);

    // Convert VideoNal -> Vec<u8> for recorder
    tokio::spawn(async move {
        let mut rx = video_nal_rx;
        while let Some(vnal) = rx.recv().await {
            let _ = h264_tx.try_send(vnal.data);
        }
    });

    // Recorder task
    {
        let motion_rx = bus.subscribe().await;
        let rec_cfg = ClipRecorderConfig {
            output_dir: std::env::var("OUTPUT_DIR")
                .or_else(|_| std::env::var("CLIP_DIR"))
                .unwrap_or_else(|_| "clips".to_string())
                .into(),
            post_roll: std::time::Duration::from_secs(
                std::env::var("POST_ROLL_SECS")
                    .or_else(|_| std::env::var("CLIP_POST_ROLL_SECS"))
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3),
            ),
            assumed_fps: std::env::var("ASSUMED_FPS")
                .or_else(|_| std::env::var("CLIP_FPS"))
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(25),
            stream_copy: std::env::var("STREAM_COPY")
                .or_else(|_| std::env::var("CLIP_STREAM_COPY"))
                .ok()
                .and_then(|v| {
                    // support 1/0 and true/false
                    match v.as_str() {
                        "1" => Some(true),
                        "0" => Some(false),
                        _ => v.parse().ok(),
                    }
                })
                .unwrap_or(true),
        };

        tokio::spawn(async move {
            let rec = ClipRecorder::new(rec_cfg);
            if let Err(e) = rec.run(motion_rx, h264_rx).await {
                error!(error = %e, "Clip recorder error");
            }
        });
    }

    // ONVIF poller task (DO NOT bubble errors to main)
    {
        let bus2 = bus.clone();
        tokio::spawn(async move {
            if let Err(e) = run_onvif_motion_poller(bus2).await {
                error!(error = %e, "ONVIF motion poller error");
            }
            warn!("ONVIF task ended");
        });
    }

    // RTSP receiver task (optional, and should NOT kill the process)
    if !onvif_only {
        let recv_cfg = UdpReceiverConfig::from_env();
        let recv_cancel = cancel.clone();
        let nal_tx2 = nal_tx.clone();

        tokio::spawn(async move {
            // Keep trying unless cancelled. This prevents a transient RTSP failure
            // from terminating the whole program.
            let mut attempt: u64 = 0;
            loop {
                if recv_cancel.is_cancelled() {
                    break;
                }
                attempt += 1;

                // Log connection attempt with context
                info!(
                    attempt = attempt,
                    host = %recv_cfg.host,
                    port = recv_cfg.port,
                    url = %recv_cfg.rtsp_url(),
                    "Starting RTSP receiver"
                );

                match run_udp_receiver(recv_cfg.clone(), nal_tx2.clone(), recv_cancel.clone()).await {
                    Ok(_) => {
                        warn!("RTSP receiver ended normally");
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "RTSP receiver error, retrying in 2s");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                }
            }
        });
    } else {
        info!("RTSP receiver disabled (--onvif-only flag set)");
    }

    // Keep the process alive until Ctrl-C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, gracefully terminating");
            cancel.cancel();
        }
    }

    // Give ffmpeg time to finalize mp4
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    Ok(())
}
