use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use sentinel_rtp_cam::{
    run_clip_meta_poster, run_disk_cleanup, run_heartbeat_poster, run_motion_event_poster,
    run_onvif_motion_poller, run_sse_config_listener, run_udp_receiver, AgentConfig, ClipRecorder,
    ClipRecorderConfig, DiskCleanupConfig, Event, EventBus, MotionStateBus, UdpReceiverConfig,
    VideoNal,
};

fn spawn_logger(mut rx: mpsc::Receiver<Event>) {
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let Event::Motion(m) = ev;
            if m.active {
                info!(
                    camera_id = %m.camera_id,
                    event_id = %m.event_id,
                    rule = %m.rule,
                    timestamp = %m.ts,
                    "Motion detected"
                );
            } else {
                info!(
                    camera_id = %m.camera_id,
                    event_id = %m.event_id,
                    rule = %m.rule,
                    timestamp = %m.ts,
                    "Motion ended"
                );
            }
        }
    });
}

fn arg_flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
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

    // Load agent configuration
    let agent_config = AgentConfig::from_env();
    info!(
        camera_id = %agent_config.camera_id,
        motion_enabled = agent_config.motion.enabled,
        server_enabled = agent_config.server.enabled,
        "Agent configuration loaded"
    );

    let cancel = CancellationToken::new();

    // Event bus for motion events (logging)
    let bus = EventBus::new(128);

    // Motion state bus (recorder control)
    let (motion_state, _motion_rx) = MotionStateBus::new();

    // Logger subscriber
    spawn_logger(bus.subscribe().await);

    // NAL channel from receiver to recorder
    let (nal_tx, video_nal_rx) = mpsc::channel::<VideoNal>(2048);
    let (h264_tx, h264_rx) = mpsc::channel::<Vec<u8>>(2048);

    // ClipMeta channel from recorder to server
    let (clip_meta_tx, clip_meta_rx) = mpsc::channel(128);

    // Convert VideoNal -> Vec<u8> for recorder
    tokio::spawn(async move {
        let mut rx = video_nal_rx;
        while let Some(vnal) = rx.recv().await {
            let _ = h264_tx.try_send(vnal.data);
        }
    });

    // Recorder task
    {
        let motion_rx = motion_state.subscribe();
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
            min_clip_duration: std::time::Duration::from_secs(
                std::env::var("CLIP_MIN_DURATION_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5),
            ),
            flush_interval: std::time::Duration::from_secs(
                std::env::var("CLIP_FLUSH_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1),
            ),
            stale_part_max_age: std::time::Duration::from_secs(
                std::env::var("CLIP_STALE_PART_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(24 * 60 * 60),
            ),
            max_files: std::env::var("CLIP_MAX_FILES")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_age_secs: std::env::var("CLIP_MAX_AGE_SECS")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_total_bytes: std::env::var("CLIP_MAX_TOTAL_BYTES")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_clip_secs: std::env::var("CLIP_MAX_SECS")
                .ok()
                .and_then(|v| v.parse().ok()),
        };

        tokio::spawn(async move {
            let rec = ClipRecorder::new(rec_cfg).with_clip_meta_tx(clip_meta_tx);
            if let Err(e) = rec.run(motion_rx, h264_rx).await {
                error!(error = %e, "Clip recorder error");
            }
        });
    }

    // ONVIF poller task (DO NOT bubble errors to main)
    {
        let bus2 = bus.clone();
        let motion_state2 = motion_state.clone();
        tokio::spawn(async move {
            if let Err(e) = run_onvif_motion_poller(bus2, motion_state2).await {
                error!(error = %e, "ONVIF motion poller error");
            }
            warn!("ONVIF task ended");
        });
    }

    // Server integration tasks (optional)
    if agent_config.server.enabled {
        info!("Starting server integration tasks");
        let server_cfg = agent_config.server.clone();

        // SSE config listener
        let camera_id_clone = agent_config.camera_id.clone();
        let server_cfg_clone = server_cfg.clone();
        tokio::spawn(async move {
            match run_sse_config_listener(server_cfg_clone, camera_id_clone).await {
                Ok(mut config_rx) => {
                    info!("SSE config listener started, waiting for updates");
                    loop {
                        config_rx.changed().await.ok();
                        let new_config = config_rx.borrow().clone();
                        info!(
                            config = %serde_json::to_string(&new_config).unwrap_or_default(),
                            "Received config update from server"
                        );
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to start SSE config listener");
                }
            }
        });

        // Motion event poster
        let motion_rx = bus.subscribe().await;
        let server_cfg_clone = server_cfg.clone();
        tokio::spawn(async move {
            // Convert Event to MotionEvent
            let (motion_event_tx, motion_event_rx) = mpsc::channel(128);
            tokio::spawn(async move {
                let mut rx = motion_rx;
                while let Some(ev) = rx.recv().await {
                    let sentinel_rtp_cam::Event::Motion(m) = ev;
                    let _ = motion_event_tx.send(m).await;
                }
            });

            if let Err(e) = run_motion_event_poster(motion_event_rx, server_cfg_clone).await {
                error!(error = %e, "Motion event poster error");
            }
        });

        // Clip metadata poster
        let server_cfg_clone = server_cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = run_clip_meta_poster(clip_meta_rx, server_cfg_clone).await {
                error!(error = %e, "Clip metadata poster error");
            }
        });

        // Heartbeat poster
        let camera_id = agent_config.camera_id.clone();
        let server_cfg_clone = server_cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = run_heartbeat_poster(server_cfg_clone, camera_id).await {
                error!(error = %e, "Heartbeat poster error");
            }
        });
    }

    // Disk cleanup task
    let cleanup_cfg = &agent_config.cleanup;
    info!(
        interval_secs = cleanup_cfg.interval_secs,
        min_free_mb = cleanup_cfg.min_free_bytes / 1_000_000,
        "Starting disk cleanup task"
    );

    let disk_cfg = DiskCleanupConfig {
        clips_dir: std::env::var("OUTPUT_DIR")
            .or_else(|_| std::env::var("CLIP_DIR"))
            .unwrap_or_else(|_| "clips".to_string())
            .into(),
        min_free_bytes: cleanup_cfg.min_free_bytes,
        check_interval: std::time::Duration::from_secs(cleanup_cfg.interval_secs),
    };

    tokio::spawn(async move {
        if let Err(e) = run_disk_cleanup(disk_cfg).await {
            error!(error = %e, "Disk cleanup task error");
        }
    });

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

                match run_udp_receiver(recv_cfg.clone(), nal_tx2.clone(), recv_cancel.clone()).await
                {
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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Ok(())
}
