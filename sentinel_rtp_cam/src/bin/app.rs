use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use std::path::PathBuf;

use sentinel_rtp_cam::{
    run_clip_meta_poster, run_disk_cleanup, run_heartbeat_poster, run_motion_event_poster,
    run_onvif_motion_poller, run_sse_config_listener, run_udp_receiver, AgentConfig, ClipRecorder,
    ClipRecorderConfig, DiskCleanupConfig, Event, EventBus, MotionStateBus, UdpReceiverConfig,
    VideoNal,
};

const DEFAULT_SERVER_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/server.json";
const DEFAULT_CAMERA_CONFIG_PATH: &str = "/etc/sentinel_rtp_cam/camera.json";

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

fn runtime_var(name: &str) -> Option<String> {
    AgentConfig::runtime_var(name).filter(|v| !v.trim().is_empty())
}

fn runtime_bool(name: &str, default: bool) -> bool {
    AgentConfig::runtime_bool(name, default)
}

fn runtime_u64(name: &str, default: u64) -> u64 {
    runtime_var(name)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn runtime_opt_u64(name: &str) -> Option<u64> {
    runtime_var(name).and_then(|v| v.parse::<u64>().ok())
}

fn runtime_opt_usize(name: &str) -> Option<usize> {
    runtime_var(name).and_then(|v| v.parse::<usize>().ok())
}

fn paths_resolve_to_same_file(a: &std::path::Path, b: &std::path::Path) -> bool {
    if a == b {
        return true;
    }
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a_canon), Ok(b_canon)) => a_canon == b_canon,
        _ => false,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let server_source_path = PathBuf::from(DEFAULT_SERVER_CONFIG_PATH);
    let camera_config_path = PathBuf::from(DEFAULT_CAMERA_CONFIG_PATH);

    // Load JSON config and install runtime overrides before reading runtime settings.
    let mut server_error: Option<String> = None;
    let mut camera_error: Option<String> = None;
    let server_value = match AgentConfig::load_server_json(&server_source_path) {
        Ok(value) => value,
        Err(e) => {
            server_error = Some(e.to_string());
            serde_json::Value::Object(Default::default())
        }
    };
    let camera_value = match AgentConfig::load_camera_json(&camera_config_path) {
        Ok(value) => value,
        Err(e) => {
            camera_error = Some(e.to_string());
            serde_json::Value::Object(Default::default())
        }
    };
    let config_value = AgentConfig::merge_server_camera_configs(&camera_value, &server_value);
    AgentConfig::apply_json_env_overrides(&config_value);

    // Initialize tracing subscriber
    let log_filter = runtime_var("RUST_LOG").unwrap_or_else(|| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    if let Some(error) = server_error {
        warn!(
            error = %error,
            path = %server_source_path.display(),
            "Failed to load server config JSON, using defaults"
        );
    }
    if let Some(error) = camera_error {
        warn!(
            error = %error,
            path = %camera_config_path.display(),
            "Failed to load camera config JSON, using defaults"
        );
    }

    let onvif_only = arg_flag("--onvif-only");
    let local_clip_enabled = runtime_bool("LOCAL_CLIP_ENABLED", true);
    let rtsp_receiver_enabled =
        !onvif_only && local_clip_enabled && runtime_bool("RTSP_RECEIVER_ENABLED", true);

    // Build agent configuration from JSON
    let agent_config = AgentConfig::from_json_value(config_value.clone());
    info!(
        server_config_path = %server_source_path.display(),
        camera_config_path = %camera_config_path.display(),
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

    // Optional local clip pipeline
    let mut nal_tx_opt = None;
    let mut clip_meta_rx_opt = None;

    if local_clip_enabled {
        // NAL channel from receiver to recorder
        let (nal_tx, video_nal_rx) = mpsc::channel::<VideoNal>(2048);
        let (h264_tx, h264_rx) = mpsc::channel::<Vec<u8>>(2048);

        // ClipMeta channel from recorder to server
        let (clip_meta_tx, clip_meta_rx) = mpsc::channel(128);
        nal_tx_opt = Some(nal_tx);
        clip_meta_rx_opt = Some(clip_meta_rx);

        // Convert VideoNal -> Vec<u8> for recorder
        tokio::spawn(async move {
            let mut rx = video_nal_rx;
            while let Some(vnal) = rx.recv().await {
                let _ = h264_tx.try_send(vnal.data);
            }
        });

        // Recorder task
        let motion_rx = motion_state.subscribe();
        let rec_cfg = ClipRecorderConfig {
            output_dir: runtime_var("OUTPUT_DIR")
                .or_else(|| runtime_var("CLIP_DIR"))
                .unwrap_or_else(|| "clips".to_string())
                .into(),
            post_roll: std::time::Duration::from_secs(
                runtime_var("POST_ROLL_SECS")
                    .or_else(|| runtime_var("CLIP_POST_ROLL_SECS"))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3),
            ),
            min_clip_duration: std::time::Duration::from_secs(runtime_u64("CLIP_MIN_DURATION_SECS", 5)),
            flush_interval: std::time::Duration::from_secs(runtime_u64("CLIP_FLUSH_SECS", 1)),
            stale_part_max_age: std::time::Duration::from_secs(runtime_u64(
                "CLIP_STALE_PART_SECS",
                24 * 60 * 60,
            )),
            write_batch_bytes: runtime_opt_usize("CLIP_WRITE_BATCH_BYTES").unwrap_or(256 * 1024),
            max_files: runtime_opt_usize("CLIP_MAX_FILES"),
            max_age_secs: runtime_opt_u64("CLIP_MAX_AGE_SECS"),
            max_total_bytes: runtime_opt_u64("CLIP_MAX_TOTAL_BYTES"),
            max_clip_bytes: runtime_opt_u64("CLIP_MAX_BYTES"),
            max_clip_secs: runtime_opt_u64("CLIP_MAX_SECS"),
        };

        tokio::spawn(async move {
            let rec = ClipRecorder::new(rec_cfg).with_clip_meta_tx(clip_meta_tx);
            if let Err(e) = rec.run(motion_rx, h264_rx).await {
                error!(error = %e, "Clip recorder error");
            }
        });
    } else {
        info!("Local clip recording disabled (LOCAL_CLIP_ENABLED=0)");
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
        let stream_id = runtime_var("CAM1_STREAM_ID").and_then(|v| v.parse::<u32>().ok());
        let server_cfg_clone = server_cfg.clone();
        let server_config_path = server_source_path.clone();
        let camera_config_path = camera_config_path.clone();
        let uses_single_config_file =
            paths_resolve_to_same_file(&server_config_path, &camera_config_path);
        tokio::spawn(async move {
            match run_sse_config_listener(server_cfg_clone, camera_id_clone, stream_id).await {
                Ok(mut config_rx) => {
                    info!("SSE config listener started, waiting for updates");
                    loop {
                        config_rx.changed().await.ok();
                        let new_config = config_rx.borrow().clone();
                        info!(
                            config = %serde_json::to_string(&new_config).unwrap_or_default(),
                            "Received config update from server"
                        );
                        if uses_single_config_file {
                            if let Err(e) =
                                AgentConfig::merge_json_file(&camera_config_path, &new_config)
                            {
                                error!(
                                    error = %e,
                                    path = %camera_config_path.display(),
                                    "Failed to persist config update"
                                );
                            } else {
                                info!(
                                    path = %camera_config_path.display(),
                                    "Persisted config update to JSON"
                                );
                            }
                            continue;
                        }

                        if let Some(server_update) = new_config.get("server") {
                            let server_payload = serde_json::json!({ "server": server_update });
                            if let Err(e) = AgentConfig::merge_json_file_with_default(
                                &server_config_path,
                                &server_payload,
                                AgentConfig::default_server_json(),
                            ) {
                                error!(
                                    error = %e,
                                    path = %server_config_path.display(),
                                    "Failed to persist server config update"
                                );
                            } else {
                                info!(
                                    path = %server_config_path.display(),
                                    "Persisted server config update to JSON"
                                );
                            }
                        }

                        let mut camera_update = new_config.clone();
                        if let Some(obj) = camera_update.as_object_mut() {
                            obj.remove("server");
                        }
                        if let Err(e) = AgentConfig::merge_json_file_with_default(
                            &camera_config_path,
                            &camera_update,
                            AgentConfig::default_camera_json(),
                        ) {
                            error!(
                                error = %e,
                                path = %camera_config_path.display(),
                                "Failed to persist camera config update"
                            );
                        } else {
                            info!(
                                path = %camera_config_path.display(),
                                "Persisted camera config update to JSON"
                            );
                        }
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

        // Clip metadata poster (local clips only)
        if let Some(clip_meta_rx) = clip_meta_rx_opt {
            let server_cfg_clone = server_cfg.clone();
            tokio::spawn(async move {
                if let Err(e) = run_clip_meta_poster(clip_meta_rx, server_cfg_clone).await {
                    error!(error = %e, "Clip metadata poster error");
                }
            });
        } else {
            info!("Clip metadata poster disabled (local clips disabled)");
        }

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
        clips_dir: runtime_var("OUTPUT_DIR")
            .or_else(|| runtime_var("CLIP_DIR"))
            .unwrap_or_else(|| "clips".to_string())
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
    if rtsp_receiver_enabled {
        if let Some(nal_tx) = nal_tx_opt {
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

                    match run_udp_receiver(recv_cfg.clone(), nal_tx2.clone(), recv_cancel.clone())
                        .await
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
            warn!("RTSP receiver enabled but local clips are disabled");
        }
    } else if onvif_only {
        info!("RTSP receiver disabled (--onvif-only flag set)");
    } else if !local_clip_enabled {
        info!("RTSP receiver disabled (local clips disabled)");
    } else {
        info!("RTSP receiver disabled (RTSP_RECEIVER_ENABLED=0)");
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
