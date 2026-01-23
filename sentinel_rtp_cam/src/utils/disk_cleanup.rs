use anyhow::Result;
use std::path::Path;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Configuration for disk space monitoring and cleanup
#[derive(Clone, Debug)]
pub struct DiskCleanupConfig {
    /// Directory to monitor and clean
    pub clips_dir: std::path::PathBuf,
    /// Minimum free bytes to maintain (cleanup triggers when below this)
    pub min_free_bytes: u64,
    /// How often to check disk space
    pub check_interval: Duration,
}

impl Default for DiskCleanupConfig {
    fn default() -> Self {
        Self {
            clips_dir: std::path::PathBuf::from("clips"),
            min_free_bytes: 1_000_000_000, // 1 GB default
            check_interval: Duration::from_secs(60),
        }
    }
}

/// Get available disk space in bytes for the filesystem containing the given path
/// Uses libc::statvfs on Unix systems
#[cfg(unix)]
pub fn get_available_space(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path_cstr = CString::new(path.as_os_str().as_bytes())?;

    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(path_cstr.as_ptr(), &mut stat) != 0 {
            return Err(anyhow::anyhow!(
                "statvfs failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        // f_bavail = available blocks for unprivileged users
        // f_frsize = fragment size (or f_bsize if frsize == 0)
        let block_size = if stat.f_frsize > 0 {
            stat.f_frsize as u64
        } else {
            stat.f_bsize as u64
        };

        Ok((stat.f_bavail as u64) * block_size)
    }
}

#[cfg(not(unix))]
pub fn get_available_space(_path: &Path) -> Result<u64> {
    // Placeholder for non-Unix systems
    Err(anyhow::anyhow!("statvfs not available on this platform"))
}

/// Delete oldest .mp4 files until available space is above threshold
async fn cleanup_until_space_available(clips_dir: &Path, min_free_bytes: u64) -> Result<usize> {
    let mut deleted = 0;

    loop {
        let available = get_available_space(clips_dir)?;
        if available >= min_free_bytes {
            break;
        }

        // Find oldest .mp4 file
        let mut entries = tokio::fs::read_dir(clips_dir).await?;
        let mut oldest: Option<(std::path::PathBuf, std::time::SystemTime)> = None;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("mp4") {
                continue;
            }

            if let Ok(meta) = entry.metadata().await {
                if let Ok(modified) = meta.modified() {
                    match &oldest {
                        None => oldest = Some((path, modified)),
                        Some((_, old_time)) if modified < *old_time => {
                            oldest = Some((path, modified));
                        }
                        _ => {}
                    }
                }
            }
        }

        // Delete the oldest file
        if let Some((oldest_path, _)) = oldest {
            match tokio::fs::remove_file(&oldest_path).await {
                Ok(_) => {
                    info!(
                        file = ?oldest_path,
                        available_mb = available / 1_000_000,
                        threshold_mb = min_free_bytes / 1_000_000,
                        "Deleted old clip to free disk space"
                    );
                    deleted += 1;
                }
                Err(e) => {
                    error!(
                        error = %e,
                        file = ?oldest_path,
                        "Failed to delete old clip"
                    );
                    break;
                }
            }
        } else {
            // No more .mp4 files to delete
            warn!(
                available_mb = available / 1_000_000,
                threshold_mb = min_free_bytes / 1_000_000,
                "Disk space low but no more clips to delete"
            );
            break;
        }
    }

    Ok(deleted)
}

/// Run periodic disk space monitoring and cleanup
/// This task runs forever, checking disk space at configured intervals
/// and deleting oldest clips when free space falls below threshold
pub async fn run_disk_cleanup(cfg: DiskCleanupConfig) -> Result<()> {
    info!(
        clips_dir = ?cfg.clips_dir,
        min_free_mb = cfg.min_free_bytes / 1_000_000,
        check_interval_secs = cfg.check_interval.as_secs(),
        "Starting disk cleanup task"
    );

    // Ensure clips directory exists
    tokio::fs::create_dir_all(&cfg.clips_dir).await?;

    loop {
        sleep(cfg.check_interval).await;

        match get_available_space(&cfg.clips_dir) {
            Ok(available) => {
                debug!(
                    available_mb = available / 1_000_000,
                    threshold_mb = cfg.min_free_bytes / 1_000_000,
                    "Disk space check"
                );

                if available < cfg.min_free_bytes {
                    match cleanup_until_space_available(&cfg.clips_dir, cfg.min_free_bytes).await {
                        Ok(deleted) if deleted > 0 => {
                            info!(deleted = deleted, "Disk cleanup completed");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!(error = %e, "Disk cleanup failed");
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to check disk space");
            }
        }
    }
}
