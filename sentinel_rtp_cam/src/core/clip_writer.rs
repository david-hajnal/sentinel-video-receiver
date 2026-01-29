use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

pub struct ClipWriter {
    part_path: PathBuf,
    final_path: PathBuf,
    writer: BufWriter<File>,
    pending: Vec<u8>,
    max_pending_bytes: usize,
    written_bytes: u64,
}

impl ClipWriter {
    pub async fn create(
        part_path: PathBuf,
        final_path: PathBuf,
        max_pending_bytes: usize,
    ) -> Result<Self> {
        let file = File::create(&part_path)
            .await
            .with_context(|| format!("create clip part file: {}", part_path.display()))?;
        let writer = BufWriter::with_capacity(256 * 1024, file);
        Ok(Self {
            part_path,
            final_path,
            writer,
            pending: Vec::with_capacity(max_pending_bytes.min(512 * 1024)),
            max_pending_bytes: max_pending_bytes.max(16 * 1024),
            written_bytes: 0,
        })
    }

    pub async fn write_nal(&mut self, nal: &[u8]) -> Result<()> {
        self.pending.extend_from_slice(nal);
        if self.pending.len() >= self.max_pending_bytes {
            self.flush_pending(false).await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.flush_pending(true).await?;
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<PathBuf> {
        self.flush_pending(true).await?;
        drop(self.writer);
        tokio::fs::rename(&self.part_path, &self.final_path)
            .await
            .with_context(|| {
                format!(
                    "rename {} -> {}",
                    self.part_path.display(),
                    self.final_path.display()
                )
            })?;
        Ok(self.final_path)
    }

    pub fn part_path(&self) -> &PathBuf {
        &self.part_path
    }

    pub fn total_bytes(&self) -> u64 {
        self.written_bytes + (self.pending.len() as u64)
    }

    async fn flush_pending(&mut self, do_flush: bool) -> Result<()> {
        if !self.pending.is_empty() {
            self.writer.write_all(&self.pending).await?;
            self.written_bytes += self.pending.len() as u64;
            self.pending.clear();
        }
        if do_flush {
            self.writer.flush().await?;
        }
        Ok(())
    }
}
