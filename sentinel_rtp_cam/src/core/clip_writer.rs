use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

pub struct ClipWriter {
    part_path: PathBuf,
    final_path: PathBuf,
    writer: BufWriter<File>,
}

impl ClipWriter {
    pub async fn create(part_path: PathBuf, final_path: PathBuf) -> Result<Self> {
        let file = File::create(&part_path)
            .await
            .with_context(|| format!("create clip part file: {}", part_path.display()))?;
        let writer = BufWriter::with_capacity(256 * 1024, file);
        Ok(Self {
            part_path,
            final_path,
            writer,
        })
    }

    pub async fn write_nal(&mut self, nal: &[u8]) -> Result<()> {
        self.writer.write_all(nal).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<PathBuf> {
        self.writer.flush().await?;
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
}
