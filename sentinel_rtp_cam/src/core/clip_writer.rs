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

#[cfg(test)]
mod tests {
    use super::ClipWriter;
    use std::path::PathBuf;
    use tokio::fs;
    use ulid::Ulid;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sentinel_clip_writer_{name}_{}", Ulid::new()))
    }

    fn test_paths(name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let dir = test_dir(name);
        let part_path = dir.join("clip.h264.part");
        let final_path = dir.join("clip.h264");
        (dir, part_path, final_path)
    }

    #[tokio::test]
    async fn create_initializes_part_file_and_zero_byte_accounting() {
        let (dir, part_path, final_path) = test_paths("create");
        fs::create_dir_all(&dir).await.unwrap();

        let writer = ClipWriter::create(part_path.clone(), final_path, 64 * 1024)
            .await
            .unwrap();

        assert_eq!(writer.part_path(), &part_path);
        assert_eq!(writer.total_bytes(), 0);
        assert!(fs::metadata(&part_path).await.unwrap().is_file());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn write_nal_updates_total_bytes_before_flush_and_preserves_payload() {
        let (dir, part_path, final_path) = test_paths("write");
        fs::create_dir_all(&dir).await.unwrap();

        let nal = vec![0, 0, 0, 1, 0x65, 0x88, 0x99];
        let mut writer = ClipWriter::create(part_path.clone(), final_path, 64 * 1024)
            .await
            .unwrap();
        writer.write_nal(&nal).await.unwrap();

        assert_eq!(writer.total_bytes(), nal.len() as u64);

        writer.flush().await.unwrap();
        assert_eq!(fs::read(&part_path).await.unwrap(), nal);
        assert_eq!(writer.total_bytes(), nal.len() as u64);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn write_nal_threshold_rollover_keeps_data_buffered_until_flush() {
        let (dir, part_path, final_path) = test_paths("threshold");
        fs::create_dir_all(&dir).await.unwrap();

        let nal = vec![0xAB; 20_000];
        let mut writer = ClipWriter::create(part_path.clone(), final_path.clone(), 1)
            .await
            .unwrap();
        writer.write_nal(&nal).await.unwrap();

        assert_eq!(writer.total_bytes(), nal.len() as u64);
        assert!(fs::metadata(&part_path).await.unwrap().is_file());
        assert!(fs::metadata(&final_path).await.is_err());

        writer.flush().await.unwrap();
        assert_eq!(fs::read(&part_path).await.unwrap(), nal);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn finalize_renames_part_file_and_preserves_contents() {
        let (dir, part_path, final_path) = test_paths("finalize");
        fs::create_dir_all(&dir).await.unwrap();

        let nal = vec![0, 0, 0, 1, 0x41, 0x9A];
        let mut writer = ClipWriter::create(part_path.clone(), final_path.clone(), 64 * 1024)
            .await
            .unwrap();
        writer.write_nal(&nal).await.unwrap();

        let got_final_path = writer.finalize().await.unwrap();

        assert_eq!(got_final_path, final_path);
        assert!(fs::metadata(&part_path).await.is_err());
        assert_eq!(fs::read(&final_path).await.unwrap(), nal);

        fs::remove_dir_all(&dir).await.unwrap();
    }
}
