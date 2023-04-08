use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::Writable;

pub struct WritableFile {
    f: File,
}

impl WritableFile {
    pub async fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self { f })
    }
}

#[async_trait]
impl Writable for WritableFile {
    async fn append(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.f.write(data).await
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.f.flush().await
    }

    async fn sync(&self) -> std::io::Result<()> {
        self.f.sync_all().await
    }
}
