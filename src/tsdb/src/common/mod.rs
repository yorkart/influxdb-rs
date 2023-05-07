use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite, AsyncWriteExt};

#[derive(Default)]
pub struct Position {
    pub offset: u64,
    pub size: u64,
}

impl Position {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    pub fn max_offset(&self) -> u64 {
        self.offset + self.size
    }

    pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
        w.write_u64(self.offset).await?;
        w.write_u64(self.size).await?;
        Ok(())
    }

    pub async fn read_from<R: AsyncRead + AsyncSeek + Send + Unpin>(
        mut r: R,
    ) -> anyhow::Result<(Self, usize)> {
        let mut i = 0;

        // Read Position's offset.
        let offset = r.read_u64().await?;
        i += 8;

        // Read Position's size.
        let size = r.read_u64().await?;
        i += 8;

        Ok((Self { offset, size }, i))
    }
}
