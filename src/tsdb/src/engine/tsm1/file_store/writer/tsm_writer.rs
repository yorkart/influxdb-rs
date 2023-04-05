use std::path::Path;

use bytes::BytesMut;
use filepath::FilePath;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::engine::tsm1::block::decoder::block_type;
use crate::engine::tsm1::block::encoder::encode_block;
use crate::engine::tsm1::encoding::{TValues, Values};
use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::writer::index_writer::{
    DirectIndex, FileIndexBuffer, IndexWriter, MemoryIndexBuffer,
};
use crate::engine::tsm1::file_store::{FSYNC_EVERY, HEADER, MAX_INDEX_ENTRIES, MAX_KEY_LENGTH};

/// TSMWriter writes TSM formatted key and values.
#[async_trait]
pub trait TSMWriter {
    /// write writes a new block for key containing and values.  Writes append
    /// blocks in the order that the Write function is called.  The caller is
    /// responsible for ensuring keys and blocks are sorted appropriately.
    /// Values are encoded as a full block.  The caller is responsible for
    /// ensuring a fixed number of values are encoded in each block as well as
    /// ensuring the Values are sorted. The first and last timestamp values are
    /// used as the minimum and maximum values for the index entry.
    async fn write(&mut self, key: &[u8], values: Values) -> anyhow::Result<()>;

    /// write_block writes a new block for key containing the bytes in block.  WriteBlock appends
    /// blocks in the order that the WriteBlock function is called.  The caller is
    /// responsible for ensuring keys and blocks are sorted appropriately, and that the
    /// block and index information is correct for the block.  The min_time and max_time
    /// timestamp values are used as the minimum and maximum values for the index entry.
    async fn write_block(
        &mut self,
        key: &[u8],
        min_time: i64,
        max_time: i64,
        block: &[u8],
    ) -> anyhow::Result<()>;

    /// write_index finishes the TSM write streams and writes the index.
    async fn write_index(&mut self) -> anyhow::Result<()>;

    /// Flushes flushes all pending changes to the underlying file resources.
    async fn flush(&mut self) -> anyhow::Result<()>;

    /// close closes any underlying file resources.
    async fn close(self) -> anyhow::Result<()>;

    /// size returns the current size in bytes of the file.
    fn size(&self) -> u32;

    async fn remove(mut self) -> anyhow::Result<()>;
}

pub struct DefaultTSMWriter<I>
where
    I: IndexWriter + Send + 'static,
{
    fd: File,
    buf: BytesMut,

    index: I,
    n: u64,

    // The bytes written count of when we last fsync'd
    last_sync: u64,
}

impl DefaultTSMWriter<DirectIndex<MemoryIndexBuffer>> {
    pub async fn with_mem_buffer(tsm_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::new(tsm_path, DirectIndex::with_mem_buffer(1024 * 1024)).await
    }
}

impl DefaultTSMWriter<DirectIndex<FileIndexBuffer>> {
    pub async fn with_disk_buffer(
        tsm_path: impl AsRef<Path>,
        idx_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        Self::new(tsm_path, DirectIndex::with_disk_buffer(idx_path).await?).await
    }
}

impl<I> DefaultTSMWriter<I>
where
    I: IndexWriter + Send + 'static,
{
    pub async fn new(tsm_path: impl AsRef<Path>, index: I) -> anyhow::Result<Self> {
        let fd = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(tsm_path)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Self {
            fd,
            buf: BytesMut::with_capacity(1024 * 1024),
            index,
            n: 0,
            last_sync: 0,
        })
    }

    async fn write_header(&mut self) -> anyhow::Result<()> {
        // let mut buf = Vec::with_capacity(5);
        // buf.put_u32(MAGIC_NUMBER);
        // buf.put_u8(VERSION);

        let n = self.fd.write(&HEADER).await.map_err(|e| anyhow!(e))?;
        self.n = n as u64;

        Ok(())
    }

    async fn sync(&mut self) -> anyhow::Result<()> {
        self.fd.flush().await.map_err(|e| anyhow!(e))?;
        self.fd.sync_all().await.map_err(|e| anyhow!(e))
    }
}

#[async_trait]
impl<I> TSMWriter for DefaultTSMWriter<I>
where
    I: IndexWriter + Send + 'static,
{
    async fn write(&mut self, key: &[u8], values: Values) -> anyhow::Result<()> {
        if key.len() > MAX_KEY_LENGTH {
            // TODO return ErrMaxKeyLengthExceeded
            return Err(anyhow!("ErrMaxKeyLengthExceeded"));
        }

        // Nothing to write
        if values.len() == 0 {
            return Ok(());
        }

        let min_time = values.min_time();
        let max_time = values.max_time();

        let mut block = vec![];
        encode_block(&mut block, values)?;

        self.write_block(key, min_time, max_time, block.as_slice())
            .await
    }

    async fn write_block(
        &mut self,
        key: &[u8],
        min_time: i64,
        max_time: i64,
        block: &[u8],
    ) -> anyhow::Result<()> {
        if key.len() > MAX_KEY_LENGTH {
            // TODO return ErrMaxKeyLengthExceeded
            return Err(anyhow!("ErrMaxKeyLengthExceeded"));
        }

        // Nothing to write
        if block.len() == 0 {
            return Ok(());
        }

        let block_type = block_type(block)?;

        // Write header only after we have some data to write.
        if self.n == 0 {
            self.write_header().await?;
        }

        let mut n = 0;
        let checksum = crc32fast::hash(block);
        self.fd.write_u32(checksum).await.map_err(|e| anyhow!(e))?;
        n += 4;
        n += self.fd.write(block).await.map_err(|e| anyhow!(e))?;

        // Record this block in index
        let index_entry = IndexEntry {
            min_time,
            max_time,
            offset: self.n,
            size: n as u32,
        };
        self.index.add(key, block_type, index_entry).await;

        // Increment file position pointer
        self.n += n as u64;

        // fsync the file periodically to avoid long pauses with very big files.
        if self.n - self.last_sync > FSYNC_EVERY {
            self.sync().await?;
            self.last_sync = self.n
        }

        if self.index.entries(key).map(|x| x.len()).unwrap_or_default() >= MAX_INDEX_ENTRIES {
            // TODO return ErrMaxBlocksExceeded
            return Err(anyhow!("ErrMaxBlocksExceeded"));
        }

        Ok(())
    }

    /// WriteIndex writes the index section of the file.  If there are no index entries to write,
    /// this returns ErrNoValues.
    async fn write_index(&mut self) -> anyhow::Result<()> {
        let index_pos = self.n;

        if self.index.key_count() == 0 {
            // TODO return ErrNoValues
            return Err(anyhow!("ErrNoValues"));
        }

        // Set the destination file on the index so we can periodically
        // fsync while writing the index.
        // if f, ok := t.wrapped.(syncer); ok {
        //     t.index.(*directIndex).f = f
        // }

        // Write the index
        self.index.write_to(&mut self.fd).await?;

        // Write the index index position
        self.fd.write_u64(index_pos).await.map_err(|e| anyhow!(e))
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.sync().await
    }

    async fn close(mut self) -> anyhow::Result<()> {
        self.flush().await?;
        self.index.close(true).await?;

        // if c, ok := t.wrapped.(io.Closer); ok {
        //     return c.Close()
        // }
        // return nil
        Ok(())
    }

    fn size(&self) -> u32 {
        self.n as u32 + self.index.size()
    }

    async fn remove(mut self) -> anyhow::Result<()> {
        let Self { fd, index, .. } = self;

        index.close(false).await?;

        let fd = fd.into_std().await;
        let path = fd.path()?;

        drop(fd);

        tokio::fs::remove_file(path).await.map_err(|e| anyhow!(e))
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::encoding::{Value, Values};
    use crate::engine::tsm1::file_store::writer::tsm_writer::{DefaultTSMWriter, TSMWriter};

    #[test]
    fn test_crc() {
        let checksum = crc32fast::hash("adsafafas".as_bytes());
        assert_eq!(checksum, 2344674872);
    }

    #[tokio::test]
    async fn test_tsm_writer_write_empty() {
        let dir = tempfile::tempdir().unwrap();
        let tsm_file = dir.as_ref().join("tsm1_test");
        println!("{}", tsm_file.to_str().unwrap());

        let mut w = DefaultTSMWriter::with_mem_buffer(&tsm_file).await.unwrap();

        let values = Values::Float(vec![Value::new(0, 1.0)]);

        w.write("cpu".as_bytes(), values).await.unwrap();
        w.write_index().await.unwrap();
        w.close().await.unwrap();

        let data = tokio::fs::read(tsm_file).await.unwrap();
        let checksum = crc32fast::hash(data.as_slice());
        assert_eq!(checksum, 1704948981);
        assert_eq!(
            data.as_slice(),
            &[
                22, 209, 22, 209, 1, 227, 243, 238, 20, 0, 9, 28, 0, 0, 0, 0, 0, 0, 0, 0, 16, 63,
                240, 0, 0, 0, 0, 0, 0, 195, 252, 0, 128, 0, 0, 0, 0, 0, 16, 0, 3, 99, 112, 117, 0,
                0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0,
                0, 34, 0, 0, 0, 0, 0, 0, 0, 39
            ]
        );
    }
}
