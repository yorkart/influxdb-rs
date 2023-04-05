use std::cmp::Ordering;
use std::io::{Error, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, BytesMut};
use filepath::FilePath;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::engine::tsm1::io::index::{IndexEntries, IndexEntry};
use crate::engine::tsm1::io::{FSYNC_EVERY, INDEX_COUNT_SIZE, INDEX_ENTRY_SIZE, MAX_INDEX_ENTRIES};

/// IndexWriter writes a TSMIndex.
#[async_trait]
pub trait IndexWriter {
    /// add records a new block entry for a key in the index.
    async fn add(&mut self, key: &[u8], block_type: u8, index_entry: IndexEntry);

    /// entries returns all index entries for a key.
    fn entries(&self, key: &[u8]) -> Option<&[IndexEntry]>;

    /// key_count returns the count of unique keys in the index.
    fn key_count(&self) -> usize;

    /// size returns the size of a the current index in bytes.
    fn size(&self) -> u32;

    /// marshal_binary returns a byte slice encoded version of the index.
    /// for test
    fn marshal_binary(&self) -> anyhow::Result<Vec<u8>>;

    /// write_to writes the index contents to a writer.
    async fn write_to<W: AsyncWrite + Send + Unpin>(&mut self, w: W) -> anyhow::Result<u64>;

    async fn close(self, flush: bool) -> anyhow::Result<()>;
}

#[async_trait]
trait Syncer: Send + Sync {
    fn name(&self) -> &str;
    async fn sync(&mut self) -> anyhow::Result<()>;
}

struct DefaultSyncer {}

#[async_trait]
impl Syncer for DefaultSyncer {
    fn name(&self) -> &str {
        ""
    }

    async fn sync(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait IndexBuffer: AsyncWrite + Unpin + Send {
    async fn write_to<W: AsyncWrite + Send + Unpin>(&mut self, w: W) -> std::io::Result<u64>;
    async fn sync(&mut self) -> std::io::Result<()>;
    async fn clear(self) -> std::io::Result<()>;
}

pub(crate) struct MemoryIndexBuffer {
    buf: BytesMut,
}

impl MemoryIndexBuffer {
    pub fn new(sz: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(sz),
        }
    }
}

impl AsyncWrite for MemoryIndexBuffer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        self.buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl IndexBuffer for MemoryIndexBuffer {
    async fn write_to<W: AsyncWrite + Send + Unpin>(&mut self, mut w: W) -> std::io::Result<u64> {
        w.write(self.buf.as_ref()).await.map(|w| w as u64)
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn clear(self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) struct FileIndexBuffer {
    fd: File,
}

impl FileIndexBuffer {
    pub fn new(fd: File) -> Self {
        Self { fd }
    }
}

#[async_trait]
impl IndexBuffer for FileIndexBuffer {
    async fn write_to<W: AsyncWrite + Send + Unpin>(&mut self, mut w: W) -> std::io::Result<u64> {
        self.fd.seek(SeekFrom::Start(0)).await?;
        tokio::io::copy(&mut self.fd, &mut w).await
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.fd.flush().await?;
        self.fd.sync_all().await
    }

    async fn clear(self) -> std::io::Result<()> {
        let fd = self.fd.into_std().await;
        let path = fd.path()?;

        drop(fd);

        tokio::fs::remove_file(path).await
    }
}

impl AsyncWrite for FileIndexBuffer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.fd).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.fd).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.fd).poll_shutdown(cx)
    }
}

/// directIndex is a simple in-memory index implementation for a TSM file.  The full index
/// must fit in memory.
pub(crate) struct DirectIndex<B>
where
    B: IndexBuffer + 'static,
{
    key_count: usize,
    size: u32,

    /// The bytes written count of when we last fsync'd
    last_sync: u32,
    buf: B,

    f: Box<dyn Syncer>,

    key: Vec<u8>,
    index_entries: Option<IndexEntries>,
}

impl DirectIndex<MemoryIndexBuffer> {
    pub fn with_mem_buffer(sz: usize) -> Self {
        Self {
            key_count: 0,
            size: 0,
            last_sync: 0,
            buf: MemoryIndexBuffer::new(sz),
            f: Box::new(DefaultSyncer {}),
            key: vec![],
            index_entries: None,
        }
    }
}

impl DirectIndex<FileIndexBuffer> {
    pub async fn with_disk_buffer(idx_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let idx_fd = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(idx_path)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(Self {
            key_count: 0,
            size: 0,
            last_sync: 0,
            buf: FileIndexBuffer::new(idx_fd),
            f: Box::new(DefaultSyncer {}),
            key: vec![],
            index_entries: None,
        })
    }
}

impl<B> DirectIndex<B>
where
    B: IndexBuffer + 'static,
{
    pub fn new(buf: B) -> Self {
        Self {
            key_count: 0,
            size: 0,
            last_sync: 0,
            buf,
            f: Box::new(DefaultSyncer {}),
            key: vec![],
            index_entries: None,
        }
    }

    pub fn entry(&self, key: &[u8], t: i64) -> Option<&IndexEntry> {
        let entries = self.entries(key);
        if let Some(entries) = entries {
            for entry in entries {
                if entry.contains(t) {
                    return Some(entry);
                }
            }
        }

        return None;
    }

    async fn flush(&mut self) -> anyhow::Result<u64> {
        if self.key.len() == 0 {
            return Ok(0);
        }

        // For each key, individual entries are sorted by time
        let mut index_entries = self.index_entries.take().unwrap();

        if index_entries.entries.len() > MAX_INDEX_ENTRIES {
            return Err(anyhow!(
                "key '{:?}' exceeds max index entries: {} > {}",
                self.key.as_slice(),
                index_entries.entries.len(),
                MAX_INDEX_ENTRIES
            ));
        }

        index_entries.sort();

        let mut buf = Vec::with_capacity(5);
        buf.put_u16(self.key.len() as u16);
        buf.push(index_entries.typ);
        buf.put_u16(index_entries.entries.len() as u16);

        let mut total = 0_u64;

        // Append the key length
        self.buf
            .write(&buf[0..2])
            .await
            .map_err(|e| anyhow!("write: writer key length error: {}", e.to_string()))?;
        total += 2;

        // Append the key
        self.buf
            .write(self.key.as_slice())
            .await
            .map_err(|e| anyhow!("write: writer key error: {}", e.to_string()))?;
        total += self.key.len() as u64;

        // Append the block type and count
        self.buf.write(&buf[2..5]).await.map_err(|e| {
            anyhow!(
                "write: writer block type and count error: {}",
                e.to_string()
            )
        })?;
        total += 3;

        // Append each index entry for all blocks for this key
        let n = index_entries
            .write_to(&mut self.buf)
            .await
            .map_err(|e| anyhow!("write: writer entries error: {}", e.to_string()))?;
        total += n;

        self.key.clear();
        self.index_entries = None;

        // If this is a disk based index and we've written more than the fsync threshold,
        // fsync the data to avoid long pauses later on.
        if self.size - self.last_sync > FSYNC_EVERY as u32 {
            self.buf.sync().await.map_err(|e| anyhow!(e))?;
            self.last_sync = self.size;
        }

        Ok(total)
    }
}

#[async_trait]
impl<B> IndexWriter for DirectIndex<B>
where
    B: IndexBuffer + 'static,
{
    async fn add(&mut self, key: &[u8], block_type: u8, index_entry: IndexEntry) {
        // Is this the first block being added?
        if self.key.len() == 0 {
            // size of the key stored in the index
            self.size += (2 + key.len()) as u32;
            // size of the count of entries stored in the index
            self.size += INDEX_COUNT_SIZE as u32;

            self.key.extend_from_slice(key);
            if self.index_entries.is_none() {
                self.index_entries = Some(IndexEntries::new(block_type));
            }

            let index_entries = self.index_entries.as_mut().unwrap();
            index_entries.typ = block_type;
            index_entries.entries.push(index_entry);

            // size of the encoded index entry
            self.size += INDEX_ENTRY_SIZE as u32;
            self.key_count += 1;

            return;
        }

        match self.key.as_slice().cmp(key) {
            Ordering::Equal => {
                let index_entries = self.index_entries.as_mut().unwrap();

                // The last block is still this key
                index_entries.entries.push(index_entry);

                // size of the encoded index entry
                self.size += INDEX_ENTRY_SIZE as u32;
            }
            Ordering::Less => {
                self.flush();
                // We have a new key that is greater than the last one so we need to add
                // a new index block section.

                // size of the key stored in the index
                self.size += (2 + key.len()) as u32;
                // size of the count of entries stored in the index
                self.size += INDEX_COUNT_SIZE as u32;

                self.key.clear();
                self.key.extend_from_slice(key);

                let index_entries = self.index_entries.as_mut().unwrap();
                index_entries.typ = block_type;
                index_entries.entries.push(index_entry);

                // size of the encoded index entry
                self.size += INDEX_ENTRY_SIZE as u32;
                self.key_count += 1;
            }
            Ordering::Greater => {
                // Keys can't be added out of order.
                panic!(
                    "keys must be added in sorted order: {:?} < {:?}",
                    key,
                    self.key.as_slice()
                );
            }
        }
    }

    fn entries(&self, key: &[u8]) -> Option<&[IndexEntry]> {
        if self.key.len() == 0 {
            return None;
        }

        if let Ordering::Equal = self.key.as_slice().cmp(key) {
            return self.index_entries.as_ref().map(|ie| ie.entries.as_slice());
        }

        return None;
    }

    fn key_count(&self) -> usize {
        self.key_count
    }

    fn size(&self) -> u32 {
        self.size
    }

    fn marshal_binary(&self) -> anyhow::Result<Vec<u8>> {
        todo!()
    }

    async fn write_to<W: AsyncWrite + Send + Unpin>(&mut self, w: W) -> anyhow::Result<u64> {
        self.flush().await?;
        self.buf.sync().await.map_err(|e| anyhow!(e))?;
        self.buf.write_to(w).await.map_err(|e| anyhow!(e))
    }

    async fn close(mut self, flush: bool) -> anyhow::Result<()> {
        if flush {
            // Flush anything remaining in the index
            self.buf
                .sync()
                .await
                .map_err(|e| anyhow!("flush buf error: {}", e))?;
        }

        self.buf
            .clear()
            .await
            .map_err(|e| anyhow!("clear buf error: {}", e))
    }
}
