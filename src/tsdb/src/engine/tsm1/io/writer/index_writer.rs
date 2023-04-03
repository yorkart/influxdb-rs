use bytes::{BufMut, BytesMut};
use filepath::FilePath;
use std::cmp::Ordering;
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::engine::tsm1::io::index::{IndexEntries, IndexEntry};
use crate::engine::tsm1::io::{indexCountSize, indexEntrySize, maxIndexEntries};

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
    fn marshal_binary() -> anyhow::Result<Vec<u8>>;

    /// write_to writes the index contents to a writer.
    async fn write_to<W: AsyncWrite + Send>(&mut self, w: W) -> anyhow::Result<i64>;

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

/// directIndex is a simple in-memory index implementation for a TSM file.  The full index
/// must fit in memory.
struct DirectIndex {
    key_count: usize,
    size: u32,

    /// The bytes written count of when we last fsync'd
    last_sync: u32,
    fd: File,
    buf: BytesMut,

    f: Box<dyn Syncer>,

    key: Vec<u8>,
    index_entries: Option<IndexEntries>,
}

impl DirectIndex {
    pub fn new(fd: File) -> Self {
        Self {
            key_count: 0,
            size: 0,
            last_sync: 0,
            fd,
            buf: BytesMut::new(),
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

        if index_entries.entries.len() > maxIndexEntries {
            return Err(anyhow!(
                "key '{:?}' exceeds max index entries: {} > {}",
                self.key.as_slice(),
                index_entries.entries.len(),
                maxIndexEntries
            ));
        }

        index_entries.sort();

        let mut buf = Vec::with_capacity(5);
        buf.put_u16(self.key.len() as u16);
        buf.push(index_entries.typ);
        buf.put_u16(index_entries.entries.len() as u16);

        let mut total = 0_u64;

        // Append the key length
        self.fd
            .write(&buf[0..2])
            .await
            .map_err(|e| anyhow!("write: writer key length error: {}", e.to_string()))?;
        total += 2;

        // Append the key
        self.fd
            .write(self.key.as_slice())
            .await
            .map_err(|e| anyhow!("write: writer key error: {}", e.to_string()))?;
        total += self.key.len() as u64;

        // Append the block type and count
        self.fd.write(&buf[2..5]).await.map_err(|e| {
            anyhow!(
                "write: writer block type and count error: {}",
                e.to_string()
            )
        })?;
        total += 3;

        // Append each index entry for all blocks for this key
        let n = index_entries
            .write_to(&mut self.fd)
            .await
            .map_err(|e| anyhow!("write: writer entries error: {}", e.to_string()))?;
        total += n;

        self.key.clear();
        self.index_entries = None;

        Ok(total)
    }
}

#[async_trait]
impl IndexWriter for DirectIndex {
    async fn add(&mut self, key: &[u8], block_type: u8, index_entry: IndexEntry) {
        // Is this the first block being added?
        if self.key.len() == 0 {
            // size of the key stored in the index
            self.size += (2 + key.len()) as u32;
            // size of the count of entries stored in the index
            self.size += indexCountSize as u32;

            self.key.extend_from_slice(key);
            if self.index_entries.is_none() {
                self.index_entries = Some(IndexEntries::new(block_type));
            }

            let index_entries = self.index_entries.as_mut().unwrap();
            index_entries.typ = block_type;
            index_entries.entries.push(index_entry);

            // size of the encoded index entry
            self.size += indexEntrySize as u32;
            self.key_count += 1;

            return;
        }

        match self.key.as_slice().cmp(key) {
            Ordering::Equal => {
                let index_entries = self.index_entries.as_mut().unwrap();

                // The last block is still this key
                index_entries.entries.push(index_entry);

                // size of the encoded index entry
                self.size += indexEntrySize as u32;
            }
            Ordering::Less => {
                self.flush();
                // We have a new key that is greater than the last one so we need to add
                // a new index block section.

                // size of the key stored in the index
                self.size += (2 + key.len()) as u32;
                // size of the count of entries stored in the index
                self.size += indexCountSize as u32;

                self.key.clear();
                self.key.extend_from_slice(key);

                let index_entries = self.index_entries.as_mut().unwrap();
                index_entries.typ = block_type;
                index_entries.entries.push(index_entry);

                // size of the encoded index entry
                self.size += indexEntrySize as u32;
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

    fn marshal_binary() -> anyhow::Result<Vec<u8>> {
        todo!()
    }

    async fn write_to<W: AsyncWrite + Send>(&mut self, w: W) -> anyhow::Result<i64> {
        self.flush().await?;
        self.fd.flush().await.map_err(|e| anyhow!(e))?;
    }

    async fn close(mut self, flush: bool) -> anyhow::Result<()> {
        if flush {
            // Flush anything remaining in the index
            self.fd
                .flush()
                .await
                .map_err(|e| anyhow!("flush file error: {}", e))?;
        }

        let fd = self.fd.into_std().await;
        let path = fd
            .path()
            .map_err(|e| anyhow!("get file path error: {}", e))?;

        drop(fd);

        tokio::fs::remove_file(path)
            .await
            .map_err(|e| anyhow!("remote file error: {}", e))
    }
}
