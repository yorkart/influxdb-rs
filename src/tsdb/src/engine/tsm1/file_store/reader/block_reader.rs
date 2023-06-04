use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};

use influxdb_storage::opendal::Reader;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::engine::tsm1::file_store::index::IndexEntry;

/// BlockAccessor abstracts a method of accessing blocks from a
/// TSM file.
#[async_trait]
pub trait TSMBlock: Send + Sync {
    async fn read_block(
        &self,
        reader: &mut Reader,
        entry: &IndexEntry,
        buf: &mut Vec<u8>,
    ) -> anyhow::Result<()>;
    async fn free(&self) -> anyhow::Result<()>;
}

pub(crate) struct DefaultBlockAccessor {
    /// Counter incremented everytime the mmapAccessor is accessed
    access_count: AtomicU64,
    /// Counter to determine whether the accessor can free its resources
    free_count: AtomicU64,

    max_offset: u64,
}

impl DefaultBlockAccessor {
    pub async fn new(max_offset: u64) -> anyhow::Result<Self> {
        let access_count = AtomicU64::new(1);
        let free_count = AtomicU64::new(1);

        Ok(Self {
            access_count,
            free_count,
            max_offset,
        })
    }

    fn inc_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl TSMBlock for DefaultBlockAccessor {
    /// returns buf as Vec<u8>, buf[0] is crc,  buf[1..] is blocks
    async fn read_block(
        &self,
        reader: &mut Reader,
        entry: &IndexEntry,
        buf: &mut Vec<u8>,
    ) -> anyhow::Result<()> {
        self.inc_access();

        if entry.offset + entry.size as u64 > self.max_offset {
            return Err(anyhow!("tsm file closed"));
        }

        reader.seek(SeekFrom::Start(entry.offset)).await?;

        let _checksum = reader.read_u32().await?;

        let block_size = entry.size as usize - 4;
        buf.resize(block_size, 0);
        let n = reader.read(buf.as_mut_slice()).await?;
        if n != block_size {
            return Err(anyhow!("not enough entry were read"));
        }

        Ok(())
    }

    async fn free(&self) -> anyhow::Result<()> {
        let access_count = self.access_count.load(Ordering::Relaxed);
        let free_count = self.free_count.load(Ordering::Relaxed);

        // Already freed everything.
        if free_count == 0 && access_count == 0 {
            return Ok(());
        }

        // Were there accesses after the last time we tried to free?
        // If so, don't free anything and record the access count that we
        // see now for the next check.
        if access_count != free_count {
            self.free_count.store(access_count, Ordering::Relaxed);
            return Ok(());
        }

        // Reset both counters to zero to indicate that we have freed everything.
        self.access_count.store(0, Ordering::Relaxed);
        self.free_count.store(0, Ordering::Relaxed);

        Ok(())
    }
}
