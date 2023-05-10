use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::tsm1::block::decoder::{
    decode_bool_block, decode_float_block, decode_integer_block, decode_string_block,
    decode_unsigned_block,
};
use influxdb_storage::opendal::Reader;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::engine::tsm1::encoding::{
    BooleanValues, FloatValues, IntegerValues, StringValues, UnsignedValues,
};
use crate::engine::tsm1::file_store::index::IndexEntry;

/// BlockAccessor abstracts a method of accessing blocks from a
/// TSM file.
#[async_trait]
pub(crate) trait TSMBlock: Send + Sync {
    // async fn read(&mut self, key: &[u8], timestamp: i64) -> anyhow::Result<Values>;
    // async fn read_all(&mut self, key: &[u8]) -> anyhow::Result<Values>;
    // async fn read_block(&mut self, entry: IndexEntry, values: &mut Values) -> anyhow::Result<()>;
    async fn read_float_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut FloatValues,
    ) -> anyhow::Result<()>;
    async fn read_integer_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut IntegerValues,
    ) -> anyhow::Result<()>;
    async fn read_unsigned_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut UnsignedValues,
    ) -> anyhow::Result<()>;
    async fn read_string_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut StringValues,
    ) -> anyhow::Result<()>;
    async fn read_boolean_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut BooleanValues,
    ) -> anyhow::Result<()>;
    async fn read_bytes(
        &mut self,
        reader: &mut Reader,
        entry: IndexEntry,
    ) -> anyhow::Result<Vec<u8>>;
    async fn rename(&mut self, path: &str) -> anyhow::Result<()>;
    async fn close(&mut self) -> anyhow::Result<()>;
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

    async fn read_block(&self, reader: &mut Reader, entry: IndexEntry) -> anyhow::Result<Vec<u8>> {
        self.inc_access();

        if entry.offset + entry.size as u64 > self.max_offset {
            return Err(anyhow!("tsm file closed"));
        }

        reader.seek(SeekFrom::Start(entry.offset)).await?;

        let _checksum = reader.read_u32().await?;

        let mut buf = Vec::with_capacity(entry.size as usize - 4);
        buf.resize(entry.size as usize, 0);
        let n = reader.read(buf.as_mut_slice()).await?;
        if n != entry.size as usize {
            return Err(anyhow!("not enough entry were read"));
        }

        Ok(buf)
    }
}

#[async_trait]
impl TSMBlock for DefaultBlockAccessor {
    async fn read_float_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut FloatValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(reader, entry).await?;
        decode_float_block(buf.as_slice(), values)
    }

    async fn read_integer_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut IntegerValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(reader, entry).await?;
        decode_integer_block(buf.as_slice(), values)
    }

    async fn read_unsigned_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut UnsignedValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(reader, entry).await?;
        decode_unsigned_block(buf.as_slice(), values)
    }

    async fn read_string_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut StringValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(reader, entry).await?;
        decode_string_block(buf.as_slice(), values)
    }

    async fn read_boolean_block(
        &self,
        reader: &mut Reader,
        entry: IndexEntry,
        values: &mut BooleanValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(reader, entry).await?;
        decode_bool_block(buf.as_slice(), values)
    }

    /// returns buf as Vec<u8>, buf[0] is crc,  buf[1..] is blocks
    /// TODO 以流式返回，比如采用返回iterator方式进行
    async fn read_bytes(
        &mut self,
        reader: &mut Reader,
        entry: IndexEntry,
    ) -> anyhow::Result<Vec<u8>> {
        let buf = self.read_block(reader, entry).await?;
        Ok(buf)
    }

    async fn rename(&mut self, _path: &str) -> anyhow::Result<()> {
        todo!()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
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
