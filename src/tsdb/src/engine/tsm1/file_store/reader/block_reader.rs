use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::tsm1::block::decoder::{
    decode_bool_block, decode_float_block, decode_integer_block, decode_string_block,
    decode_unsigned_block,
};
use influxdb_storage::opendal::{Operator, Reader};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;

use crate::engine::tsm1::encoding::{
    BooleanValues, FloatValues, IntegerValues, StringValues, UnsignedValues,
};
use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::reader::index_reader::IndirectIndex;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::{MAGIC_NUMBER, VERSION};

/// BlockAccessor abstracts a method of accessing blocks from a
/// TSM file.
#[async_trait]
trait BlockAccessor<T: TSMIndex> {
    // async fn read(&mut self, key: &[u8], timestamp: i64) -> anyhow::Result<Values>;
    // async fn read_all(&mut self, key: &[u8]) -> anyhow::Result<Values>;
    // async fn read_block(&mut self, entry: IndexEntry, values: &mut Values) -> anyhow::Result<()>;
    async fn read_float_block(
        &self,
        entry: IndexEntry,
        values: &mut FloatValues,
    ) -> anyhow::Result<()>;
    async fn read_integer_block(
        &self,
        entry: IndexEntry,
        values: &mut IntegerValues,
    ) -> anyhow::Result<()>;
    async fn read_unsigned_block(
        &self,
        entry: IndexEntry,
        values: &mut UnsignedValues,
    ) -> anyhow::Result<()>;
    async fn read_string_block(
        &self,
        entry: IndexEntry,
        values: &mut StringValues,
    ) -> anyhow::Result<()>;
    async fn read_boolean_block(
        &self,
        entry: IndexEntry,
        values: &mut BooleanValues,
    ) -> anyhow::Result<()>;
    async fn read_bytes(&mut self, entry: IndexEntry) -> anyhow::Result<Vec<u8>>;
    async fn rename(&mut self, path: &str) -> anyhow::Result<()>;
    fn path(&self) -> &str;
    async fn close(self) -> anyhow::Result<()>;
    async fn free(&mut self) -> anyhow::Result<()>;
}

struct DefaultBlockAccessor {
    /// Counter incremented everytime the mmapAccessor is accessed
    access_count: AtomicU64,
    /// Counter to determine whether the accessor can free its resources
    free_count: AtomicU64,

    tsm_path: String,
    op: Operator,
    reader: RwLock<Reader>,
    max_offset: u64,

    index: IndirectIndex,
}

impl DefaultBlockAccessor {
    pub async fn new(tsm_path: String, op: Operator) -> anyhow::Result<Self> {
        let mut reader = op.reader(tsm_path.as_str()).await?;

        Self::verify_version(&mut reader).await?;

        reader.seek(SeekFrom::Start(0)).await?;

        let stat = op.stat(tsm_path.as_str()).await?;
        let file_size = stat.content_length();
        if file_size < 8 {
            return Err(anyhow!(
                "BlockAccessor: byte slice too small for IndirectIndex"
            ));
        }

        let index_ofs_pos = file_size - 8;
        reader.seek(SeekFrom::Start(index_ofs_pos)).await?;
        let index_start = reader.read_u64().await?;

        let index = IndirectIndex::new(
            op.reader(tsm_path.as_str()).await?,
            index_start,
            (index_ofs_pos - index_start) as u32,
        )
        .await?;

        let access_count = AtomicU64::new(1);
        let free_count = AtomicU64::new(1);

        Ok(Self {
            access_count,
            free_count,
            tsm_path,
            op,
            reader: RwLock::new(reader),
            max_offset: index_ofs_pos,
            index,
        })
    }

    async fn verify_version(reader: &mut Reader) -> anyhow::Result<()> {
        reader
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|e| anyhow!("init: error reading magic number of file: {}", e))?;

        let magic_number = reader
            .read_u32()
            .await
            .map_err(|e| anyhow!("init: error reading magic number of file: {}", e))?;
        if magic_number != MAGIC_NUMBER {
            return Err(anyhow!("can only read from tsm file"));
        }

        let version = reader
            .read_u8()
            .await
            .map_err(|e| anyhow!("init: error reading version: {}", e))?;
        if version != VERSION {
            return Err(anyhow!(
                "init: file is version {}. expected {}",
                version,
                VERSION
            ));
        }

        Ok(())
    }

    fn inc_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    async fn read_block(&self, entry: IndexEntry) -> anyhow::Result<Vec<u8>> {
        self.inc_access();

        if entry.offset + entry.size as u64 > self.max_offset {
            return Err(anyhow!("tsm file closed"));
        }

        let mut reader = self.reader.write().await;

        // TODO optimize: 这里buf大小不可控，可能会oom，应该才有固定大小的buf，以流式的方式解析
        let mut buf = Vec::with_capacity(entry.size as usize);
        buf.resize(entry.size as usize, 0);

        reader.seek(SeekFrom::Start(entry.offset)).await?;
        let n = reader.read(buf.as_mut_slice()).await?;
        if n != entry.size as usize {
            return Err(anyhow!("not enough entry were read"));
        }

        Ok(buf)
    }
}

#[async_trait]
impl BlockAccessor<IndirectIndex> for DefaultBlockAccessor {
    async fn read_float_block(
        &self,
        entry: IndexEntry,
        values: &mut FloatValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(entry).await?;
        decode_float_block(buf.as_slice(), values)
    }

    async fn read_integer_block(
        &self,
        entry: IndexEntry,
        values: &mut IntegerValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(entry).await?;
        decode_integer_block(buf.as_slice(), values)
    }

    async fn read_unsigned_block(
        &self,
        entry: IndexEntry,
        values: &mut UnsignedValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(entry).await?;
        decode_unsigned_block(buf.as_slice(), values)
    }

    async fn read_string_block(
        &self,
        entry: IndexEntry,
        values: &mut StringValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(entry).await?;
        decode_string_block(buf.as_slice(), values)
    }

    async fn read_boolean_block(
        &self,
        entry: IndexEntry,
        values: &mut BooleanValues,
    ) -> anyhow::Result<()> {
        let buf = self.read_block(entry).await?;
        decode_bool_block(buf.as_slice(), values)
    }

    /// returns buf as Vec<u8>, buf[0] is crc,  buf[1..] is blocks
    /// TODO 以流式返回，比如采用返回iterator方式进行
    async fn read_bytes(&mut self, entry: IndexEntry) -> anyhow::Result<Vec<u8>> {
        let buf = self.read_block(entry).await?;
        Ok(buf)
    }

    async fn rename(&mut self, _path: &str) -> anyhow::Result<()> {
        todo!()
    }

    fn path(&self) -> &str {
        self.tsm_path.as_str()
    }

    async fn close(self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn free(&mut self) -> anyhow::Result<()> {
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
