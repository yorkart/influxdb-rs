use crate::engine::tsm1::block::decoder::decode_block;
use influxdb_storage::opendal::{Operator, Reader};
use influxdb_storage::RandomAccess;
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;

use crate::engine::tsm1::encoding::{
    BooleanValues, FloatValues, IntegerValues, StringValues, UnsignedValues, Values,
};
use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::reader::batch_deleter::BatchDeleter;
use crate::engine::tsm1::file_store::reader::index_reader::IndirectIndex;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::stat::FileStat;
use crate::engine::tsm1::file_store::tombstone::{TombstoneStat, Tombstoner};
use crate::engine::tsm1::file_store::{MAGIC_NUMBER, VERSION};

/// TimeRange holds a min and max timestamp.
#[derive(Clone)]
pub struct TimeRange {
    pub(crate) min: i64,
    pub(crate) max: i64,
}

impl TimeRange {
    pub fn new(min: i64, max: i64) -> Self {
        Self { min, max }
    }

    pub fn unbound() -> Self {
        Self::new(i64::MIN, i64::MAX)
    }

    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.min <= other.max && self.max >= other.min
    }
}

/// TimeRange holds a min and max timestamp.
pub struct KeyRange<'a, 'b> {
    min: &'a [u8],
    max: &'b [u8],
}

/// TSMFile represents an on-disk TSM file.
#[async_trait]
pub trait TSMReader {
    /// path returns the underlying file path for the TSMFile.  If the file
    /// has not be written or loaded from disk, the zero value is returned.
    async fn path(&self) -> &str;

    /// read returns all the values in the block where time t resides.
    async fn read(&mut self, key: &[u8], t: i64) -> anyhow::Result<()>;

    /// read_at returns all the values in the block identified by entry.
    async fn read_at(&mut self, entry: IndexEntry, values: Values) -> anyhow::Result<()>;

    async fn read_float_block_at(
        &mut self,
        entry: IndexEntry,
        values: FloatValues,
    ) -> anyhow::Result<()>;

    async fn read_integer_block_at(
        &mut self,
        entry: IndexEntry,
        values: IntegerValues,
    ) -> anyhow::Result<()>;

    async fn read_unsigned_block_at(
        &mut self,
        entry: IndexEntry,
        values: UnsignedValues,
    ) -> anyhow::Result<()>;

    async fn read_string_block_at(
        &mut self,
        entry: IndexEntry,
        values: StringValues,
    ) -> anyhow::Result<()>;

    async fn read_boolean_block_at(
        &mut self,
        entry: IndexEntry,
        values: BooleanValues,
    ) -> anyhow::Result<()>;

    /// Entries returns the index entries for all blocks for the given key.
    async fn read_entries(
        &mut self,
        key: &[u8],
        entries: &mut Vec<IndexEntry>,
    ) -> anyhow::Result<()>;

    /// Returns true if the TSMFile may contain a value with the specified
    /// key and time.
    async fn contains_value(&mut self, key: &[u8], t: i64) -> bool;

    /// contains returns true if the file contains any values for the given
    /// key.
    async fn contains(&mut self, key: &[u8]) -> bool;

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    async fn overlaps_time_range(&mut self, min: i64, max: i64) -> bool;

    /// overlaps_key_range returns true if the key range of the file intersects min and max.
    async fn overlaps_key_range(&mut self, min: &[u8], max: &[u8]) -> bool;

    /// time_range returns the min and max time across all keys in the file.
    async fn time_range(&self) -> (i64, i64);

    /// tombstone_range returns ranges of time that are deleted for the given key.
    async fn tombstone_range(&self, key: &[u8]) -> Vec<TimeRange>;

    /// key_range returns the min and max keys in the file.
    async fn key_range(&self) -> (&[u8], &[u8]);

    /// key_count returns the number of distinct keys in the file.
    async fn key_count(&self) -> usize;

    /// seek returns the position in the index with the key <= key.
    async fn seek(&mut self, key: &[u8]) -> usize;

    /// key_at returns the key located at index position idx.
    async fn key_at(&mut self, idx: usize) -> (&[u8], u8);

    /// Type returns the block type of the values stored for the key.  Returns one of
    /// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
    /// an error is returned.
    async fn block_type(&mut self, key: &[u8]) -> anyhow::Result<u8>;

    /// batch_delete return a BatchDeleter that allows for multiple deletes in batches
    /// and group commit or rollback.
    async fn batch_delete(&mut self) -> Box<dyn BatchDeleter>;

    /// delete removes the keys from the set of keys available in this file.
    async fn delete(&mut self, keys: &[&[u8]]) -> anyhow::Result<()>;

    /// delete_range removes the values for keys between timestamps min and max.
    async fn delete_range(&mut self, keys: &[&[u8]], min: i64, max: i64) -> anyhow::Result<()>;

    /// has_tombstones returns true if file contains values that have been deleted.
    async fn has_tombstones(&self) -> bool;

    /// tombstone_stats returns the tombstone filestats if there are any tombstones
    /// written for this file.
    async fn tombstone_stats(&self) -> TombstoneStat;

    /// close closes the underlying file resources.
    async fn close(&mut self) -> anyhow::Result<()>;

    /// size returns the size of the file on disk in bytes.
    async fn size(&self) -> u32;

    /// rename renames the existing TSM file to a new name and replaces the mmap backing slice using the new
    /// file name. Index and Reader state are not re-initialized.
    async fn rename(&mut self, path: &str) -> anyhow::Result<()>;

    /// remove deletes the file from the filesystem.
    async fn remove(&mut self) -> anyhow::Result<()>;

    /// in_use returns true if the file is currently in use by queries.
    async fn in_use(&self) -> bool;

    /// use_ref records that this file is actively in use.
    async fn use_ref(&mut self);

    /// use_unref records that this file is no longer in use.
    async fn use_unref(&mut self);

    /// stats returns summary information about the TSM file.
    async fn stats(&self) -> FileStat;

    /// free releases any resources held by the FileStore to free up system resources.
    async fn free(&mut self) -> anyhow::Result<()>;
}

pub(crate) struct DefaultTSMReader<I>
where
    I: TSMIndex,
{
    /// refs is the count of active references to this reader.
    refs: AtomicU64,

    mu: RwLock<bool>,

    /// accessor provides access and decoding of blocks for the reader.
    accessor: Box<dyn RandomAccess>,

    /// index is the index of all blocks.
    index: RwLock<I>,

    /// tombstoner ensures tombstoned keys are not available by the index.
    tombstoner: Tombstoner,

    /// size is the size of the file on disk.
    size: i64,

    /// last_modified is the last time this file was modified on disk
    last_modified: i64,

    /// Counter incremented everytime the mmapAccessor is accessed
    access_count: u64,
    /// Counter to determine whether the accessor can free its resources
    free_count: u64,
}

impl<I> DefaultTSMReader<I>
where
    I: TSMIndex,
{
    // pub fn new(op: Operator, path: impl AsRef<Path>) -> Self {
    //     Self {
    //         refs: AtomicU64::new(0),
    //         mu: (),
    //         accessor: (),
    //         index: (),
    //         tombstoner: (),
    //         size: (),
    //         last_modified: (),
    //         access_count: (),
    //         free_count: (),
    //     }
    // }
}

/// BlockAccessor abstracts a method of accessing blocks from a
/// TSM file.
#[async_trait]
trait BlockAccessor<T: TSMIndex> {
    async fn read(&mut self, key: &[u8], timestamp: i64) -> anyhow::Result<Values>;
    async fn read_all(&mut self, key: &[u8]) -> anyhow::Result<Values>;
    async fn read_block(&mut self, entry: IndexEntry, values: &mut Values) -> anyhow::Result<()>;
    async fn read_float_block(
        &mut self,
        entry: IndexEntry,
        values: &mut FloatValues,
    ) -> anyhow::Result<()>;
    async fn read_integer_block(
        &mut self,
        entry: IndexEntry,
        values: &mut IntegerValues,
    ) -> anyhow::Result<()>;
    async fn read_unsigned_block(
        &mut self,
        entry: IndexEntry,
        values: &mut UnsignedValues,
    ) -> anyhow::Result<()>;
    async fn read_string_block(
        &mut self,
        entry: IndexEntry,
        values: &mut StringValues,
    ) -> anyhow::Result<()>;
    async fn read_boolean_block(
        &mut self,
        entry: IndexEntry,
        values: &mut BooleanValues,
    ) -> anyhow::Result<()>;
    async fn read_bytes(&mut self, entry: IndexEntry, buf: &[u8]) -> anyhow::Result<(u32, &[u8])>;
    async fn rename(&mut self, path: &str) -> anyhow::Result<()>;
    fn path(&self) -> String;
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn free(&mut self) -> anyhow::Result<()>;
}

struct DefaultBlockAccessor {
    /// Counter incremented everytime the mmapAccessor is accessed
    access_count: AtomicU64,
    /// Counter to determine whether the accessor can free its resources
    free_count: AtomicU64,

    tsm_path: String,
    op: Operator,

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
}

#[async_trait]
impl BlockAccessor<IndirectIndex> for DefaultBlockAccessor {
    async fn read(&mut self, key: &[u8], timestamp: i64) -> anyhow::Result<Values> {
        todo!()
    }

    async fn read_all(&mut self, key: &[u8]) -> anyhow::Result<Values> {
        todo!()
    }

    async fn read_block(&mut self, entry: IndexEntry, values: &mut Values) -> anyhow::Result<()> {
        self.inc_access();

        // decode_block()
        Ok(())
    }

    async fn read_float_block(
        &mut self,
        entry: IndexEntry,
        values: &mut FloatValues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn read_integer_block(
        &mut self,
        entry: IndexEntry,
        values: &mut IntegerValues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn read_unsigned_block(
        &mut self,
        entry: IndexEntry,
        values: &mut UnsignedValues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn read_string_block(
        &mut self,
        entry: IndexEntry,
        values: &mut StringValues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn read_boolean_block(
        &mut self,
        entry: IndexEntry,
        values: &mut BooleanValues,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn read_bytes(&mut self, entry: IndexEntry, buf: &[u8]) -> anyhow::Result<(u32, &[u8])> {
        todo!()
    }

    async fn rename(&mut self, path: &str) -> anyhow::Result<()> {
        todo!()
    }

    fn path(&self) -> String {
        todo!()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    async fn free(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
