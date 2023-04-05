use crate::engine::tsm1::encoding::{
    BooleanValues, FloatValues, IntegerValues, StringValues, UnsignedValues, Values,
};
use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::reader::batch_deleter::BatchDeleter;
use crate::engine::tsm1::file_store::stat::{FileStat, TombstoneStat};

/// TimeRange holds a min and max timestamp.
pub struct TimeRange {
    min: i64,
    max: i64,
}

impl TimeRange {
    pub fn new(min: i64, max: i64) -> Self {
        Self { min, max }
    }

    pub fn overlaps(&self, min: i64, max: i64) -> bool {
        self.min <= max && self.max >= min
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
    fn path(&self) -> &str;

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
    async fn read_entries(&mut self, key: &[u8], entries: &mut Vec<IndexEntry>);

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
