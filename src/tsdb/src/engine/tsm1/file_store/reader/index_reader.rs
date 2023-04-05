use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::reader::tsm_reader::TimeRange;
use tokio::sync::RwLock;

/// TSMIndex represent the index section of a TSM file.  The index records all
/// blocks, their locations, sizes, min and max times.
#[async_trait]
pub trait TSMIndex {
    /// delete removes the given keys from the index.
    async fn delete(&mut self, keys: &[&[u8]]);

    /// delete_range removes the given keys with data between min_time and max_time from the index.
    async fn delete_range(&mut self, keys: &[&[u8]], min_time: i64, max_time: i64);

    /// contains_key returns true if the given key may exist in the index.  This func is faster than
    /// contains but, may return false positives.
    async fn contains_key(&self, key: &[u8]) -> bool;

    /// contains return true if the given key exists in the index.
    async fn contains(&self, key: &[u8]) -> bool;

    /// contains_value returns true if key and time might exist in this file.  This function could
    /// return true even though the actual point does not exists.  For example, the key may
    /// exist in this file, but not have a point exactly at time t.
    async fn contains_value(&self, key: &[u8], timestamp: i64) -> bool;

    /// read_entries reads the index entries for key into entries.
    async fn read_entries(&mut self, key: &[u8], entries: &mut Vec<IndexEntry>);

    /// entry returns the index entry for the specified key and timestamp.  If no entry
    /// matches the key and timestamp, nil is returned.
    async fn entry(&mut self, key: &[u8], timestamp: i64) -> IndexEntry;

    /// key returns the key in the index at the given position, using entries to avoid allocations.
    async fn key(&mut self, index: usize, entries: &mut Vec<IndexEntry>) -> (&[u8], u8);

    /// key_at returns the key in the index at the given position.
    async fn key_at(&mut self, index: usize) -> (&[u8], u8);

    /// key_count returns the count of unique keys in the index.
    async fn key_count(&self) -> usize;

    /// seek returns the position in the index where key <= value in the index.
    async fn seek(&mut self, key: &[u8]) -> usize;

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    async fn overlaps_time_range(&self, min: i64, max: i64) -> bool;

    /// overlaps_key_range returns true if the min and max keys of the file overlap the arguments min and max.
    async fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool;

    /// size returns the size of the current index in bytes.
    async fn size(&self) -> u32;

    /// time_range returns the min and max time across all keys in the file.
    async fn time_range(&self) -> TimeRange;

    /// tombstone_range returns ranges of time that are deleted for the given key.
    async fn tombstone_range(&self, key: &[u8]) -> &[TimeRange];

    /// key_range returns the min and max keys in the file.
    async fn key_range(&self) -> (&[u8], &[u8]);

    /// Type returns the block type of the values stored for the key.  Returns one of
    /// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
    /// an error is returned.
    async fn block_type(&self, key: &[u8]) -> anyhow::Result<u8>;

    /// close closes the index and releases any resources.
    async fn close(&mut self) -> anyhow::Result<()>;
}

// IndirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
// implementation can be used for indexes that may be MMAPed into memory.
pub(crate) struct IndirectIndex {
    mu: RwLock<bool>,
}
