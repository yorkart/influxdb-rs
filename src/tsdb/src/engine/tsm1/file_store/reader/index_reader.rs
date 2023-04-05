use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;

use influxdb_storage::{RandomAccessFile, RandomAccessFileExt};
use tokio::sync::RwLock;

use crate::engine::tsm1::file_store::index::{IndexEntries, IndexEntry};
use crate::engine::tsm1::file_store::reader::tsm_reader::TimeRange;
use crate::engine::tsm1::file_store::{INDEX_COUNT_SIZE, INDEX_ENTRY_SIZE};

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
    fn contains_key(&self, key: &[u8]) -> bool;

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
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<u64>;

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    fn overlaps_time_range(&self, min: i64, max: i64) -> bool;

    /// overlaps_key_range returns true if the min and max keys of the file overlap the arguments min and max.
    fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool;

    /// size returns the size of the current index in bytes.
    fn size(&self) -> u32;

    /// time_range returns the min and max time across all keys in the file.
    fn time_range(&self) -> TimeRange;

    /// tombstone_range returns ranges of time that are deleted for the given key.
    async fn tombstone_range(&self, key: &[u8]) -> &[TimeRange];

    /// key_range returns the min and max keys in the file.
    fn key_range(&self) -> (&[u8], &[u8]);

    /// Type returns the block type of the values stored for the key.  Returns one of
    /// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
    /// an error is returned.
    async fn block_type(&self, key: &[u8]) -> anyhow::Result<u8>;

    /// close closes the index and releases any resources.
    async fn close(&mut self) -> anyhow::Result<()>;
}

/// IndirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
/// implementation can be used for indexes that may be MMAPed into memory.
pub(crate) struct IndirectIndex {
    accessor: Box<dyn RandomAccessFile>,
    index_offset: u64,
    index_len: u32,

    /// offsets contains the positions in b for each key.  It points to the 2 byte length of
    /// key.
    offsets: RwLock<Vec<u64>>,

    /// min_key, max_key are the minimum and maximum (lexicographically sorted) contained in the
    /// file
    min_key: Vec<u8>,
    max_key: Vec<u8>,

    /// min_time, max_time are the minimum and maximum times contained in the file across all
    /// series.
    min_time: i64,
    max_time: i64,

    /// tombstones contains only the tombstoned keys with subset of time values deleted.  An
    /// entry would exist here if a subset of the points for a key were deleted and the file
    /// had not be re-compacted to remove the points on disk.
    tombstones: HashMap<String, Vec<TimeRange>>,
}

impl IndirectIndex {
    pub async fn new(
        accessor: Box<dyn RandomAccessFile>,
        index_offset: u64,
        index_len: u32,
    ) -> anyhow::Result<Self> {
        if index_len == 0 {
            return Err(anyhow!("no index found"));
        }

        let mut min_time: i64 = i64::MAX;
        let mut max_time = 0_i64;

        // To create our "indirect" index, we need to find the location of all the keys in
        // the raw byte slice.  The keys are listed once each (in sorted order).  Following
        // each key is a time ordered list of index entry blocks for that key.  The loop below
        // basically skips across the slice keeping track of the counter when we are at a key
        // field.
        let mut i = index_offset;
        let mut offsets = Vec::new();
        let i_max = index_offset + index_len as u64;
        while i < i_max {
            offsets.push(i);

            // Skip to the start of the values
            // key length value (2) + type (1) + length of key
            if i + 2 >= i_max {
                return Err(anyhow!(
                    "indirectIndex: not enough data for key length value"
                ));
            }
            let key_len = accessor.read_u16(i).await.map_err(|e| anyhow!(e))?;
            i += 3 + key_len as u64;

            // count of index entries
            if i + INDEX_COUNT_SIZE as u64 >= i_max {
                return Err(anyhow!(
                    "indirectIndex: not enough data for index entries count"
                ));
            }
            let count = accessor.read_u16(i).await.map_err(|e| anyhow!(e))?;
            i += INDEX_COUNT_SIZE as u64;

            // Find the min time for the block
            // first entry's min_time
            if i + 8 >= i_max {
                return Err(anyhow!("indirectIndex: not enough data for min time"));
            }
            let min_t = accessor.read_u64(i).await.map_err(|e| anyhow!(e))? as i64;
            if min_t < min_time {
                min_time = min_t;
            }

            i += (count as u64 - 1) * (INDEX_ENTRY_SIZE as u64);

            // Find the max time for the block
            // latest entry's max_time
            if i + 16 >= i_max {
                return Err(anyhow!("indirectIndex: not enough data for max time"));
            }
            let max_t = accessor.read_u64(i + 8).await.map_err(|e| anyhow!(e))? as i64;
            if max_t > max_time {
                max_time = max_t
            }

            i += INDEX_ENTRY_SIZE as u64;
        }

        let first_ofs = offsets[0];
        let (_, min_key) = read_key(&accessor, first_ofs)
            .await
            .map_err(|e| anyhow!(e))?;

        let last_ofs = offsets[offsets.len() - 1];
        let (_, max_key) = read_key(&accessor, last_ofs)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Self {
            accessor,
            index_offset,
            index_len,
            offsets: RwLock::new(offsets),
            min_key,
            max_key,
            min_time,
            max_time,
            tombstones: Default::default(),
        })
    }

    async fn binary_search(&self, offsets: &[u64], key: &[u8]) -> io::Result<isize> {
        let size = offsets.len();
        let mut left = 0;
        let mut right = size;

        let mut key_buf: Vec<u8> = vec![];
        while left < right {
            let mid = left + size / 2;

            let cmp = {
                let offset = offsets[mid];
                let key_len = self.accessor.read_u16(offset).await? as usize;
                if key_buf.len() < key_len {
                    key_buf.resize(key_len, 0_u8);
                } else if key_buf.len() > key_len {
                    key_buf.truncate(key_len);
                }
                self.accessor.read(offset + 2, &mut key_buf).await?;

                key_buf.as_slice().cmp(key)
            };

            if cmp == Ordering::Less {
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                right = mid;
            } else {
                return Ok(mid as isize);
            }
        }

        Ok(left as isize * -1)
    }

    /// search_offset searches the offsets slice for key and returns the position in
    /// offsets where key would exist.
    async fn search_offset(&self, key: &[u8]) -> anyhow::Result<u64> {
        let offsets = self.offsets.read().await;
        let i = self
            .binary_search(offsets.as_slice(), key)
            .await
            .map_err(|e| anyhow!(e))?;
        if i < 0 {
            return Err(anyhow!("key `{:?}` not found", key));
        }

        Ok(offsets[i as usize])
    }
}

#[async_trait]
impl TSMIndex for IndirectIndex {
    async fn delete(&mut self, keys: &[&[u8]]) {
        todo!()
    }

    async fn delete_range(&mut self, keys: &[&[u8]], min_time: i64, max_time: i64) {
        todo!()
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        key.cmp(self.min_key.as_slice()).is_ge() && key.cmp(self.max_key.as_slice()).is_le()
    }

    async fn contains(&self, key: &[u8]) -> bool {
        todo!()
    }

    async fn contains_value(&self, key: &[u8], timestamp: i64) -> bool {
        todo!()
    }

    async fn read_entries(&mut self, key: &[u8], entries: &mut Vec<IndexEntry>) {
        todo!()
    }

    async fn entry(&mut self, key: &[u8], timestamp: i64) -> IndexEntry {
        todo!()
    }

    async fn key(&mut self, index: usize, entries: &mut Vec<IndexEntry>) -> (&[u8], u8) {
        let offsets = self.offsets.read().await;
    }

    async fn key_at(&mut self, index: usize) -> (&[u8], u8) {
        todo!()
    }

    async fn key_count(&self) -> usize {
        let offsets = self.offsets.read().await;
        offsets.len()
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<u64> {
        self.search_offset(key).await
    }

    fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min_time <= max && self.max_time >= min
    }

    fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
        self.min_key.as_slice().cmp(max).is_le() && self.max_key.as_slice().cmp(min).is_ge()
    }

    fn size(&self) -> u32 {
        self.index_len
    }

    fn time_range(&self) -> TimeRange {
        TimeRange::new(self.min_time, self.max_time)
    }

    async fn tombstone_range(&self, key: &[u8]) -> &[TimeRange] {
        todo!()
    }

    fn key_range(&self) -> (&[u8], &[u8]) {
        (self.min_key.as_slice(), self.max_key.as_slice())
    }

    async fn block_type(&self, key: &[u8]) -> anyhow::Result<u8> {
        let offset = self.search_offset(key).await?;
        let (n, _key) = read_key(&self.accessor, offset).await?;

        let typ = self
            .accessor
            .read_u8(offset + n as u64)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(typ)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}

async fn read_key(
    accessor: &Box<dyn RandomAccessFile>,
    index_offset: u64,
) -> io::Result<(u16, Vec<u8>)> {
    let key_len = accessor.read_u16(index_offset).await?;

    let mut key = Vec::with_capacity(key_len as usize);
    let _ = accessor.read(index_offset + 2, &mut key).await?;

    Ok((key_len + 2, key))
}

async fn read_entries(
    accessor: &Box<dyn RandomAccessFile>,
    mut offset: u64,
    max_offset: u64,
    entries: &mut IndexEntries,
) -> anyhow::Result<u64> {
    if max_offset - offset < (1 + INDEX_COUNT_SIZE) as u64 {
        return Err(anyhow!("readEntries: data too short for headers"));
    }

    // 1 byte block type
    let typ = accessor.read_u8(offset).await.map_err(|e| anyhow!(e))?;
    entries.set_block_type(typ);
    offset += 1;

    // 2 byte count of index entries
    let count = accessor.read_u16(offset).await.map_err(|e| anyhow!(e))? as usize;
    offset += 2;

    entries.clear_with_cap(count);

    let mut entry_buf = [0_u8; INDEX_ENTRY_SIZE];
    for _ in 0..count {
        accessor
            .read(offset, &mut entry_buf)
            .await
            .map_err(|e| anyhow!(e))?;
        offset += INDEX_ENTRY_SIZE as u64;

        let entry = IndexEntry::unmarshal_binary(&entry_buf)?;
        entries.push(entry);
    }

    Ok(offset)
}
