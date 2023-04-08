use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;

use influxdb_storage::{RandomAccess, RandomAccessExt};
use tokio::sync::RwLock;

use crate::engine::tsm1::file_store::index::{IndexEntries, IndexEntry};
use crate::engine::tsm1::file_store::reader::tsm_reader::TimeRange;
use crate::engine::tsm1::file_store::{INDEX_COUNT_SIZE, INDEX_ENTRY_SIZE};

const NIL_OFFSET: u64 = u64::MAX;

/// TSMIndex represent the index section of a TSM file.  The index records all
/// blocks, their locations, sizes, min and max times.
#[async_trait]
pub trait TSMIndex {
    /// delete removes the given keys from the index.
    async fn delete(&mut self, keys: &mut [&[u8]]) -> anyhow::Result<()>;

    /// delete_range removes the given keys with data between min_time and max_time from the index.
    async fn delete_range(
        &mut self,
        keys: &mut [&[u8]],
        min_time: i64,
        max_time: i64,
    ) -> anyhow::Result<()>;

    /// contains_key returns true if the given key may exist in the index.  This func is faster than
    /// contains but, may return false positives.
    fn contains_key(&self, key: &[u8]) -> bool;

    /// contains return true if the given key exists in the index.
    async fn contains(&self, key: &[u8]) -> anyhow::Result<bool>;

    /// contains_value returns true if key and time might exist in this file.  This function could
    /// return true even though the actual point does not exists.  For example, the key may
    /// exist in this file, but not have a point exactly at time t.
    async fn contains_value(&self, key: &[u8], timestamp: i64) -> anyhow::Result<bool>;

    /// entries reads the index entries for key into entries.
    async fn entries(&self, key: &[u8], entries: &mut IndexEntries) -> anyhow::Result<()>;

    /// entry returns the index entry for the specified key and timestamp.  If no entry
    /// matches the key and timestamp, nil is returned.
    async fn entry(&self, key: &[u8], timestamp: i64) -> anyhow::Result<Option<IndexEntry>>;

    /// key returns the key in the index at the given position, using entries to avoid allocations.
    async fn key(&self, index: usize, entries: &mut IndexEntries) -> anyhow::Result<Vec<u8>>;

    /// key_at returns the key in the index at the given position.
    async fn key_at(&mut self, index: usize) -> anyhow::Result<Option<(Vec<u8>, u8)>>;

    /// key_count returns the count of unique keys in the index.
    async fn key_count(&self) -> usize;

    /// seek returns the position in the index where key <= value in the index.
    async fn seek(&self, key: &[u8]) -> anyhow::Result<u64>;

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    fn overlaps_time_range(&self, min: i64, max: i64) -> bool;

    /// overlaps_key_range returns true if the min and max keys of the file overlap the arguments min and max.
    fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool;

    /// size returns the size of the current index in bytes.
    fn size(&self) -> u32;

    /// time_range returns the min and max time across all keys in the file.
    fn time_range(&self) -> TimeRange;

    // /// tombstone_range returns ranges of time that are deleted for the given key.
    // async fn tombstone_range(&self, key: &[u8]) -> &[TimeRange];

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
    accessor: Box<dyn RandomAccess>,
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
    ///
    /// Map<String, Vec<TimeRange>>
    tombstones: RwLock<HashMap<Vec<u8>, Vec<TimeRange>>>,
}

impl IndirectIndex {
    pub async fn new(
        accessor: Box<dyn RandomAccess>,
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
    async fn search_offset(&self, offsets: &[u64], key: &[u8]) -> anyhow::Result<Option<usize>> {
        if !self.contains_key(key) {
            return Ok(None);
        }

        let i = self
            .binary_search(offsets, key)
            .await
            .map_err(|e| anyhow!(e))?;
        if i < 0 {
            return Ok(None);
        }

        Ok(Some(i as usize))
    }
}

#[async_trait]
impl TSMIndex for IndirectIndex {
    async fn delete(&mut self, keys: &mut [&[u8]]) -> anyhow::Result<()> {
        if keys.len() == 0 {
            return Ok(());
        }

        keys.sort();

        // Both keys and offsets are sorted.  Walk both in order and skip
        // any keys that exist in both.
        let mut offsets = self.offsets.write().await;
        let start = {
            let start = self
                .binary_search(offsets.as_slice(), &keys[0])
                .await
                .map_err(|e| anyhow!(e))?;
            isize::abs(start) as usize
        };
        let mut key_index = 0;

        for i in start..offsets.len() {
            if key_index >= keys.len() {
                break;
            }

            let offset = offsets[i];
            let del_key = keys[key_index];

            let (_, key) = read_key(&self.accessor, offset)
                .await
                .map_err(|e| anyhow!(e))?;

            while key_index < keys.len() && del_key.cmp(key.as_slice()).is_lt() {
                key_index += 1;
            }

            if key_index < keys.len() && del_key.cmp(key.as_slice()).is_eq() {
                key_index += 1;
                offsets[i] = NIL_OFFSET;
            }
        }

        // pack
        let mut j = 0;
        for i in 0..offsets.len() {
            if offsets[i] == NIL_OFFSET {
                continue;
            } else {
                offsets[j] = offsets[i];
                j += 1;
            }
        }
        offsets.truncate(j);

        Ok(())
    }

    async fn delete_range(
        &mut self,
        keys: &mut [&[u8]],
        min_time: i64,
        max_time: i64,
    ) -> anyhow::Result<()> {
        if keys.len() == 0 {
            return Ok(());
        }

        keys.sort();

        // If we're deleting the max time range, just use tombstoning to remove the
        // key from the offsets slice
        if min_time == i64::MIN && max_time == i64::MAX {
            self.delete(keys).await?;
            return Ok(());
        }

        // Is the range passed in outside the time range for the file?
        let time_range = self.time_range();
        if min_time > time_range.max || max_time < time_range.min {
            return Ok(());
        }

        let mut full_keys = Vec::with_capacity(keys.len());
        let mut entries = IndexEntries::default();
        let mut key_index = 0;
        let key_count = self.key_count().await;

        for i in 0..key_count {
            if key_index >= keys.len() {
                break;
            }

            let k = self.key(i, &mut entries).await?;
            while key_index < keys.len() && keys[key_index].cmp(k.as_slice()).is_lt() {
                key_index += 1;
            }

            // No more keys to delete, we're done.
            if key_index >= keys.len() {
                break;
            }

            // If the current key is greater than the index one, continue to the next
            // index key.
            let del_key = keys[key_index];
            if del_key.cmp(k.as_slice()).is_gt() {
                continue;
            }

            // If multiple tombstones are saved for the same key
            if entries.entries.len() == 0 {
                continue;
            }

            // Is the time range passed outside the time range we've stored for this key?
            let time_range = entries.time_range();
            if min_time > time_range.max || max_time < time_range.min {
                continue;
            }

            // Does the range passed in cover every value for the key?
            if min_time < time_range.min && max_time >= time_range.max {
                full_keys.push(del_key);
                key_index += 1;
                continue;
            }

            // Append the new tombstones to the existing ones
            {
                let mut tombstones = self.tombstones.write().await;
                let existing = tombstones.entry(del_key.to_vec()).or_insert(vec![]);
                existing.push(TimeRange::new(min_time, max_time));

                // Sort the updated tombstones if necessary
                if existing.len() > 1 {
                    existing.sort_by(|a, b| {
                        if a.min == b.min && a.max <= b.max || a.min < b.min {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    });
                }
            };

            // We need to see if all the tombstones end up deleting the entire series.  This
            // could happen if there is one tombstone with min,max time spanning all the block
            // time ranges or from multiple smaller tombstones to delete segments.  To detect
            // this cases, we use a window starting at the first tombstone and grow it be each
            // tombstone that is immediately adjacent to the current window or if it overlaps.
            // If there are any gaps, we abort.
            // 检查所有的tombstones是否时连续的，如果连续的，计算出起始两个时间点，即min & max
            {
                let tombstones = self.tombstones.read().await;
                let new_ts = tombstones.get(del_key).unwrap();

                let mut min_ts = new_ts[0].min;
                let mut max_ts = new_ts[0].max;

                for j in 1..new_ts.len() {
                    let prev_ts = &new_ts[j - 1];
                    let ts = &new_ts[j];

                    // Make sure all the tombstone line up for a continuous range.  We don't
                    // want to have two small deletes on each edge's end up causing us to
                    // remove the full key.
                    if prev_ts.max != ts.min - 1 && !prev_ts.overlaps(ts) {
                        min_ts = i64::MAX;
                        max_ts = i64::MIN;
                        break;
                    }

                    if ts.min < min_ts {
                        min_ts = ts.min
                    }
                    if ts.max > max_ts {
                        max_ts = ts.max
                    }
                }

                // If we have a fully deleted series, delete it all of it.
                if min_ts <= time_range.min && max_ts >= time_range.max {
                    full_keys.push(del_key);
                    key_index += 1;
                    continue;
                }
            }
        }

        // Delete all the keys that fully deleted in bulk
        if full_keys.len() > 0 {
            self.delete(full_keys.as_mut_slice()).await?;
        }

        Ok(())
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        key.cmp(self.min_key.as_slice()).is_ge() && key.cmp(self.max_key.as_slice()).is_le()
    }

    async fn contains(&self, key: &[u8]) -> anyhow::Result<bool> {
        // let mut entries = IndexEntries::default();
        // self.entries(key, &mut entries).await?;
        // Ok(entries.entries.len() > 0)

        // optimization
        let offsets = self.offsets.read().await;
        let offset_index = self.search_offset(offsets.as_slice(), key).await?;
        Ok(offset_index.is_some())
    }

    async fn contains_value(&self, key: &[u8], timestamp: i64) -> anyhow::Result<bool> {
        let entry = self.entry(key, timestamp).await?;
        if entry.is_none() {
            return Ok(false);
        }

        let tombstones = self.tombstones.read().await;
        let tombstone = tombstones.get(key);
        if let Some(tombstone) = tombstone {
            for t in tombstone {
                if t.min <= timestamp && t.max >= timestamp {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn entries(&self, key: &[u8], entries: &mut IndexEntries) -> anyhow::Result<()> {
        let offsets = self.offsets.read().await;
        let offset_index = self.search_offset(offsets.as_slice(), key).await?;
        if let Some(index) = offset_index {
            let k = self.key(index, entries).await?;
            if !k.as_slice().cmp(key).is_eq() {
                return Err(anyhow!(
                    "key is inconsistency, expect: {:?}, found: {:?}",
                    key,
                    k.as_slice()
                ));
            }
        }

        Ok(())
    }

    // TODO optimization: 先读取完整entry集合，再时间过滤，复杂度较高
    async fn entry(&self, key: &[u8], timestamp: i64) -> anyhow::Result<Option<IndexEntry>> {
        let mut entries = IndexEntries::default();
        self.entries(key, &mut entries).await?;

        for entry in entries.entries {
            if entry.contains(timestamp) {
                return Ok(Some(entry));
            }
        }
        return Ok(None);
    }

    async fn key(&self, index: usize, entries: &mut IndexEntries) -> anyhow::Result<Vec<u8>> {
        let offsets = self.offsets.read().await;
        if index >= offsets.len() {
            return Err(anyhow!("offset's index out of bounds"));
        }

        let mut offset = offsets[index];
        let (n, key) = read_key(&self.accessor, offset)
            .await
            .map_err(|e| anyhow!(e))?;
        offset += n as u64;

        let _ = read_entries(
            &self.accessor,
            offset,
            self.index_offset + self.index_len as u64,
            entries,
        )
        .await?;

        Ok(key)
    }

    async fn key_at(&mut self, index: usize) -> anyhow::Result<Option<(Vec<u8>, u8)>> {
        let offsets = self.offsets.read().await;
        if index >= offsets.len() {
            return Ok(None);
        }

        let mut offset = offsets[index];
        let (n, key) = read_key(&self.accessor, offset)
            .await
            .map_err(|e| anyhow!(e))?;
        offset += n as u64;

        let typ = self
            .accessor
            .read_u8(offset)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Some((key, typ)))
    }

    async fn key_count(&self) -> usize {
        let offsets = self.offsets.read().await;
        offsets.len()
    }

    async fn seek(&self, key: &[u8]) -> anyhow::Result<u64> {
        let offsets = self.offsets.read().await;
        let offset_index = self
            .search_offset(offsets.as_slice(), key)
            .await?
            .ok_or(anyhow!("key not found"))?;
        Ok(offsets[offset_index])
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

    fn key_range(&self) -> (&[u8], &[u8]) {
        (self.min_key.as_slice(), self.max_key.as_slice())
    }

    async fn block_type(&self, key: &[u8]) -> anyhow::Result<u8> {
        let offsets = self.offsets.read().await;
        let offset_index = self
            .search_offset(offsets.as_slice(), key)
            .await?
            .ok_or(anyhow!("key not found"))?;
        let offset = offsets[offset_index];

        let (n, _key) = read_key(&self.accessor, offset).await?;

        let typ = self
            .accessor
            .read_u8(offset + n as u64)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(typ)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn read_key(
    accessor: &Box<dyn RandomAccess>,
    index_offset: u64,
) -> io::Result<(u16, Vec<u8>)> {
    let key_len = accessor.read_u16(index_offset).await?;

    let mut key = Vec::with_capacity(key_len as usize);
    let _ = accessor.read(index_offset + 2, &mut key).await?;

    Ok((key_len + 2, key))
}

async fn read_entries(
    accessor: &Box<dyn RandomAccess>,
    mut offset: u64,
    max_offset: u64,
    entries: &mut IndexEntries,
) -> anyhow::Result<u64> {
    // check space: | type(1B) | count(2B) |
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

// #[cfg(test)]
// mod tests {
//     use crate::engine::tsm1::block::BLOCK_FLOAT64;
//     use crate::engine::tsm1::file_store::index::IndexEntry;
//     use crate::engine::tsm1::file_store::writer::index_writer::{DirectIndex, IndexWriter};
//
//     fn must_make_index(keys: usize, blocks: usize) {
//         let mut index = DirectIndex::with_mem_buffer(1024 * 1024);
//         for i in 0..keys {
//             for j in 0..blocks {
//                 let s = format!("cpu-{%03d}", i);
//                 index.add(
//                     s.as_bytes(),
//                     BLOCK_FLOAT64,
//                     IndexEntry {
//                         min_time: (i * j * 2) as i64,
//                         max_time: (i * j * 2 + 1) as i64,
//                         offset: 10,
//                         size: 100,
//                     },
//                 );
//             }
//         }
//     }
// }
