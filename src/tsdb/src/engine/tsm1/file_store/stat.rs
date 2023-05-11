use crate::engine::tsm1::file_store::{KeyRange, TimeRange};

/// FileStat holds information about a TSM file on disk.
pub struct FileStat {
    path: String,
    has_tombstone: bool,
    size: u32,
    last_modified: i64,

    time_range: TimeRange,
    key_range: KeyRange,
}

impl FileStat {
    pub fn new(
        path: String,
        has_tombstone: bool,
        size: u32,
        last_modified: i64,
        time_range: TimeRange,
        key_range: KeyRange,
    ) -> Self {
        Self {
            path,
            has_tombstone,
            size,
            last_modified,
            time_range,
            key_range,
        }
    }

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.time_range.min <= max && self.time_range.max >= min
    }

    /// overlaps_key_range returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
        min.len() != 0
            && max.len() != 0
            && self.key_range.min.as_slice().cmp(max).is_le()
            && self.key_range.max.as_slice().cmp(min).is_ge()
    }

    /// contains_key returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.key_range.min.as_slice().cmp(key).is_ge()
            || key.cmp(self.key_range.max.as_slice()).is_le()
    }
}

/// FileStoreStatistics keeps statistics about the file store.
pub struct FileStoreStatistics {
    disk_bytes: i64,
    file_count: i64,
}
