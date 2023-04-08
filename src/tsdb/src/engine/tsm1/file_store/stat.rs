/// FileStat holds information about a TSM file on disk.
pub struct FileStat {
    path: String,
    has_tombstone: bool,
    size: u32,
    last_modified: i64,

    min_time: i64,
    max_time: i64,

    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

impl FileStat {
    pub fn new(
        path: String,
        has_tombstone: bool,
        size: u32,
        last_modified: i64,
        min_time: i64,
        max_time: i64,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
    ) -> Self {
        Self {
            path,
            has_tombstone,
            size,
            last_modified,
            min_time,
            max_time,
            min_key,
            max_key,
        }
    }

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min_time <= max && self.max_time >= min
    }

    /// overlaps_key_range returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
        min.len() != 0
            && max.len() != 0
            && self.min_key.as_slice().cmp(max).is_le()
            && self.max_key.as_slice().cmp(min).is_ge()
    }

    /// contains_key returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.min_key.as_slice().cmp(key).is_ge() || key.cmp(self.max_key.as_slice()).is_le()
    }
}

/// FileStoreStatistics keeps statistics about the file store.
pub struct FileStoreStatistics {
    disk_bytes: i64,
    file_count: i64,
}
