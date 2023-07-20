use common_base::iterator::{AsyncIterator, AsyncIterators};
use futures::TryStreamExt;
use influxdb_storage::{path_join, StorageOperator};
use tokio::sync::RwLock;

use crate::series::series_file::SERIES_FILE_PARTITION_N;
use crate::series::series_index::SeriesIndex;
use crate::series::series_segment::{
    parse_series_segment_filename, SeriesEntry, SeriesEntryFlag, SeriesOffset, SeriesSegment,
};

/// DEFAULT_SERIES_PARTITION_COMPACT_THRESHOLD is the number of series IDs to hold in the in-memory
/// series map before compacting and rebuilding the on-disk representation.
const DEFAULT_SERIES_PARTITION_COMPACT_THRESHOLD: usize = 1 << 17; // 128K

struct KeyRange {
    entry: SeriesEntry,
    offset: SeriesOffset,
}

impl KeyRange {
    pub fn new(entry: SeriesEntry, offset: SeriesOffset) -> Self {
        Self { entry, offset }
    }
}

struct SeriesPartitionInner {
    id: u16,
    op: StorageOperator,
    segments: Vec<SeriesSegment>,
    index: SeriesIndex,
    seq: u64, // series id sequence
}

impl SeriesPartitionInner {
    pub fn new(
        id: u16,
        op: StorageOperator,
        segments: Vec<SeriesSegment>,
        index: SeriesIndex,
        seq: u64,
    ) -> Self {
        Self {
            id,
            op,
            segments,
            index,
            seq,
        }
    }

    /// active_segment returns the last segment.
    fn active_segment(&self) -> &SeriesSegment {
        &self.segments[self.segments.len() - 1]
    }

    /// active_segment returns the last segment as mut.
    fn active_segment_mut(&mut self) -> &mut SeriesSegment {
        let active = self.segments.len() - 1;
        &mut self.segments[active]
    }

    /// file_size returns the size of all partitions, in bytes.
    pub async fn file_size(&self) -> u64 {
        let mut n = 0_u64;
        for segment in &self.segments {
            n += segment.size() as u64;
        }
        n
    }

    pub async fn find_series(
        &self,
        keys: &[&[u8]],
        key_partition_ids: &[u16],
        ids: &mut [u64],
    ) -> anyhow::Result<bool> {
        let mut write_required = false;
        for i in 0..keys.len() {
            if key_partition_ids[i] != self.id {
                continue;
            }

            let id = self
                .index
                .find_id_by_series_key(self.segments.as_slice(), keys[i])
                .await?;
            if id == 0 {
                write_required = true;
            } else {
                ids[i] = id;
            }
        }

        Ok(write_required)
    }

    pub async fn insert_series(
        &mut self,
        keys: &[&[u8]],
        key_partition_ids: &[u16],
        ids: &mut [u64],
    ) -> anyhow::Result<()> {
        let mut new_key_ranges = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            if key_partition_ids[i] != self.id || ids[i] != 0 {
                continue;
            }

            // Re-attempt lookup under write lock.
            let key = keys[i];
            let id = self
                .index
                .find_id_by_series_key(self.segments.as_slice(), key)
                .await?;
            if id != 0 {
                ids[i] = id;
                continue;
            }

            // Write to series log and save offset.
            let key_range = self.insert(key).await?;
            ids[i] = key_range.entry.id;
            new_key_ranges.push(key_range);
        }

        // Flush active segment writes so we can access data.
        self.active_segment_mut().flush().await?;

        // Add keys to hash map(s).
        for key_range in new_key_ranges {
            let KeyRange { entry, offset } = key_range;
            self.index.exec_entry(entry, offset);
        }

        // Check if we've crossed the compaction threshold.

        Ok(())
    }

    async fn insert(&mut self, key: &[u8]) -> anyhow::Result<KeyRange> {
        let id = self.seq;
        let entry = SeriesEntry::new(SeriesEntryFlag::InsertFlag(key.to_vec()), id);
        let offset = self.write_log_entry(&entry).await?;

        self.seq += SERIES_FILE_PARTITION_N as u64;
        Ok(KeyRange::new(entry, offset))
    }

    /// writeLogEntry appends an entry to the end of the active segment.
    /// If there is no more room in the segment then a new segment is added.
    async fn write_log_entry(&mut self, entry: &SeriesEntry) -> anyhow::Result<SeriesOffset> {
        let mut segment = self.active_segment_mut();
        if !segment.can_write(&entry) {
            self.create_segment().await?;
            segment = self.active_segment_mut();
        }

        segment.write_log_entry(&entry).await
    }

    async fn create_segment(&mut self) -> anyhow::Result<()> {
        // Close writer for active segment, if one exists.
        self.active_segment_mut().close_for_write().await?;

        // Generate a new sequential segment identifier.
        let id = self.active_segment().id() + 1;
        let filename = format!("{:04}", id);

        // Generate new empty segment.
        let segment_path = path_join(self.op.path(), filename.as_str());
        let mut segment = SeriesSegment::create(id, self.op.to_op(segment_path.as_str())).await?;
        segment.init_for_write().await?;
        self.segments.push(segment);

        Ok(())
    }

    /// delete_series_id flags a series as permanently deleted.
    /// If the series is reintroduced later then it must create a new id.
    pub async fn delete_series_id(&mut self, id: u64) -> anyhow::Result<()> {
        if self.index.id_delete(id).await? {
            return Ok(());
        }

        let entry = SeriesEntry::new(SeriesEntryFlag::TombstoneFlag, id);
        let offset = self.write_log_entry(&entry).await?;

        // Flush active segment write.
        let segment = self.active_segment_mut();
        segment.flush().await?;

        // Mark tombstone in memory.
        self.index.exec_entry(entry, offset);

        Ok(())
    }

    /// IsDeleted returns true if the ID has been deleted before.
    pub async fn is_delete(&self, id: u64) -> anyhow::Result<bool> {
        self.index.id_delete(id).await
    }

    /// series_key returns the series key for a given id.
    pub async fn series_key(&self, id: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let series_offset = self.index.find_offset_by_id(id).await?;
        if let Some(series_offset) = series_offset {
            let v = self.series_key_by_offset(series_offset).await?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    async fn series_key_by_offset(&self, series_offset: SeriesOffset) -> anyhow::Result<Vec<u8>> {
        let (segment_id, pos) = series_offset.split();
        for segment in &self.segments {
            if segment.id() != segment_id {
                continue;
            }

            let mut itr = segment.series_iterator(pos).await?;
            let (entry, _, _) = itr.try_next().await?.ok_or(anyhow!("key not found"))?;
            return entry.flag.into_key();
        }

        return Ok(vec![]);
    }

    /// find_id_by_series_key return the series id for the series key.
    pub async fn find_id_by_series_key(&self, key: &[u8]) -> anyhow::Result<u64> {
        self.index.find_id_by_series_key(&self.segments, key).await
    }

    /// series_count returns the number of series.
    pub fn series_count(&self) -> u64 {
        self.index.count()
    }

    /// series_iterator returns a list of all series ids.
    pub async fn series_iterator(&self) -> anyhow::Result<impl AsyncIterator> {
        let mut itrs = Vec::with_capacity(self.segments.len());
        for segment in &self.segments {
            itrs.push(segment.series_iterator(0).await?);
        }

        Ok(AsyncIterators::new(itrs))
    }
}

/// SeriesPartition represents a subset of series file data.
pub struct SeriesPartition {
    id: u16,
    op: StorageOperator,

    inner: RwLock<SeriesPartitionInner>,

    seq: u64, // series id sequence
}

impl SeriesPartition {
    pub async fn new(id: u16, op: StorageOperator) -> anyhow::Result<Self> {
        op.create_dir().await?;

        // open all segments
        let (segments, seq) = Self::open_segments(id, op.clone()).await?;

        // Init last segment for writes.
        // noop

        // open index
        let index_path = path_join(op.path(), "index");
        let index = SeriesIndex::new(op.to_op(index_path.as_str())).await?;

        Ok(Self {
            id,
            op: op.clone(),
            inner: RwLock::new(SeriesPartitionInner::new(id, op, segments, index, seq)),
            seq,
        })
    }

    async fn open_segments(
        partition_id: u16,
        op: StorageOperator,
    ) -> anyhow::Result<(Vec<SeriesSegment>, u64)> {
        let mut segments = Vec::new();

        let mut lister = op.list().await?;
        while let Some(de) = lister.try_next().await? {
            if let Ok(segment_id) = parse_series_segment_filename(de.name()) {
                let segment = SeriesSegment::open(segment_id, op.to_op(de.path()), true).await?;
                segments.push(segment);
            }
        }

        segments.sort_by_key(|x| x.id());

        let mut seq = (partition_id + 1) as u64;
        // Find max series id by searching segments in reverse order.
        for segment in segments.iter().rev() {
            let max_series_id = segment.max_series_id().await?;
            if max_series_id >= seq {
                // Reset our sequence num to the next one to assign
                seq = max_series_id + SERIES_FILE_PARTITION_N as u64;
                break;
            }
        }

        // Create initial segment if none exist.
        if segments.len() == 0 {
            let op = op.to_op(path_join(op.path(), "0000").as_str());
            let segment = SeriesSegment::create(0, op).await?;
            segments.push(segment);
        }

        let active = segments.len() - 1;
        (&mut segments[active]).init_for_write().await?;
        Ok((segments, seq))
    }

    /// id returns the partition id.
    pub fn id(&self) -> u16 {
        self.id
    }

    /// file_size returns the size of all partitions, in bytes.
    pub async fn file_size(&self) -> u64 {
        let inner = self.inner.read().await;
        inner.file_size().await
    }

    /// create_series_list_if_not_exists creates a list of series in bulk if they don't exist.
    /// The ids parameter is modified to contain series IDs for all keys belonging to this partition.
    pub async fn create_series_list_if_not_exists(
        &self,
        keys: &[&[u8]],
        key_partition_ids: &[u16],
        ids: &mut [u64],
    ) -> anyhow::Result<()> {
        let write_required = {
            let inner = self.inner.read().await;
            inner.find_series(keys, key_partition_ids, ids).await?
        };
        // Exit if all series for this partition already exist.
        if !write_required {
            return Ok(());
        }

        let mut inner = self.inner.write().await;
        inner.insert_series(keys, key_partition_ids, ids).await
    }

    pub async fn iterator(&self) -> anyhow::Result<impl AsyncIterator> {
        let inner = self.inner.read().await;
        inner.series_iterator().await
    }
}

/// SeriesPartitionCompactor represents an object reindex a series partition
/// and optionally compacts segments.
pub struct SeriesPartitionCompactor {}
