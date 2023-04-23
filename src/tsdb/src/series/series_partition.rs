use futures::TryStreamExt;
use influxdb_storage::{path_join, StorageOperator};

use crate::series::series_file::SERIES_FILE_PARTITION_N;
use crate::series::series_index::SeriesIndex;
use crate::series::series_segment::{parse_series_segment_filename, SeriesSegment};

/// DEFAULT_SERIES_PARTITION_COMPACT_THRESHOLD is the number of series IDs to hold in the in-memory
/// series map before compacting and rebuilding the on-disk representation.
const DEFAULT_SERIES_PARTITION_COMPACT_THRESHOLD: usize = 1 << 17; // 128K

/// SeriesPartition represents a subset of series file data.
pub struct SeriesPartition {
    id: u16,
    op: StorageOperator,

    segments: Vec<SeriesSegment>,
    index: SeriesIndex,
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
            op,
            segments,
            index,
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
                let segment = SeriesSegment::new(segment_id, op.to_op(de.path())).await?;
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

        Ok((segments, seq))
    }
}
