use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;

use influxdb_storage::StorageOperator;
use influxdb_utils::hash::{distance, hash_key};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::common::Section;
use crate::series::series_segment::{
    read_series_key_from_segments, SeriesEntry, SeriesEntryFlag, SeriesOffset, SeriesSegment,
    SERIES_ENTRY_HEADER_SIZE,
};

const SERIES_INDEX_VERSION: u8 = 1;
const SERIES_INDEX_MAGIC: &'static str = "SIDX";

/// offset + id
const SERIES_INDEX_ELEM_SIZE: u32 = 16;
// /// rhh load factor
// const SERIES_INDEX_LOAD_FACTOR: u32 = 90;

// const SERIES_INDEX_HEADER_SIZE: u32 = 0 +
//     4 + 1 + // magic + version
//     8 + 8 + // max series + max offset
//     8 + 8 + // count + capacity
//     8 + 8 + // key/id map offset & size
//     8 + 8 + // id/offset map offset & size
//     0;

///SeriesIndexHeader represents the header of a series index.
#[derive(Default)]
pub struct SeriesIndexHeader {
    version: u8,

    max_series_id: u64,
    max_offset: SeriesOffset,

    count: u64,
    capacity: u64,

    key_id_map: Section,
    id_offset_map: Section,
}

impl SeriesIndexHeader {
    pub fn new() -> Self {
        let mut hdr = SeriesIndexHeader::default();
        hdr.version = SERIES_INDEX_VERSION;
        hdr
    }

    pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
        w.write(SERIES_INDEX_MAGIC.as_bytes()).await?;
        w.write_u8(self.version).await?;
        w.write_u64(self.max_series_id).await?;
        w.write_u64(self.max_offset.0).await?;
        w.write_u64(self.count).await?;
        w.write_u64(self.capacity).await?;
        self.key_id_map.write_to(&mut w).await?;
        self.id_offset_map.write_to(&mut w).await?;

        Ok(())
    }

    pub async fn read_from<R: AsyncRead + AsyncSeek + Send + Unpin>(
        r: &mut R,
    ) -> anyhow::Result<(Self, usize)> {
        let mut i = 0;

        // Read magic number.
        let mut magic = [0_u8; 4];
        let n = r.read(magic.as_mut()).await?;
        if !SERIES_INDEX_MAGIC.as_bytes().cmp(magic.as_slice()).is_eq() {
            return Err(anyhow!("invalid series index"));
        }
        i += n;

        // Read version.
        let version = r.read_u8().await?;
        i += 1;

        // Read max series id.
        let max_series_id = r.read_u64().await?;
        i += 8;

        // Read max offset.
        let max_offset = r.read_u64().await?;
        i += 8;

        // Read count.
        let count = r.read_u64().await?;
        i += 8;

        // Read capacity.
        let capacity = r.read_u64().await?;
        i += 8;

        // Read key/id map position.
        let (key_id_map, len) = Section::read_from(r).await?;
        i += len;

        // Read offset/id map position.
        let (id_offset_map, len) = Section::read_from(r).await?;
        i += len;

        Ok((
            Self {
                version,
                max_series_id,
                max_offset: SeriesOffset(max_offset),
                count,
                capacity,
                key_id_map,
                id_offset_map,
            },
            i,
        ))
    }
}

/// SeriesIndex represents an index of key-to-id & id-to-offset mappings.
pub struct SeriesIndex {
    op: StorageOperator,

    hdr: SeriesIndexHeader,

    /// In-memory data since rebuild.
    /// map: key -> segment_id
    key_id_map: HashMap<Vec<u8>, u64>,
    /// map: segment_id -> offset
    id_offset_map: HashMap<u64, SeriesOffset>,
    /// set: segment_id
    tombstones: HashSet<u64>,
}

impl SeriesIndex {
    pub async fn new(op: StorageOperator) -> anyhow::Result<Self> {
        let hdr = if op.exist().await? {
            let mut reader = op.reader().await?;
            let (hdr, _) = SeriesIndexHeader::read_from(&mut reader).await?;
            hdr
        } else {
            SeriesIndexHeader::new()
        };

        Ok(Self {
            op,
            hdr,
            key_id_map: HashMap::new(),
            id_offset_map: HashMap::new(),
            tombstones: HashSet::new(),
        })
    }

    /// count returns the number of series in the index.
    pub fn count(&self) -> u64 {
        self.on_disk_count() + self.in_mem_count()
    }

    /// on_disk_count returns the number of series in the on-disk index.
    pub fn on_disk_count(&self) -> u64 {
        self.hdr.count
    }

    /// in_mem_count returns the number of series in the in-memory index.
    pub fn in_mem_count(&self) -> u64 {
        self.id_offset_map.len() as u64
    }

    pub async fn id_delete(&self, series_id: u64) -> anyhow::Result<bool> {
        if self.tombstones.contains(&series_id) {
            return Ok(true);
        }

        let offset = self.find_offset_by_id(series_id).await?;
        Ok(offset.is_none())
    }

    pub fn exec_entry(&mut self, entry: SeriesEntry, series_offset: SeriesOffset) {
        let SeriesEntry { flag, id } = entry;
        match flag {
            SeriesEntryFlag::InsertFlag(key) => {
                self.key_id_map.insert(key, id);
                self.id_offset_map.insert(id, series_offset);

                if id > self.hdr.max_series_id {
                    self.hdr.max_series_id = id;
                }
                if series_offset > self.hdr.max_offset {
                    self.hdr.max_offset = series_offset;
                }
            }
            SeriesEntryFlag::TombstoneFlag => {
                self.tombstones.insert(id);
            }
        }
    }

    pub async fn find_id_by_series_key(
        &self,
        segments: &[SeriesSegment],
        key: &[u8],
    ) -> anyhow::Result<u64> {
        if let Some(v) = self.key_id_map.get(key) {
            let id = *v;
            if id != 0 && !self.id_delete(id).await? {
                return Ok(id);
            }
        }

        let mask = self.hdr.capacity - 1;
        let hash = hash_key(key);

        let mut reader = self.op.reader().await?;

        let mut d = 0_u64;
        let mut pos = hash & mask;

        loop {
            let offset = self.hdr.key_id_map.offset + pos * SERIES_INDEX_ELEM_SIZE as u64;
            reader.seek(SeekFrom::Start(offset)).await?;

            let elem_offset = reader.read_u64().await?;
            if elem_offset == 0 {
                return Ok(0);
            }

            // todo memory optimize
            let elem_key = read_series_key_from_segments(
                segments,
                elem_offset + SERIES_ENTRY_HEADER_SIZE as u64,
            )
            .await?;
            if elem_key.is_none() {
                return Ok(0);
            }
            let elem_key = elem_key.unwrap();

            let elem_hash = hash_key(elem_key.as_slice());
            if d > distance(elem_hash, pos as usize, self.hdr.capacity) {
                return Ok(0);
            } else if elem_hash == hash && elem_key.as_slice().eq(key) {
                let series_id = reader.read_u64().await?;
                if self.id_delete(series_id).await? {
                    return Ok(0);
                }
                return Ok(series_id);
            }

            d += 1;
            pos = (pos + 1) & mask;
        }
    }

    pub async fn find_offset_by_id(&self, series_id: u64) -> anyhow::Result<Option<SeriesOffset>> {
        if let Some(series_offset) = self.id_offset_map.get(&series_id) {
            return Ok(Some(*series_offset));
        }

        let mask = self.hdr.capacity - 1;
        let hash = hash_key(series_id.to_be_bytes().as_slice());

        let mut reader = self.op.reader().await?;

        let mut d = 0_u64;
        let mut pos = hash & mask; // same hash % self.hdr.capacity
        loop {
            let offset = self.hdr.id_offset_map.offset + pos * SERIES_INDEX_ELEM_SIZE as u64;
            reader.seek(SeekFrom::Start(offset)).await?;

            let element_id = reader.read_u64().await?;
            if element_id == series_id {
                let offset = reader.read_u64().await?;
                return Ok(Some(SeriesOffset(offset)));
            } else {
                if element_id == 0 {
                    return Ok(None);
                }

                let hash = hash_key(element_id.to_be_bytes().as_slice());
                if d > distance(hash, pos as usize, self.hdr.capacity) {
                    return Ok(None);
                }
            }

            d += 1;
            pos = (pos + 1) & mask;
        }
    }
}
