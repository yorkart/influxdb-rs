use std::fmt::{Display, Formatter};

use bytes::BufMut;
use influxdb_utils::time::unix_nano_to_time;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::engine::tsm1::file_store::INDEX_ENTRY_SIZE;

/// IndexEntry is the index information for a given block in a TSM file.
pub struct IndexEntry {
    /// The min and max time of all points stored in the block.
    pub min_time: i64,
    pub max_time: i64,

    /// The absolute position in the file where this block is located.
    pub offset: u64,

    /// The size in bytes of the block in the file.
    pub size: u32,
}

impl IndexEntry {
    pub fn new(min_time: i64, max_time: i64, offset: u64, size: u32) -> Self {
        Self {
            min_time,
            max_time,
            offset,
            size,
        }
    }

    /// unmarshal_binary decodes an IndexEntry from a byte slice.
    pub fn unmarshal_binary(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < INDEX_ENTRY_SIZE {
            return Err(anyhow!(
                "unmarshalBinary: short buf: {} < {}",
                b.len(),
                INDEX_ENTRY_SIZE
            ));
        }

        let min_time = u64::from_be_bytes(b[..8].try_into().unwrap()) as i64; //  int64(binary.BigEndian.Uint64(b[:8]))
        let max_time = u64::from_be_bytes(b[8..16].try_into().unwrap()) as i64; // int64(binary.BigEndian.Uint64(b[8:16]))
        let offset = u64::from_be_bytes(b[16..24].try_into().unwrap()); //int64(binary.BigEndian.Uint64(b[16:24]))
        let size = u32::from_be_bytes(b[24..28].try_into().unwrap()); //binary.BigEndian.Uint32(b[24:28])

        Ok(Self {
            min_time,
            max_time,
            offset,
            size,
        })
    }

    /// append_to writes a binary-encoded version of IndexEntry to b, allocating
    /// and returning a new slice, if necessary.
    pub fn append_to(&self, b: &mut Vec<u8>) {
        b.put_u64(self.min_time as u64);
        b.put_u64(self.max_time as u64);
        b.put_u64(self.offset as u64);
        b.put_u32(self.size);
    }

    /// contains returns true if this IndexEntry may contain values for the given time.
    /// The min and max times are inclusive.
    pub fn contains(&self, t: i64) -> bool {
        self.min_time <= t && self.max_time >= t
    }

    /// overlaps_time_range returns true if the given time ranges are completely within the entry's time bounds.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min_time <= max && self.max_time >= min
    }
}

impl Display for IndexEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "min={} max={} ofs={} siz={}",
            unix_nano_to_time(self.min_time),
            unix_nano_to_time(self.max_time),
            self.offset,
            self.size,
        )
    }
}

#[derive(Default)]
pub(crate) struct IndexEntries {
    pub typ: u8,
    pub entries: Vec<IndexEntry>,
}

impl IndexEntries {
    pub fn new(typ: u8) -> Self {
        Self {
            typ,
            entries: vec![],
        }
    }

    pub fn set_block_type(&mut self, typ: u8) {
        self.typ = typ;
    }

    pub fn clear_with_cap(&mut self, cap: usize) {
        if self.entries.capacity() < cap {
            self.entries.reserve_exact(cap - self.entries.len());
        }
        self.entries.clear();
    }

    pub fn push(&mut self, entry: IndexEntry) {
        self.entries.push(entry);
    }

    pub fn marshal_binary(&self) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.entries.len() * INDEX_ENTRY_SIZE);

        for entry in &self.entries {
            entry.append_to(&mut buf);
        }

        Ok(buf)
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, mut w: W) -> anyhow::Result<u64> {
        let mut buf = Vec::with_capacity(INDEX_ENTRY_SIZE);
        let mut total = 0;

        for entry in &self.entries {
            buf.clear();
            entry.append_to(&mut buf);
            let n = w.write(buf.as_slice()).await.map_err(|e| anyhow!(e))?;
            total += n as u64;
        }

        Ok(total)
    }

    pub fn sort(&mut self) {
        self.entries.sort_by_key(|x| x.min_time)
    }
}
