use std::io::{Cursor, SeekFrom};

use bytes::Buf;
use influxdb_storage::opendal::Reader;
use influxdb_storage::opendal::Writer;
use influxdb_storage::StorageOperator;
use regex::Regex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::series::series_file::read_series_key1;

const TMP_FILE_SUFFIX: &'static str = ".initializing";

pub(crate) const SERIES_SEGMENT_VERSION: u8 = 1;
pub(crate) const SERIES_SEGMENT_MAGIC: &'static str = "SSEG";
pub(crate) const SERIES_SEGMENT_HEADER_SIZE: usize = 5;

pub(crate) const SERIES_ENTRY_HEADER_SIZE: usize = 1 + 8;

const SERIES_ENTRY_INSERT_FLAG: u8 = 0x01;
const SERIES_ENTRY_TOMBSTONE_FLAG: u8 = 0x02;

pub enum SeriesEntryFlag {
    InsertFlag(Vec<u8>),
    TombstoneFlag,
}

impl SeriesEntryFlag {
    pub fn flag(&self) -> u8 {
        match self {
            Self::InsertFlag(_) => SERIES_ENTRY_INSERT_FLAG,
            Self::TombstoneFlag => SERIES_ENTRY_TOMBSTONE_FLAG,
        }
    }

    pub fn into_key(self) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::InsertFlag(key) => Ok(key),
            Self::TombstoneFlag => Err(anyhow!("unsupported")),
        }
    }
}

pub struct SeriesEntry {
    pub(crate) flag: SeriesEntryFlag,
    pub(crate) id: u64,
}

impl SeriesEntry {
    pub fn new(flag: SeriesEntryFlag, id: u64) -> Self {
        Self { flag, id }
    }

    pub fn len(&self) -> usize {
        let key_len = match &self.flag {
            SeriesEntryFlag::InsertFlag(key) => key.len(),
            SeriesEntryFlag::TombstoneFlag => 0,
        };

        SERIES_ENTRY_HEADER_SIZE + key_len
    }

    pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
        let flag = self.flag.flag();
        w.write_u8(flag).await?;
        w.write_u64(self.id).await?;

        match &self.flag {
            SeriesEntryFlag::InsertFlag(key) => {
                w.write(key).await?;
            }
            SeriesEntryFlag::TombstoneFlag => {}
        };

        Ok(())
    }

    pub async fn read_from<R: AsyncRead + AsyncSeek + Send + Unpin>(
        mut r: R,
    ) -> anyhow::Result<(Self, usize)> {
        let mut n = 0;

        // If flag byte is zero then no more entries exist.
        let flag = r.read_u8().await?;
        n += 1;

        // series id
        let id = r.read_u64().await?;
        n += 8;

        let (flag, len) = match flag {
            SERIES_ENTRY_INSERT_FLAG => {
                let (key, len) = read_series_key1(r).await?;
                Ok((SeriesEntryFlag::InsertFlag(key), len))
            }
            SERIES_ENTRY_TOMBSTONE_FLAG => Ok((SeriesEntryFlag::TombstoneFlag, 0)),
            _ => Err(anyhow!("unknown series entry flag: {}", flag)),
        }?;
        n += len;

        Ok((Self::new(flag, id), n))
    }
}

/// SeriesSegmentHeader represents the header of a series segment.
pub struct SeriesSegmentHeader {
    version: u8,
}

impl SeriesSegmentHeader {
    pub fn new() -> Self {
        Self {
            version: SERIES_SEGMENT_VERSION,
        }
    }

    pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
        w.write(SERIES_SEGMENT_MAGIC.as_bytes()).await?;
        w.write_u8(self.version).await?;

        Ok(())
    }
}

impl<'a> TryFrom<&'a [u8]> for SeriesSegmentHeader {
    type Error = anyhow::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        if value.len() < SERIES_SEGMENT_HEADER_SIZE {
            return Err(anyhow!(""));
        }
        let magic = &value[..SERIES_SEGMENT_MAGIC.len()];
        if !magic.cmp(SERIES_SEGMENT_MAGIC.as_bytes()).is_eq() {
            return Err(anyhow!("invalid series segment"));
        }

        let mut cursor = Cursor::new(&value[SERIES_SEGMENT_MAGIC.len()..]);
        let version = cursor.get_u8();

        Ok(Self { version })
    }
}

pub struct SeriesSegment {
    id: u16,

    op: StorageOperator,
    writer: Option<Writer>,
    reader: Reader,
    write_offset: u32,
    max_file_size: u32,
}

impl SeriesSegment {
    pub async fn new(id: u16, op: StorageOperator) -> anyhow::Result<Self> {
        let mut reader = op.reader().await?;
        let file_size = op.stat().await?.content_length();

        let mut write_offset = SERIES_SEGMENT_HEADER_SIZE as u32;
        while write_offset < file_size as u32 {
            let (_entry, len) = SeriesEntry::read_from(&mut reader).await?;
            write_offset += len as u32;
        }

        // todo replace with writer seek and ignore this check
        if file_size != 0 && write_offset as u64 != file_size - 1 {
            return Err(anyhow!("file corruption: {}", op.path()));
        }

        let max_file_size = series_segment_size(id);
        Ok(Self {
            id,
            op,
            writer: None,
            reader,
            write_offset,
            max_file_size,
        })
    }

    pub async fn create(id: u16, op: StorageOperator) -> anyhow::Result<Self> {
        // Generate segment in temp location.
        let tmp_op = op.to_tmp(TMP_FILE_SUFFIX);
        {
            let mut writer = tmp_op.writer().await?;

            let hdr = SeriesSegmentHeader::new();
            hdr.write_to(&mut writer).await?;

            writer.close().await?;
        }
        op.rename(op.path()).await?;

        // todo truncate file: f.Truncate(int64(series_segment_size(id)))

        Self::new(id, op).await
    }

    /// InitForWrite initializes a write handle for the segment.
    /// This is only used for the last segment in the series file.
    pub async fn init_for_write(&mut self) -> anyhow::Result<()> {
        let writer = self.op.writer().await?;
        self.writer = Some(writer);
        Ok(())
    }

    pub async fn close_for_write(&mut self) -> anyhow::Result<()> {
        let writer = self.writer.take();
        if let Some(mut writer) = writer {
            writer.close().await?;
        }
        Ok(())
    }

    /// write_log_entry writes entry data into the segment.
    /// Returns the offset of the beginning of the entry.
    pub async fn write_log_entry(&mut self, entry: &SeriesEntry) -> anyhow::Result<u64> {
        if !self.can_write(entry) {
            return Err(anyhow!("series segment not writable"));
        }

        let offset = join_series_offset(self.id, self.write_offset);

        let writer = self.writer.as_mut().unwrap();
        entry.write_to(writer).await?;
        self.write_offset += entry.len() as u32;

        Ok(offset)
    }

    pub fn can_write(&self, entry: &SeriesEntry) -> bool {
        self.writer.is_some()
            && (self.write_offset as u64 + entry.len() as u64) < self.max_file_size as u64
    }

    /// Flush flushes the buffer to disk.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
        }
        Ok(())
    }

    pub async fn series_iterator(&self) -> anyhow::Result<SeriesEntryIterator> {
        let reader = self.op.reader().await?;
        let itr = SeriesEntryIterator::new(reader, self.write_offset, self.id).await?;
        Ok(itr)
    }

    /// append_series_ids appends all the segments ids to a slice. Returns the new slice.
    pub async fn series_ids(&mut self) -> anyhow::Result<Vec<u64>> {
        let mut itr = self.series_iterator().await?;

        let mut ids = Vec::new();
        while let Some((entry, _offset)) = itr.next().await? {
            ids.push(entry.id);
        }

        Ok(ids)
    }

    /// max_series_id returns the highest series id in the segment.
    pub async fn max_series_id(&self) -> anyhow::Result<u64> {
        let mut itr = self.series_iterator().await?;

        let mut max = 0;
        while let Some((entry, _offset)) = itr.next().await? {
            if let SeriesEntryFlag::InsertFlag(_) = &entry.flag {
                if entry.id > max {
                    max = entry.id;
                }
            }
        }

        Ok(max)
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn size(&self) -> u32 {
        self.write_offset
    }

    pub fn path(&self) -> &str {
        self.op.path()
    }
}

pub struct SeriesEntryIterator {
    reader: Reader,
    read_offset: u32,
    max_offset: u32,
    segment_id: u16,
}

impl SeriesEntryIterator {
    pub async fn new(mut reader: Reader, max_offset: u32, segment_id: u16) -> anyhow::Result<Self> {
        // skip header & header check
        let offset = SERIES_SEGMENT_HEADER_SIZE as u64;
        reader.seek(SeekFrom::Start(offset)).await?;
        Ok(Self {
            reader,
            read_offset: offset as u32,
            max_offset,
            segment_id,
        })
    }

    pub fn reset(&mut self, read_offset: u32) {
        self.read_offset = read_offset;
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<(SeriesEntry, u64)>> {
        let entry_offset = self.read_offset;
        if entry_offset >= self.max_offset {
            return Ok(None);
        }

        let (se, len) = SeriesEntry::read_from(&mut self.reader).await?;
        self.read_offset += len as u32;

        let offset = join_series_offset(self.segment_id, entry_offset as u32);
        Ok(Some((se, offset)))
    }
}

// /// https://stackoverflow.com/questions/65663021/how-to-call-an-async-function-in-poll-method
// impl Stream for SeriesEntryIterator {
//     type Item = anyhow::Result<(SeriesEntry, u64)>;
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let entry_offset = self.read_offset;
//         if entry_offset >= self.max_offset {
//             return Poll::Ready(None);
//         }
//
//         let mut f = SeriesEntry::read_from(&mut self.reader);
//         let f = Pin::new(&mut f);
//         let n = f.poll(cx);
//
//         match n {
//             Poll::Ready(r) => match r {
//                 Ok((se, len)) => {
//                     self.read_offset += len as u32;
//
//                     let offset = join_series_offset(self.segment_id, entry_offset as u32);
//                     Poll::Ready(Some(Ok((se, offset))))
//                 }
//                 Err(e) => Poll::Ready(Some(Err(e))),
//             },
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }

/// series_segment_size returns the maximum size of the segment.
/// The size goes up by powers of 2 starting from 4MB and reaching 256MB.
pub fn series_segment_size(id: u16) -> u32 {
    let min = 22; // 4MB
    let max = 28; // 256MB

    let mut shift = id + min;
    if shift >= max {
        shift = max
    }
    1 << shift
}

// join_series_offset returns an offset that combines the 2-byte segmentID and 4-byte pos.
pub fn join_series_offset(segment_id: u16, pos: u32) -> u64 {
    return ((segment_id as u64) << 32) | (pos as u64);
}

/// split_series_offset splits a offset into its 2-byte segmentID and 4-byte pos parts.
pub fn split_series_offset(offset: u64) -> (u16, u32) {
    ((offset >> 32 & 0xFFFF) as u16, (offset & 0xFFFFFFFF) as u32)
}

/// parse_series_segment_filename returns the id represented by the hexadecimal filename.
pub fn parse_series_segment_filename(filename: &str) -> anyhow::Result<u16> {
    let n: u16 = filename.parse::<u16>().map_err(|e| anyhow!(e))?;
    Ok(n)
}

/// is_valid_series_segment_filename returns true if filename is a 4-character lowercase hexadecimal number.
pub fn is_valid_series_segment_filename(filename: &str) -> bool {
    lazy_static! {
        static ref RE: Regex = Regex::new("^[0-9a-f]{4}$").unwrap();
    }

    return RE.is_match(filename);
}

/// find_segment returns a segment by id.
pub fn find_segment(a: &[SeriesSegment], id: u16) -> Option<&SeriesSegment> {
    for segment in a {
        if segment.id == id {
            return Some(segment);
        }
    }
    None
}

// read_series_key_from_segments returns a series key from an offset within a set of segments.
pub async fn read_series_key_from_segments(
    a: &[SeriesSegment],
    offset: u64,
) -> anyhow::Result<Option<Vec<u8>>> {
    let (segment_id, pos) = split_series_offset(offset);
    if let Some(segment) = find_segment(a, segment_id) {
        let mut itr = segment.series_iterator().await?;
        itr.reset(pos - SERIES_ENTRY_HEADER_SIZE as u32);
        if let Some((entry, _len)) = itr.next().await? {
            return match entry.flag {
                SeriesEntryFlag::InsertFlag(key) => Ok(Some(key)),
                SeriesEntryFlag::TombstoneFlag => Err(anyhow!("the position is tombstone")),
            };
        }
    }

    Ok(None)
}
