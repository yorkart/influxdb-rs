use std::io::SeekFrom;

use influxdb_storage::DataOperator;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWriteExt};
use tokio::io::{AsyncSeekExt, AsyncWrite};

use crate::common::Section;

/// MEASUREMENT_BLOCK_VERSION is the version of the measurement block.
pub const MEASUREMENT_BLOCK_VERSION: u16 = 1;

/// Measurement flag constants.
pub const MEASUREMENT_TOMBSTONE_FLAG: u8 = 0x01;
pub const MEASUREMENT_SERIES_ID_SET_FLAG: u8 = 0x02;

// Measurement field size constants.

/// 1 byte offset for the block to ensure non-zero offsets.
pub const MEASUREMENT_FILL_SIZE: u8 = 1;

/// Measurement trailer fields
pub const MEASUREMENT_TRAILER_SIZE: usize = 0 +
    2 + // version
    8 + 8 + // data offset/size
    8 + 8 + // hash index offset/size
    8 + 8 + // measurement sketch offset/size
    8 + 8; // tombstone measurement sketch offset/size

/// Measurement key block fields.
pub const MEASUREMENT_N_SIZE: usize = 8;
pub const MEASUREMENT_OFFSET_SIZE: usize = 8;

pub const SERIES_ID_SIZE: usize = 8;

/// MeasurementBlockTrailer represents meta data at the end of a MeasurementBlock.
pub struct MeasurementBlockTrailer {
    /// Encoding version
    version: u16,

    /// Offset & size of data section.
    data: Section,

    /// Offset & size of hash map section.
    hash_index: Section,

    /// Offset and size of cardinality sketch for measurements.
    sketch: Section,

    /// Offset and size of cardinality sketch for tombstoned measurements.
    t_sketch: Section,
}

impl MeasurementBlockTrailer {
    pub async fn read_from<R: AsyncRead + AsyncSeek + Send + Unpin>(
        reader: &mut R,
        section: &Section,
    ) -> anyhow::Result<Self> {
        if section.size != MEASUREMENT_TRAILER_SIZE as u64 {
            return Err(anyhow!("invalid index file"));
        }

        // Read version (which is located in the last two bytes of the trailer).
        let version_offset = section.max_offset() - 2;
        reader.seek(SeekFrom::Start(version_offset)).await?;
        let version = reader.read_u16().await?;
        if version != MEASUREMENT_BLOCK_VERSION {
            return Err(anyhow!("unsupported index file version"));
        }

        // Slice trailer data.
        reader.seek(SeekFrom::Start(section.offset)).await?;

        // Read data section info.
        let (data, _) = Section::read_from(reader).await?;

        // Read measurement block info.
        let (hash_index, _) = Section::read_from(reader).await?;

        // Read measurement sketch info.
        let (sketch, _) = Section::read_from(reader).await?;

        // Read tombstone measurement sketch info.
        let (t_sketch, _) = Section::read_from(reader).await?;

        Ok(Self {
            version,
            data,
            hash_index,
            sketch,
            t_sketch,
        })
    }

    pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
        // Write data section info.
        self.data.write_to(&mut w).await?;

        // Write hash index section info.
        self.hash_index.write_to(&mut w).await?;

        // Write measurement sketch info.
        self.sketch.write_to(&mut w).await?;

        // Write tombstone measurement sketch info.
        self.t_sketch.write_to(&mut w).await?;

        // Write measurement block version.
        w.write_u16(self.version).await?;

        Ok(())
    }
}

/// MeasurementBlock represents a collection of all measurements in an index.
pub struct MeasurementBlock {
    op: DataOperator,

    trailer: MeasurementBlockTrailer,
}

impl MeasurementBlock {
    pub async fn new(op: DataOperator, trailer: MeasurementBlockTrailer) -> anyhow::Result<Self> {
        Ok(Self { op, trailer })
    }
}
