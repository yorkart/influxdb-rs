pub mod index;
pub mod reader;
pub mod stat;
pub mod tombstone;
pub mod writer;

/// MAGIC_NUMBER is written as the first 4 bytes of a data file to
/// identify the file as a tsm1 formatted file
const MAGIC_NUMBER: u32 = 0x16D116D1;

/// VERSION indicates the version of the TSM file format.
const VERSION: u8 = 1;

/// Block's header: | magic number(4B) | VERSION(1B) |
const HEADER: [u8; 5] = [22, 209, 22, 209, 1];

/// size in bytes of an index entry
const INDEX_ENTRY_SIZE: usize = 28;

/// size in bytes used to store the count of index entries for a key
const INDEX_COUNT_SIZE: usize = 2;

/// size in bytes used to store the type of block encoded
const INDEX_TYPE_SIZE: usize = 1;

/// Max number of blocks for a given key that can exist in a single file
const MAX_INDEX_ENTRIES: usize = (1 << (INDEX_COUNT_SIZE * 8)) - 1;

/// max length of a key in an index entry (measurement + tags)
const MAX_KEY_LENGTH: usize = (1 << (2 * 8)) - 1;

/// The threshold amount data written before we periodically fsync a TSM file.  This helps avoid
/// long pauses due to very large fsyncs at the end of writing a TSM file.
const FSYNC_EVERY: u64 = 25 * 1024 * 1024;

/// TimeRange holds a min and max timestamp.
#[derive(Debug, Clone)]
pub struct TimeRange {
    pub(crate) min: i64,
    pub(crate) max: i64,
}

impl TimeRange {
    pub fn new(min: i64, max: i64) -> Self {
        Self { min, max }
    }

    pub fn unbound() -> Self {
        Self::new(i64::MIN, i64::MAX)
    }

    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.min <= other.max && self.max >= other.min
    }
}

/// TimeRange holds a min and max timestamp.
#[derive(Debug, Clone)]
pub struct KeyRange {
    pub(crate) min: Vec<u8>,
    pub(crate) max: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use influxdb_storage::StorageOperator;

    use crate::engine::tsm1::block::BLOCK_FLOAT64;
    use crate::engine::tsm1::encoding::{Value, Values};
    use crate::engine::tsm1::file_store::index::IndexEntries;
    use crate::engine::tsm1::file_store::reader::tsm_reader::{DefaultTSMReader, TSMReader};
    use crate::engine::tsm1::file_store::writer::tsm_writer::{DefaultTSMWriter, TSMWriter};

    #[tokio::test]
    async fn test_tsm_reader() {
        let dir = tempfile::tempdir().unwrap();
        let tsm_file = dir.as_ref().join("tsm1_test");
        println!("{}", tsm_file.to_str().unwrap());

        {
            let mut w = DefaultTSMWriter::with_mem_buffer(&tsm_file).await.unwrap();

            let values = Values::Float(vec![
                Value::new(1, 1.0),
                Value::new(2, 3.0),
                Value::new(3, 5.0),
                Value::new(4, 7.0),
            ]);

            w.write("cpu".as_bytes(), values).await.unwrap();
            w.write_index().await.unwrap();
            w.close().await.unwrap();

            let data = tokio::fs::read(tsm_file.clone()).await.unwrap();
            println!("{:?}", data);
        }

        {
            let op = influxdb_storage::operator().unwrap();
            let op = StorageOperator::new(op, tsm_file.to_str().unwrap());

            let mut r = DefaultTSMReader::new(op).await.unwrap();

            let mut entries = IndexEntries::new(BLOCK_FLOAT64);
            r.read_entries("cpu".as_bytes(), &mut entries)
                .await
                .unwrap();

            let mut float_values: Vec<Value<f64>> = Vec::new();
            for entry in entries.entries {
                r.read_block_at(entry, &mut float_values).await.unwrap();
            }

            for v in float_values {
                println!("{}, {}", v.unix_nano, v.value);
            }

            r.close().await.unwrap();
        }
    }
}
