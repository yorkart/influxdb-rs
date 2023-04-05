pub mod index;
pub mod reader;
pub mod stat;
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

/// The extension used to describe temporary snapshot files.
pub(crate) const TMP_TSMFILE_EXTENSION: &'static str = "tmp";

/// The extension used to describe corrupt snapshot files.
pub(crate) const BAD_TSMFILE_EXTENSION: &'static str = "bad";

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use crate::engine::tsm1::file_store::{HEADER, MAGIC_NUMBER, VERSION};

    #[test]
    fn test_header() {
        let mut buf = Vec::with_capacity(5);
        buf.put_u32(MAGIC_NUMBER);
        buf.put_u8(VERSION);

        assert_eq!(buf.as_slice(), &HEADER);
    }
}
