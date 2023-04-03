pub mod index;
pub mod writer;

/// MagicNumber is written as the first 4 bytes of a data file to
/// identify the file as a tsm1 formatted file
const MagicNumber: u32 = 0x16D116D1;

/// Version indicates the version of the TSM file format.
const Version: u8 = 1;

/// Size in bytes of an index entry
const indexEntrySize: usize = 28;

/// Size in bytes used to store the count of index entries for a key
const indexCountSize: usize = 2;

/// Size in bytes used to store the type of block encoded
const indexTypeSize: usize = 1;

/// Max number of blocks for a given key that can exist in a single file
const maxIndexEntries: usize = (1 << (indexCountSize * 8)) - 1;

/// max length of a key in an index entry (measurement + tags)
const maxKeyLength: usize = (1 << (2 * 8)) - 1;

/// The threshold amount data written before we periodically fsync a TSM file.  This helps avoid
/// long pauses due to very large fsyncs at the end of writing a TSM file.
const fsyncEvery: usize = 25 * 1024 * 1024;
