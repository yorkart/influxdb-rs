pub mod decoder;
pub mod encoder;

/// BLOCK_FLOAT64 designates a block encodes float64 values.
const BLOCK_FLOAT64: u8 = 0;

/// BLOCK_INTEGER designates a block encodes int64 values.
const BLOCK_INTEGER: u8 = 1;

/// BLOCK_BOOLEAN designates a block encodes boolean values.
const BLOCK_BOOLEAN: u8 = 2;

/// BLOCK_STRING designates a block encodes string values.
const BLOCK_STRING: u8 = 3;

/// BLOCK_UNSIGNED designates a block encodes uint64 values.
const BLOCK_UNSIGNED: u8 = 4;

/// ENCODED_BLOCK_HEADER_SIZE is the size of the header for an encoded block.  There is one
/// byte encoding the type of the block.
const ENCODED_BLOCK_HEADER_SIZE: usize = 1;
