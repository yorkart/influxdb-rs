//!
//! A TSM file is composed for four sections: header, blocks, index and the footer.
//!
//! ┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
//! │ Header │               Blocks               │    Index    │    Footer    │
//! │5 bytes │              N bytes               │   N bytes   │   4 bytes    │
//! └────────┴────────────────────────────────────┴─────────────┴──────────────┘
//!
//! Header is composed of a magic number to identify the file type and a version
//! number.
//!
//! ┌───────────────────┐
//! │      Header       │
//! ├─────────┬─────────┤
//! │  Magic  │ Version │
//! │ 4 bytes │ 1 byte  │
//! └─────────┴─────────┘
//!
//! Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
//! file.  The CRC32 is used for block level error detection.  The length of the blocks
//! is stored in the index.
//!
//! ┌───────────────────────────────────────────────────────────┐
//! │                          Blocks                           │
//! ├───────────────────┬───────────────────┬───────────────────┤
//! │      Block 1      │      Block 2      │      Block N      │
//! ├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
//! │  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
//! │ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
//! └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
//!
//! Following the blocks is the index for the blocks in the file.  The index is
//! composed of a sequence of index entries ordered lexicographically by key and
//! then by time.  Each index entry starts with a key length and key followed by a
//! count of the number of blocks in the file.  Each block entry is composed of
//! the min and max time for the block, the offset into the file where the block
//! is located and the the size of the block.
//!
//! The index structure can provide efficient access to all blocks as well as the
//! ability to determine the cost associated with accessing a given key.  Given a key
//! and timestamp, we can determine whether a file contains the block for that
//! timestamp as well as where that block resides and how much data to read to
//! retrieve the block.  If we know we need to read all or multiple blocks in a
//! file, we can use the size to determine how much to read in a given IO.
//!
//! ┌────────────────────────────────────────────────────────────────────────────┐
//! │                                   Index                                    │
//! ├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
//! │ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
//! │ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
//! └─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘
//!
//! The last section is the footer that stores the offset of the start of the index.
//!
//! ┌─────────┐
//! │ Footer  │
//! ├─────────┤
//! │Index Ofs│
//! │ 8 bytes │
//! └─────────┘
//!

pub mod index_writer;
pub mod tsm_writer;
