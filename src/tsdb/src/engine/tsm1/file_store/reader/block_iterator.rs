use crate::engine::tsm1::file_store::reader::tsm_reader::TSMReader;

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<'a, R>
where
    R: TSMReader,
{
    r: &'a R,
}
