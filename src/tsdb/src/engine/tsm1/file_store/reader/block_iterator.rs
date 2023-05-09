use crate::engine::tsm1::encoding::{Capacity, FieldValue};
use crate::engine::tsm1::file_store::reader::tsm_reader::TSMReader;
use std::fmt::Debug;

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<'a, R, V>
where
    R: TSMReader,
    V: Capacity + Debug + Clone + PartialOrd,
{
    r: &'a R,

    values: FieldValue<V>,
}

impl<'a, R, V> BlockIterator<'a, R, V>
where
    R: TSMReader,
    V: Capacity + Debug + Clone + PartialOrd,
{
    pub fn new(r: &'a R) -> Self {
        Self {
            r,
            values: FieldValue::with_capacity(0),
        }
    }
}
