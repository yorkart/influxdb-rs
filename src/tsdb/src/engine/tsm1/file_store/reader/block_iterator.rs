use std::fmt::Debug;

use influxdb_common::iterator::AsyncIterator;
use influxdb_storage::opendal::Reader;

use crate::engine::tsm1::encoding::{BlockDecoder, TypeEncoder, TypeValues};
use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<'a, 'b, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder + Debug,
    TypeValues<V>: BlockDecoder,
{
    key: &'a [u8],
    entries: IndexEntries,

    r: Reader,

    i: I,
    b: B,

    values: &'b mut TypeValues<V>,
}

impl<'a, 'b, B, I, V> BlockIterator<'a, 'b, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    pub async fn new(
        key: &'a [u8],
        mut r: Reader,
        i: I,
        b: B,
        values: &'b mut TypeValues<V>,
    ) -> anyhow::Result<BlockIterator<'a, 'b, B, I, V>> {
        let mut entries = IndexEntries::new(V::block_type());
        i.entries(&mut r, key, &mut entries).await?;

        Ok(Self {
            key,
            entries,
            r,
            i,
            b,
            values,
        })
    }
}

#[async_trait]
impl<'a, 'b, B, I, V> AsyncIterator for BlockIterator<'a, 'b, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    type Item = ();

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        let ie = self.entries.entries[0].clone();
        let block = self.b.read_block(&mut self.r, ie).await?;
        self.values.decode(block.as_slice())?;
        Ok(None)
    }
}
