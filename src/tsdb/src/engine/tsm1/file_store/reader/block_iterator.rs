use std::fmt::Debug;
use std::marker::PhantomData;

use common_base::iterator::AsyncIterator;
use influxdb_storage::opendal::Reader;

use crate::engine::tsm1::encoding::{BlockDecoder, TypeEncoder, TypeValues};
use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;

pub struct BlockIteratorBuilder<B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder + Debug,
    TypeValues<V>: BlockDecoder,
{
    reader: Reader,
    inner: ShareTSMReaderInner<I, B>,
    _p: PhantomData<V>,
}

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder + Debug,
    TypeValues<V>: BlockDecoder,
{
    entries: IndexEntries,
    i: usize,

    reader: &'a mut Reader,
    inner: ShareTSMReaderInner<I, B>,

    block: Vec<u8>,
    _p: PhantomData<V>,
}

impl<'a, B, I, V> BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    pub(crate) async fn new(
        key: &'a [u8],
        reader: &'a mut Reader,
        inner: ShareTSMReaderInner<I, B>,
    ) -> anyhow::Result<BlockIterator<'a, B, I, V>> {
        let mut entries = IndexEntries::new(V::block_type());
        inner.index().entries(reader, key, &mut entries).await?;

        Ok(Self {
            entries,
            i: 0,
            reader,
            inner,
            block: vec![],
            _p: Default::default(),
        })
    }
}

#[async_trait]
impl<'a, B, I, V> AsyncIterator for BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    type Item = TypeValues<V>;

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        if self.entries.entries.len() == 0 || self.i >= self.entries.entries.len() {
            return Ok(None);
        }

        let ie = self.entries.entries[self.i].clone();
        self.i += 1;
        self.inner
            .block()
            .read_block(&mut self.reader, ie, &mut self.block)
            .await?;

        let mut values: TypeValues<V> = Vec::with_capacity(0);
        values.decode(self.block.as_slice())?;
        Ok(None)
    }
}
