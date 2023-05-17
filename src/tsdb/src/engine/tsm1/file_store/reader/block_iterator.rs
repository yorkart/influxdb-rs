use common_base::iterator::AsyncIterator;
use influxdb_storage::opendal::Reader;

use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;
use crate::engine::tsm1::value::values::Values;

#[async_trait]
pub trait AsyncIteratorBuilder: Send + Sync {
    async fn build<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn AsyncIterator<Item = Values> + 'a>>
    where
        Self: 'a,
        'b: 'a;
}

pub struct BlockIteratorBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    reader: Reader,
    inner: ShareTSMReaderInner<I, B>,
}

impl<B, I> BlockIteratorBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub(crate) fn new(reader: Reader, inner: ShareTSMReaderInner<I, B>) -> Self {
        Self { reader, inner }
    }

    async fn entries(&mut self, key: &[u8]) -> anyhow::Result<IndexEntries> {
        let mut entries = IndexEntries::default();
        self.inner
            .index()
            .entries(&mut self.reader, key, &mut entries)
            .await?;
        Ok(entries)
    }
}

#[async_trait]
impl<B, I> AsyncIteratorBuilder for BlockIteratorBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    async fn build<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn AsyncIterator<Item = Values> + 'a>>
    where
        Self: 'a,
        'b: 'a,
    {
        let entries = self.entries(key).await?;
        let itr: BlockIterator<B, I> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(Box::new(itr))
    }
}

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<'a, B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    entries: IndexEntries,
    i: usize,

    reader: &'a mut Reader,
    inner: ShareTSMReaderInner<I, B>,

    block: Vec<u8>,
}

impl<'a, B, I> BlockIterator<'a, B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub(crate) async fn new(
        entries: IndexEntries,
        reader: &'a mut Reader,
        inner: ShareTSMReaderInner<I, B>,
    ) -> anyhow::Result<BlockIterator<'a, B, I>> {
        Ok(Self {
            entries,
            i: 0,
            reader,
            inner,
            block: vec![],
        })
    }
}

#[async_trait]
impl<'a, B, I> AsyncIterator for BlockIterator<'a, B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    type Item = Values;

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

        let values = Values::try_from((self.entries.typ, self.block.as_slice()))?;
        Ok(Some(values))
    }
}
