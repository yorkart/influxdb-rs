use std::fmt::Debug;
use std::marker::PhantomData;

use common_base::iterator::AsyncIterator;
use influxdb_storage::opendal::Reader;

use crate::engine::tsm1::encoding::{BlockDecoder, TypeEncoder, TypeValues, Value};
use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;

pub trait TBlockIterator<'a, V>: AsyncIterator<Item = TypeValues<V>>
where
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
}

#[async_trait]
pub trait AsyncIteratorBuilder: Send + Sync {
    // async fn build<'a, 'b: 'a, V>(
    //     &'a mut self,
    //     key: &'b [u8],
    // ) -> anyhow::Result<Box<dyn TBlockIterator<'a, V, Item = TypeValues<V>> + 'a>>
    // where
    //     Self: 'a,
    //     'b: 'a,
    //     V: Debug + Send + Clone + PartialOrd + PartialEq + 'static,
    //     Value<V>: TypeEncoder,
    //     TypeValues<V>: BlockDecoder;

    async fn build_f64<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, f64, Item = TypeValues<f64>> + 'a>>
    where
        Self: 'a,
        'b: 'a;
    async fn build_integer<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, i64, Item = TypeValues<i64>> + 'a>>
    where
        Self: 'a,
        'b: 'a;
    async fn build_bool<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, bool, Item = TypeValues<bool>> + 'a>>
    where
        Self: 'a,
        'b: 'a;
    async fn build_string<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, Vec<u8>, Item = TypeValues<Vec<u8>>> + 'a>>
    where
        Self: 'a,
        'b: 'a;
    async fn build_unsigned<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, u64, Item = TypeValues<u64>> + 'a>>
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
    async fn build_f64<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, f64, Item = TypeValues<f64>> + 'a>>
    where
        Self: 'a,
        'b: 'a,
    {
        let entries = self.entries(key).await?;
        let itr: BlockIterator<B, I, f64> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(Box::new(itr))
    }

    async fn build_integer<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, i64, Item = TypeValues<i64>> + 'a>>
    where
        Self: 'a,
        'b: 'a,
    {
        let entries = self.entries(key).await?;
        let itr: BlockIterator<B, I, i64> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(Box::new(itr))
    }

    async fn build_bool<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, bool, Item = TypeValues<bool>> + 'a>>
    where
        Self: 'a,
        'b: 'a,
    {
        let entries = self.entries(key).await?;
        let itr: BlockIterator<B, I, bool> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(Box::new(itr))
    }

    async fn build_string<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, Vec<u8>, Item = TypeValues<Vec<u8>>> + 'a>>
    where
        Self: 'a,
        'b: 'a,
    {
        let entries = self.entries(key).await?;
        let itr: BlockIterator<B, I, Vec<u8>> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(Box::new(itr))
    }

    async fn build_unsigned<'a, 'b: 'a>(
        &'a mut self,
        key: &'b [u8],
    ) -> anyhow::Result<Box<dyn TBlockIterator<'a, u64, Item = TypeValues<u64>> + 'a>>
    where
        Self: 'a,
        'b: 'a,
    {
        let entries = self.entries(key).await?;
        let itr: BlockIterator<B, I, u64> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(Box::new(itr))
    }
}

pub enum TypeBlockIterator<'a, B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    Float(BlockIterator<'a, B, I, f64>),
    Integer(BlockIterator<'a, B, I, i64>),
    Bool(BlockIterator<'a, B, I, bool>),
    String(BlockIterator<'a, B, I, Vec<u8>>),
    Unsigned(BlockIterator<'a, B, I, u64>),
}

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    entries: IndexEntries,
    i: usize,

    reader: &'a mut Reader,
    inner: ShareTSMReaderInner<I, B>,

    block: Vec<u8>,
    // values: TypeValues<V>,
    _p: PhantomData<V>,
}

impl<'a, B, I, V> BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    pub(crate) async fn new(
        entries: IndexEntries,
        reader: &'a mut Reader,
        inner: ShareTSMReaderInner<I, B>,
    ) -> anyhow::Result<BlockIterator<'a, B, I, V>> {
        Ok(Self {
            entries,
            i: 0,
            reader,
            inner,
            block: vec![],
            // values: vec![],
            _p: PhantomData,
        })
    }
}

impl<'a, B, I, V> TBlockIterator<'a, V> for BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
}

#[async_trait]
impl<'a, B, I, V> AsyncIterator for BlockIterator<'a, B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
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
        Ok(Some(values))
    }
}
