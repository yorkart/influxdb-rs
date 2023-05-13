use std::fmt::Debug;
use std::marker::PhantomData;

use common_base::iterator::AsyncIterator;
use influxdb_storage::opendal::Reader;

use crate::engine::tsm1::encoding::{BlockDecoder, TypeEncoder, TypeValues, Value};
use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;

#[async_trait]
pub trait AsyncIteratorBuilder {
    type Output<'a>
    where
        Self: 'a;

    async fn build<'a, 'b: 'a>(&'a mut self, key: &'b [u8]) -> anyhow::Result<Self::Output<'a>>;
}

pub struct BlockIteratorBuilder<B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    reader: Reader,
    inner: ShareTSMReaderInner<I, B>,

    _p: PhantomData<V>,
}

impl<B, I, V> BlockIteratorBuilder<B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    pub(crate) fn new(reader: Reader, inner: ShareTSMReaderInner<I, B>) -> Self {
        Self {
            reader,
            inner,
            _p: Default::default(),
        }
    }
}

#[async_trait]
impl<B, I, V> AsyncIteratorBuilder for BlockIteratorBuilder<B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    V: Debug + Send + Clone + PartialOrd + PartialEq,
    Value<V>: TypeEncoder,
    TypeValues<V>: BlockDecoder,
{
    type Output<'a> = BlockIterator<'a, B, I, V>
        where
            Self: 'a;

    async fn build<'a, 'b: 'a>(&'a mut self, key: &'b [u8]) -> anyhow::Result<Self::Output<'a>> {
        let mut entries = IndexEntries::default();
        self.inner
            .index()
            .entries(&mut self.reader, key, &mut entries)
            .await?;

        let itr: BlockIterator<'a, B, I, V> =
            BlockIterator::new(entries, &mut self.reader, self.inner.clone()).await?;
        Ok(itr)
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
    values: TypeValues<V>,
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
            values: vec![],
            // _p: PhantomData,
        })
    }
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
        Ok(None)
    }
}
