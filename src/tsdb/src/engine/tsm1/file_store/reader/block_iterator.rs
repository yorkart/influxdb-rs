use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::FloatValuesVec;
use common_base::iterator::{AsyncIterator, RefAsyncIterator};
use influxdb_storage::opendal::Reader;
use tokio::sync::Mutex;

use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;
use crate::engine::tsm1::value::values::BlockDecoder;
use crate::engine::tsm1::value::values::Values;
use crate::engine::tsm1::value::{FloatValues, Value};

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
    reader: Arc<Mutex<Reader>>,
    inner: ShareTSMReaderInner<I, B>,
}

impl<B, I> BlockIteratorBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub(crate) fn new(reader: Reader, inner: ShareTSMReaderInner<I, B>) -> Self {
        Self {
            reader: Arc::new(Mutex::new(reader)),
            inner,
        }
    }

    async fn entries(&mut self, key: &[u8]) -> anyhow::Result<IndexEntries> {
        let mut reader = self.reader.lock().await;
        let mut entries = IndexEntries::default();
        self.inner
            .index()
            .entries(&mut reader, key, &mut entries)
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
            BlockIterator::new(entries, self.reader.clone(), self.inner.clone()).await?;
        Ok(Box::new(itr))
    }
}

/// BlockIterator allows iterating over each block in a TSM file in order.  It provides
/// raw access to the block bytes without decoding them.
pub struct BlockIterator<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    entries: IndexEntries,
    i: usize,

    reader: Arc<Mutex<Reader>>,
    inner: ShareTSMReaderInner<I, B>,

    block: Vec<u8>,
}

impl<B, I> BlockIterator<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub(crate) async fn new(
        entries: IndexEntries,
        reader: Arc<Mutex<Reader>>,
        inner: ShareTSMReaderInner<I, B>,
    ) -> anyhow::Result<BlockIterator<B, I>> {
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
impl<B, I> RefAsyncIterator for BlockIterator<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    type Item<'b> = &'b [u8] where Self: 'b;

    async fn try_next<'c>(&'c mut self) -> anyhow::Result<Option<Self::Item<'c>>> {
        if self.entries.entries.len() == 0 || self.i >= self.entries.entries.len() {
            return Ok(None);
        }

        let ie = self.entries.entries[self.i].clone();
        self.i += 1;

        let mut reader = self.reader.lock().await;
        self.inner
            .block()
            .read_block(&mut reader, ie, &mut self.block)
            .await?;

        Ok(Some(self.block.as_slice()))
    }
}

pub struct FloatFieldIterator<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    block_itr: BlockIterator<B, I>,

    values: FloatValues,
    step: usize,
}

impl<B, I> FloatFieldIterator<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub fn new(block_itr: BlockIterator<B, I>) -> Self {
        Self {
            block_itr,
            values: vec![],
            step: 0,
        }
    }
}

#[async_trait]
impl<B, I> AsyncIterator for FloatFieldIterator<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    type Item = Value<f64>;

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        if self.values.len() == 0 || self.values.len() <= self.step {
            if let Some(v) = self.block_itr.try_next().await? {
                (&mut self.values).decode(v)?;
                if self.values.len() == 0 {
                    return Ok(None);
                }
                self.step = 0;
            } else {
                return Ok(None);
            }
        }

        let v = self.values[self.step].clone();
        self.step += 1;

        Ok(Some(v))
    }
}

#[async_trait]
pub trait ArrayBuilder {
    async fn next_time(&mut self) -> anyhow::Result<Option<i64>>;
    fn fill_value(&mut self) -> anyhow::Result<()>;
    fn fill_null(&mut self);
    fn build(&mut self) -> Arc<dyn Array>;
}

pub struct FloatArrayBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    capacity: usize,

    itr: FloatFieldIterator<B, I>,
    cur: Option<Value<f64>>,

    buf: Option<FloatValuesVec>,
}

impl<B, I> FloatArrayBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub fn new(itr: FloatFieldIterator<B, I>, capacity: usize) -> Self {
        Self {
            capacity,
            itr,
            cur: None,
            buf: Some(FloatValuesVec::with_capacity(capacity)),
        }
    }
}

#[async_trait]
impl<B, I> ArrayBuilder for FloatArrayBuilder<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    async fn next_time(&mut self) -> anyhow::Result<Option<i64>> {
        let next_value = self.itr.try_next().await?;
        self.cur = next_value;
        Ok(self.cur.map(|x| x.unix_nano))
    }

    fn fill_value(&mut self) -> anyhow::Result<()> {
        if let Some(v) = &self.cur {
            self.buf.as_mut().unwrap().push(Some(v.value));
            Ok(())
        } else {
            Err(anyhow!("value not found"))
        }
    }

    fn fill_null(&mut self) {
        self.buf.as_mut().unwrap().push(None);
    }

    fn build(&mut self) -> Arc<dyn Array> {
        let array = self.buf.take().unwrap();
        self.buf = Some(FloatValuesVec::with_capacity(self.capacity));
        array.into_arc()
    }
}
