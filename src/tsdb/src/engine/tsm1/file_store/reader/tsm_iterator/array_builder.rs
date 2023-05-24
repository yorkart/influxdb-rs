use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::FloatValuesVec;
use common_base::iterator::AsyncIterator;

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator::field_iterator::FloatFieldIterator;
use crate::engine::tsm1::value::Value;

#[async_trait]
pub trait ArrayBuilder: Send + Sync {
    async fn next(&mut self) -> anyhow::Result<Option<()>>;
    fn next_time(&self) -> Option<i64>;
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
    async fn next(&mut self) -> anyhow::Result<Option<()>> {
        let next_value = self.itr.try_next().await?;
        self.cur = next_value;

        Ok(self.cur.as_ref().map(|_x| ()))
    }

    fn next_time(&self) -> Option<i64> {
        self.cur.as_ref().map(|x| x.unix_nano)
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
