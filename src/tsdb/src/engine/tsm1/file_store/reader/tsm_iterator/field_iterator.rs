use common_base::iterator::{AsyncIterator, RefAsyncIterator};

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator::block_iterator::BlockIterator;
use crate::engine::tsm1::value::values::BlockDecoder;
use crate::engine::tsm1::value::{FloatValues, Value};

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
