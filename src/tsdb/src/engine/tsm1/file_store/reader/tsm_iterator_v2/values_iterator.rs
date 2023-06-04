use common_base::iterator::RefAsyncIterator;

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::value::Array;

#[async_trait]
pub trait EntriesValuesReader {
    async fn try_next(&mut self, value: &mut Box<dyn Array>) -> anyhow::Result<Option<()>>;
}

pub struct DefaultEntriesValuesReader<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    block_itr: BlockIterator<B, I>,
}

impl<B, I> DefaultEntriesValuesReader<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub fn new(block_itr: BlockIterator<B, I>) -> Self {
        Self { block_itr }
    }
}

#[async_trait]
impl<B, I> EntriesValuesReader for DefaultEntriesValuesReader<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    async fn try_next(&mut self, value: &mut Box<dyn Array>) -> anyhow::Result<Option<()>> {
        if let Some(v) = self.block_itr.try_next().await? {
            value.decode(v)?;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
}
