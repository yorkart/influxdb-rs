use common_base::iterator::RefAsyncIterator;

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::value::Array;

pub struct ValuesIterator<B, I, A>
where
    B: TSMBlock,
    I: TSMIndex,
    A: Array,
{
    block_itr: BlockIterator<B, I>,
    values: A,
}

impl<B, I, A> ValuesIterator<B, I, A>
where
    B: TSMBlock,
    I: TSMIndex,
    A: Array,
{
    pub fn new(block_itr: BlockIterator<B, I>) -> Self {
        Self {
            block_itr,
            values: A::default(),
        }
    }
}

#[async_trait]
impl<B, I, A> RefAsyncIterator for ValuesIterator<B, I, A>
where
    B: TSMBlock,
    I: TSMIndex,
    A: Array,
{
    type Item<'a> = &'a A
    where
        Self: 'a;

    async fn try_next<'a>(&'a mut self) -> anyhow::Result<Option<Self::Item<'a>>> {
        if let Some(v) = self.block_itr.try_next().await? {
            self.values.decode(v)?;
            Ok(Some(&self.values))
        } else {
            Ok(None)
        }
    }
}
