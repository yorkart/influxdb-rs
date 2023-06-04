use common_base::iterator::{AsyncIterator, RefAsyncIterator};
use std::marker::PhantomData;

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::value::{Array, ArrayRef};

pub struct ValuesIterator<B, I, A>
where
    B: TSMBlock,
    I: TSMIndex,
    A: Array,
{
    block_itr: BlockIterator<B, I>,
    p: PhantomData<A>,
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
            p: PhantomData::default(),
        }
    }
}

#[async_trait]
impl<B, I, A> AsyncIterator for ValuesIterator<B, I, A>
where
    B: TSMBlock,
    I: TSMIndex,
    A: Array,
{
    type Item = ArrayRef;

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        if let Some(v) = self.block_itr.try_next().await? {
            let array = A::decode_v1(v)?;
            Ok(Some(array))
        } else {
            Ok(None)
        }
    }
}
