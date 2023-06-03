use std::marker::PhantomData;

use common_base::iterator::{AsyncIterator, RefAsyncIterator};

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::value::{Array, FieldType, TimeValue, TypeValues};

pub struct ValuesIterator<B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    TypeValues<V>: Array,
    V: FieldType,
{
    block_itr: BlockIterator<B, I>,
    p: PhantomData<V>,
}

#[async_trait]
impl<B, I, V> AsyncIterator for ValuesIterator<B, I, V>
where
    B: TSMBlock,
    I: TSMIndex,
    TypeValues<V>: Array,
    V: FieldType,
{
    type Item = TypeValues<V>;

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        if let Some(v) = self.block_itr.try_next().await? {
            let mut values: Vec<TimeValue<V>> = vec![];
            values.decode1(v)?;
            Ok(Some(values))
        } else {
            Ok(None)
        }
    }
}
