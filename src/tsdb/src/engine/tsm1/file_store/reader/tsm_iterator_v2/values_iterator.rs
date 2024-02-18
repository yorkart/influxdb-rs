use crate::engine::tsm1::block::{
    BLOCK_BOOLEAN, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
};
use common_base::iterator::RefAsyncIterator;

use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::value::{
    Array, BooleanValues, FloatValues, IntegerValues, StringValues, UnsignedValues,
};

#[async_trait]
pub trait EntriesValuesReader {
    async fn try_next(&mut self) -> anyhow::Result<Option<Box<dyn Array>>>;
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
    async fn try_next(&mut self) -> anyhow::Result<Option<Box<dyn Array>>> {
        let typ = self.block_itr.typ();
        if let Some(v) = self.block_itr.try_next().await? {
            match typ {
                BLOCK_FLOAT64 => {
                    let mut values = FloatValues::new();
                    values.decode(v)?;
                    Ok(Some(Box::new(values)))
                }
                BLOCK_INTEGER => {
                    let mut values = IntegerValues::new();
                    values.decode(v)?;
                    Ok(Some(Box::new(values)))
                }
                BLOCK_BOOLEAN => {
                    let mut values = BooleanValues::new();
                    values.decode(v)?;
                    Ok(Some(Box::new(values)))
                }
                BLOCK_STRING => {
                    let mut values = StringValues::new();
                    values.decode(v)?;
                    Ok(Some(Box::new(values)))
                }
                BLOCK_UNSIGNED => {
                    let mut values = UnsignedValues::new();
                    values.decode(v)?;
                    Ok(Some(Box::new(values)))
                }
                _ => Err(anyhow!("unknown field type {}", typ)),
            }
        } else {
            Ok(None)
        }
    }
}
