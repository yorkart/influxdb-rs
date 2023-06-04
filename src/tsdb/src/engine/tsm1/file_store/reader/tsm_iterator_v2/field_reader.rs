use std::sync::Arc;

use common_base::iterator::AsyncIterator;
use influxdb_storage::opendal::Reader;
use tokio::sync::Mutex;

use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::values_iterator::ValuesIterator;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;
use crate::engine::tsm1::value::{Array, ArrayRef, FloatValues};

#[async_trait]
pub trait FieldReader: Send + Sync {
    async fn read(
        &self,
        key: &[u8],
    ) -> anyhow::Result<Box<dyn AsyncIterator<Item = Box<dyn Array>>>>;
}

pub struct DefaultFieldReader<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    reader: Arc<Mutex<Reader>>,
    inner: ShareTSMReaderInner<I, B>,
}

impl<B, I> DefaultFieldReader<B, I>
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

    async fn entries(&self, key: &[u8]) -> anyhow::Result<IndexEntries> {
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
impl<B, I> FieldReader for DefaultFieldReader<B, I>
where
    B: TSMBlock + 'static,
    I: TSMIndex + 'static,
{
    async fn read(&self, key: &[u8]) -> anyhow::Result<Box<dyn AsyncIterator<Item = ArrayRef>>> {
        let entries = self.entries(key).await?;
        let typ = entries.typ;
        let itr: BlockIterator<B, I> =
            BlockIterator::new(entries, self.reader.clone(), self.inner.clone()).await?;
        match typ {
            0 => {
                let v_iter: ValuesIterator<B, I, FloatValues> = ValuesIterator::new(itr);
                Ok(Box::new(v_iter))
            }
            _ => Err(anyhow!("unknown type: {}", typ)),
        }
    }
}

// #[async_trait]
// impl<B, I> FieldReader for DefaultFieldReader<B, I>
// where
//     B: TSMBlock + 'static,
//     I: TSMIndex + 'static,
// {
//     async fn build(
//         &self,
//         key: &[u8],
//         fields: &[&[u8]],
//     ) -> anyhow::Result<Box<dyn AsyncIterator<Item = Chunk<Arc<dyn Array>>>>> {
//         let mut builders = Vec::with_capacity(fields.len());
//
//         for field in fields {
//             let key = series_field_key(key, field);
//
//             let entries = self.entries(key.as_slice()).await?;
//             let typ = entries.typ;
//             let itr: BlockIterator<B, I> =
//                 BlockIterator::new(entries, self.reader.clone(), self.inner.clone()).await?;
//             let builder = match typ {
//                 0 => {
//                     let field_itr = FloatFieldIterator::new(itr);
//                     let array_builder = FloatArrayBuilder::new(field_itr, 1024);
//                     let array_builder: Box<dyn ArrayBuilder> = Box::new(array_builder);
//                     Ok(array_builder)
//                 }
//                 _ => Err(anyhow!("unknown type: {}", typ)),
//             }?;
//             builders.push(builder);
//         }
//
//         let itr = FieldsBatchIterator::new(builders, 1024).await?;
//         Ok(Box::new(itr))
//     }
// }
