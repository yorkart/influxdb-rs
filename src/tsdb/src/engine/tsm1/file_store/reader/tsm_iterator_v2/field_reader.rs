use std::sync::Arc;

use common_base::iterator::RefAsyncIterator;
use influxdb_storage::opendal::Reader;
use influxdb_storage::StorageOperator;
use tokio::sync::Mutex;

use crate::engine::tsm1::file_store::index::{IndexEntries, IndexEntry};
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::block_iterator::BlockIterator;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::values_iterator::{
    DefaultEntriesValuesReader, EntriesValuesReader,
};
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;
use crate::engine::tsm1::value::Array;

#[async_trait]
pub trait FieldReader: Send + Sync {
    fn path(&self) -> &str;

    async fn read<'a, 'b>(&'a self, key: &[u8]) -> anyhow::Result<Box<dyn EntriesValuesReader>>;

    async fn read_at(&self, entry: &IndexEntry, values: &mut Box<dyn Array>) -> anyhow::Result<()>;
}

pub struct DefaultFieldReader<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    path: String,
    reader: Arc<Mutex<Reader>>,
    inner: ShareTSMReaderInner<I, B>,
}

impl<B, I> DefaultFieldReader<B, I>
where
    B: TSMBlock,
    I: TSMIndex,
{
    pub(crate) async fn new(
        op: StorageOperator,
        inner: ShareTSMReaderInner<I, B>,
    ) -> anyhow::Result<Self> {
        let reader = op.reader().await?;
        let path = op.path().to_string();
        Ok(Self {
            path,
            reader: Arc::new(Mutex::new(reader)),
            inner,
        })
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
    fn path(&self) -> &str {
        self.path.as_str()
    }

    async fn read<'a, 'b>(&'a self, key: &[u8]) -> anyhow::Result<Box<dyn EntriesValuesReader>> {
        let entries = self.entries(key).await?;
        let typ = entries.typ;
        let itr: BlockIterator<B, I> =
            BlockIterator::new(entries, self.reader.clone(), self.inner.clone()).await?;
        match typ {
            0 => {
                let reader = DefaultEntriesValuesReader::new(itr);
                Ok(Box::new(reader))
            }
            _ => Err(anyhow!("unknown type: {}", typ)),
        }
    }

    async fn read_at(&self, entry: &IndexEntry, values: &mut Box<dyn Array>) -> anyhow::Result<()> {
        let entries = IndexEntries {
            typ: 0, // ignore this
            entries: vec![entry.clone()],
        };
        let mut itr: BlockIterator<B, I> =
            BlockIterator::new(entries, self.reader.clone(), self.inner.clone()).await?;
        if let Some(v) = itr.try_next().await? {
            values.decode(v)?;
        }

        Ok(())
    }
}
