use std::sync::Arc;

use common_base::iterator::RefAsyncIterator;
use influxdb_storage::opendal::Reader;
use tokio::sync::Mutex;

use crate::engine::tsm1::file_store::index::IndexEntries;
use crate::engine::tsm1::file_store::reader::block_reader::TSMBlock;
use crate::engine::tsm1::file_store::reader::index_reader::TSMIndex;
use crate::engine::tsm1::file_store::reader::tsm_reader::ShareTSMReaderInner;

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

    pub fn typ(&self) -> u8 {
        self.entries.typ
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
            .read_block(&mut reader, &ie, &mut self.block)
            .await?;

        Ok(Some(self.block.as_slice()))
    }
}
