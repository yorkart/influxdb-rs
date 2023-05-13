use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use influxdb_storage::opendal::Reader;
use influxdb_storage::StorageOperator;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;

use crate::engine::tsm1::encoding::BlockDecoder;
use crate::engine::tsm1::file_store::index::{IndexEntries, IndexEntry};
use crate::engine::tsm1::file_store::reader::batch_deleter::BatchDeleter;
use crate::engine::tsm1::file_store::reader::block_iterator::AsyncIteratorBuilder;
use crate::engine::tsm1::file_store::reader::block_reader::{DefaultBlockAccessor, TSMBlock};
use crate::engine::tsm1::file_store::reader::index_reader::{IndirectIndex, KeyIterator, TSMIndex};
use crate::engine::tsm1::file_store::stat::FileStat;
use crate::engine::tsm1::file_store::tombstone::{
    IndexTombstonerFilter, TombstoneStat, Tombstoner,
};
use crate::engine::tsm1::file_store::{KeyRange, TimeRange, MAGIC_NUMBER, VERSION};

/// TSMFile represents an on-disk TSM file.
#[async_trait]
pub trait TSMReader: Sync + Send {
    /// path returns the underlying file path for the TSMFile.  If the file
    /// has not be written or loaded from disk, the zero value is returned.
    fn path(&self) -> &str;

    async fn block_iterator_builder<B>(&self) -> anyhow::Result<B>
    where
        B: AsyncIteratorBuilder;

    async fn read_block_at<T>(&self, entry: IndexEntry, values: &mut T) -> anyhow::Result<()>
    where
        T: BlockDecoder;

    /// Entries returns the index entries for all blocks for the given key.
    async fn read_entries(&self, key: &[u8], entries: &mut IndexEntries) -> anyhow::Result<()>;

    /// contains returns true if the file contains any values for the given
    /// key.
    async fn contains(&mut self, key: &[u8]) -> anyhow::Result<bool>;

    /// overlaps_time_range returns true if the time range of the file intersect min and max.
    async fn overlaps_time_range(&mut self, min: i64, max: i64) -> bool;

    /// time_range returns the min and max time across all keys in the file.
    async fn time_range(&self) -> TimeRange;

    /// tombstone_range returns ranges of time that are deleted for the given key.
    async fn tombstone_range(&self, key: &[u8]) -> Vec<TimeRange>;

    /// key_range returns the min and max keys in the file.
    async fn key_range(&self) -> KeyRange;

    /// key_count returns the number of distinct keys in the file.
    async fn key_count(&self) -> usize;

    async fn key_iterator(&self) -> anyhow::Result<KeyIterator>;

    /// seek returns the position in the index with the key <= key.
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<u64>;

    /// key_at returns the key located at index position idx.
    async fn key_at(&mut self, idx: usize) -> anyhow::Result<Option<(Vec<u8>, u8)>>;

    /// Type returns the block type of the values stored for the key.  Returns one of
    /// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
    /// an error is returned.
    async fn block_type(&self, key: &[u8]) -> anyhow::Result<u8>;

    /// batch_delete return a BatchDeleter that allows for multiple deletes in batches
    /// and group commit or rollback.
    async fn batch_delete(&mut self) -> Box<dyn BatchDeleter>;

    /// delete removes the keys from the set of keys available in this file.
    async fn delete(&self, keys: &mut [&[u8]]) -> anyhow::Result<()>;

    /// delete_range removes the values for keys between timestamps min and max.
    async fn delete_range(&self, keys: &mut [&[u8]], min: i64, max: i64) -> anyhow::Result<()>;

    /// has_tombstones returns true if file contains values that have been deleted.
    async fn has_tombstones(&self) -> anyhow::Result<bool>;

    /// tombstone_stats returns the tombstone filestats if there are any tombstones
    /// written for this file.
    async fn tombstone_stats(&self) -> anyhow::Result<TombstoneStat>;

    /// close closes the underlying file resources.
    async fn close(&mut self) -> anyhow::Result<()>;

    /// size returns the size of the file on disk in bytes.
    async fn size(&self) -> u32;

    /// remove deletes the file from the filesystem.
    async fn remove(&mut self) -> anyhow::Result<()>;

    /// in_use returns true if the file is currently in use by queries.
    async fn in_use(&self) -> bool;

    /// use_ref records that this file is actively in use.
    async fn use_ref(&mut self);

    /// use_unref records that this file is no longer in use.
    async fn use_unref(&mut self);

    /// stats returns summary information about the TSM file.
    async fn stats(&self) -> anyhow::Result<FileStat>;

    /// free releases any resources held by the FileStore to free up system resources.
    async fn free(&mut self) -> anyhow::Result<()>;
}

pub async fn new_default_tsm_reader(op: StorageOperator) -> anyhow::Result<impl TSMReader> {
    DefaultTSMReader::new(op).await
}

pub(crate) struct TSMReaderInner<I, B>
where
    I: TSMIndex,
    B: TSMBlock,
{
    /// index is the index of all blocks.
    index: I,
    /// block is the value blocks.
    block: B,
}

impl<I, B> TSMReaderInner<I, B>
where
    I: TSMIndex,
    B: TSMBlock,
{
    pub fn new(index: I, block: B) -> Self {
        Self { index, block }
    }

    pub fn block(&self) -> &B {
        &self.block
    }

    pub fn index(&self) -> &I {
        &self.index
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) type ShareTSMReaderInner<I, B> = Arc<TSMReaderInner<I, B>>;

pub(crate) struct DefaultTSMReader<I, B>
where
    I: TSMIndex,
    B: TSMBlock,
{
    /// refs is the count of active references to this reader.
    refs: AtomicU64,

    /// accessor provides access and decoding of blocks for the reader.
    op: StorageOperator,

    /// index is the index of all blocks.
    inner: ShareTSMReaderInner<I, B>,

    /// tombstoner ensures tombstoned keys are not available by the index.
    tombstoner: RwLock<Tombstoner<IndexTombstonerFilter<I, B>>>,

    /// size is the size of the file on disk.
    size: u32,

    /// last_modified is the last time this file was modified on disk
    last_modified: i64,

    /// Counter incremented everytime the mmapAccessor is accessed
    access_count: u64,
    /// Counter to determine whether the accessor can free its resources
    free_count: u64,
}

impl DefaultTSMReader<IndirectIndex, DefaultBlockAccessor> {
    pub async fn new(op: StorageOperator) -> anyhow::Result<Self> {
        let mut reader = op.reader().await?;
        Self::verify_version(&mut reader).await?;

        reader.seek(SeekFrom::Start(0)).await?;

        let stat = op.stat().await?;
        let file_size = stat.content_length();
        if file_size < 8 {
            return Err(anyhow!(
                "BlockAccessor: byte slice too small for IndirectIndex"
            ));
        }

        let index_ofs_pos = file_size - 8;
        reader.seek(SeekFrom::Start(index_ofs_pos)).await?;
        let index_start = reader.read_u64().await?;

        let index = IndirectIndex::new(
            &mut reader,
            index_start,
            (index_ofs_pos - index_start) as u32,
        )
        .await?;
        let block = DefaultBlockAccessor::new(index_start).await?;
        let inner = Arc::new(TSMReaderInner::new(index, block));

        let tombstoner =
            Tombstoner::new(op.clone(), IndexTombstonerFilter::new(inner.clone())).await?;

        Ok(Self {
            refs: Default::default(),
            op,
            inner,
            tombstoner: RwLock::new(tombstoner),
            size: 0,
            last_modified: 0,
            access_count: 0,
            free_count: 0,
        })
    }

    async fn verify_version(reader: &mut Reader) -> anyhow::Result<()> {
        reader
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|e| anyhow!("init: error reading magic number of file: {}", e))?;

        let magic_number = reader
            .read_u32()
            .await
            .map_err(|e| anyhow!("init: error reading magic number of file: {}", e))?;
        if magic_number != MAGIC_NUMBER {
            return Err(anyhow!("can only read from tsm file"));
        }

        let version = reader
            .read_u8()
            .await
            .map_err(|e| anyhow!("init: error reading version: {}", e))?;
        if version != VERSION {
            return Err(anyhow!(
                "init: file is version {}. expected {}",
                version,
                VERSION
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl TSMReader for DefaultTSMReader<IndirectIndex, DefaultBlockAccessor> {
    fn path(&self) -> &str {
        self.op.path()
    }

    async fn block_iterator_builder<B>(&self) -> anyhow::Result<B>
    where
        B: AsyncIteratorBuilder,
    {
        // let op = self.op.reader().await?;
        // Ok(BlockIteratorBuilder::new(op, self.inner.clone()))
        todo!()
    }

    async fn read_block_at<T>(&self, entry: IndexEntry, values: &mut T) -> anyhow::Result<()>
    where
        T: BlockDecoder,
    {
        let mut reader = self.op.reader().await?;

        let mut block = vec![];
        self.inner
            .block()
            .read_block(&mut reader, entry, &mut block)
            .await?;
        values.decode(block.as_slice())?;

        Ok(())
    }

    async fn read_entries(&self, key: &[u8], entries: &mut IndexEntries) -> anyhow::Result<()> {
        let mut reader = self.op.reader().await?;
        self.inner.index().entries(&mut reader, key, entries).await
    }

    async fn contains(&mut self, key: &[u8]) -> anyhow::Result<bool> {
        let mut reader = self.op.reader().await?;
        self.inner.index().contains(&mut reader, key).await
    }

    async fn overlaps_time_range(&mut self, min: i64, max: i64) -> bool {
        self.inner.index().overlaps_time_range(min, max)
    }

    async fn time_range(&self) -> TimeRange {
        self.inner.index().time_range()
    }

    async fn tombstone_range(&self, key: &[u8]) -> Vec<TimeRange> {
        self.inner.index().tombstone_range(key).await
    }

    async fn key_range(&self) -> KeyRange {
        self.inner.index().key_range()
    }

    async fn key_count(&self) -> usize {
        self.inner.index().key_count().await
    }

    async fn key_iterator(&self) -> anyhow::Result<KeyIterator> {
        let reader = self.op.reader().await?;
        self.inner.index().key_iterator(reader).await
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<u64> {
        let mut reader = self.op.reader().await?;
        self.inner.index().seek(&mut reader, key).await
    }

    async fn key_at(&mut self, idx: usize) -> anyhow::Result<Option<(Vec<u8>, u8)>> {
        let mut reader = self.op.reader().await?;
        self.inner.index().key_at(&mut reader, idx).await
    }

    async fn block_type(&self, key: &[u8]) -> anyhow::Result<u8> {
        let mut reader = self.op.reader().await?;
        self.inner.index().block_type(&mut reader, key).await
    }

    async fn batch_delete(&mut self) -> Box<dyn BatchDeleter> {
        todo!()
    }

    async fn delete(&self, keys: &mut [&[u8]]) -> anyhow::Result<()> {
        let mut reader = self.op.reader().await?;
        self.inner.index().delete(&mut reader, keys).await
    }

    async fn delete_range(&self, keys: &mut [&[u8]], min: i64, max: i64) -> anyhow::Result<()> {
        let mut reader = self.op.reader().await?;
        self.inner
            .index()
            .delete_range(&mut reader, keys, min, max)
            .await
    }

    async fn has_tombstones(&self) -> anyhow::Result<bool> {
        let mut tombstoner = self.tombstoner.write().await;
        tombstoner.has_tombstones().await
    }

    async fn tombstone_stats(&self) -> anyhow::Result<TombstoneStat> {
        let mut tombstoner = self.tombstoner.write().await;
        tombstoner.tombstone_stats().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn size(&self) -> u32 {
        self.size
    }

    async fn remove(&mut self) -> anyhow::Result<()> {
        self.op.delete().await?;

        {
            let tombstoner = self.tombstoner.write().await;
            tombstoner.delete().await?;
        }
        Ok(())
    }

    async fn in_use(&self) -> bool {
        self.refs.load(Ordering::Relaxed) > 0
    }

    async fn use_ref(&mut self) {
        self.refs.fetch_and(1, Ordering::Relaxed);
    }

    async fn use_unref(&mut self) {
        self.refs.fetch_sub(1, Ordering::Relaxed);
    }

    async fn stats(&self) -> anyhow::Result<FileStat> {
        let i = self.inner.index();

        let time_range = i.time_range();
        let key_range = i.key_range();

        let has_tombstone = self.has_tombstones().await?;

        Ok(FileStat::new(
            self.path().to_string(),
            has_tombstone,
            self.size().await,
            0,
            time_range,
            key_range,
        ))
    }

    async fn free(&mut self) -> anyhow::Result<()> {
        self.inner.block().free().await
    }
}
