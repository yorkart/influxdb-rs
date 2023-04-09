use std::io;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};

use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use influxdb_storage::opendal::{Operator, Reader};
use influxdb_storage::Writer;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::engine::tsm1::file_store::reader::tsm_reader::TimeRange;
use crate::engine::CompactionTempExtension;

const TOMBSTONE_FILE_EXTENSION: &'static str = "tombstone";

const HEADER_SIZE: usize = 4;
const V4HEADER: u32 = 0x1504;

// Tombstone represents an individual deletion.
pub struct Tombstone {
    // Key is the tombstoned series key.
    key: Vec<u8>,

    // time_range are the min and max unix nanosecond time ranges of Key that are deleted.  If
    // the full range is deleted, both values are -1.
    time_range: TimeRange,
}

impl Tombstone {
    pub fn new(key: Vec<u8>, time_range: TimeRange) -> Self {
        Self { key, time_range }
    }
}

/// TombstoneStat holds information about a possible tombstone file on disk.
#[derive(Default, Clone)]
pub struct TombstoneStat {
    tombstone_exists: bool,
    path: String,
    last_modified: i64,
    size: u32,
}

// Tombstoner records tombstones when entries are deleted.
pub struct Tombstoner {
    op: Operator,
    tx: RwLock<Option<TombstoneTransaction>>,

    // Path is the location of the file to record tombstone. This should be the
    // full path to a TSM file.
    tombstone_path: PathBuf,

    filter_fn: Box<dyn Fn(&[u8]) -> bool>,

    // cache of the stats for this tombstone
    tombstone_stats: TombstoneStat,
    // indicates that the stats may be out of sync with what is on disk and they
    // should be refreshed.
    stats_loaded: bool,

    last_applied_offset: u64,
}

impl Tombstoner {
    pub async fn new(
        op: Operator,
        tsm_path: impl AsRef<Path>,
        filter_fn: Box<dyn Fn(&[u8]) -> bool>,
    ) -> anyhow::Result<Self> {
        let tombstone_path = Self::tombstone_path(tsm_path.as_ref().to_owned());
        Ok(Self {
            op,
            tx: RwLock::new(None),
            tombstone_path,
            filter_fn,
            tombstone_stats: TombstoneStat::default(),
            stats_loaded: false,
            last_applied_offset: 0,
        })
    }

    fn tombstone_path(tsm_path: PathBuf) -> PathBuf {
        // Filename is 0000001.tsm1
        let mut filename = tsm_path.file_name().unwrap().to_str().unwrap();

        if filename.ends_with(TOMBSTONE_FILE_EXTENSION) {
            return tsm_path;
        }

        // Strip off the tsm1
        if let Some(pos) = filename.rfind(".") {
            filename = &filename[..pos];
        }

        // Append the "tombstone" suffix to create a 0000001.tombstone file
        tsm_path
            .parent()
            .unwrap()
            .join(format!("{}.{}", filename, TOMBSTONE_FILE_EXTENSION))
    }

    pub async fn add(&mut self, keys: &[&[u8]]) -> anyhow::Result<()> {
        self.add_range(keys, TimeRange::unbound()).await
    }

    pub async fn add_range(&mut self, keys: &[&[u8]], time_range: TimeRange) -> anyhow::Result<()> {
        let mut filter_keys = keys;
        while filter_keys.len() > 0 && (self.filter_fn)(filter_keys[0]) {
            filter_keys = &filter_keys[1..];
        }

        if filter_keys.len() == 0 {
            return Ok(());
        }

        let mut tx = self.tx.write().await;
        if tx.is_none() {
            let new_tx =
                TombstoneTransaction::begin(self.op.clone(), self.tombstone_path.clone()).await?;
            tx.replace(new_tx);
        }
        let tx = tx.as_mut().unwrap();

        self.stats_loaded = false;

        for k in filter_keys {
            if !(self.filter_fn)(k) {
                continue;
            }
            tx.write_tombstone(Tombstone::new(k.to_vec(), time_range.clone()))
                .await?;
        }

        Ok(())
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let mut tx = self.tx.write().await;
        let tx = tx.take();
        if let Some(mut tx) = tx {
            let mut r = tx.commit().await;
            if r.is_err() {
                if let Err(r_e) = tx.rollback().await {
                    r = r.map_err(|e| anyhow!("commit err:{}, rollback err:{}", e, r_e));
                }
            }
            return r;
        }
        Ok(())
    }

    pub async fn rollback(&self) -> anyhow::Result<()> {
        let mut tx = self.tx.write().await;

        let tx = tx.take();
        if let Some(tx) = tx {
            tx.rollback().await?;
        }

        Ok(())
    }

    /// Delete removes all the tombstone files from disk.
    pub async fn delete(&self) -> anyhow::Result<()> {
        let mut tx = self.tx.write().await;

        let _ = self.op.delete(self.tombstone_path.to_str().unwrap()).await;

        let tx = tx.take();
        if let Some(tx) = tx {
            tx.rollback().await?;
        }

        Ok(())
    }

    /// has_tombstones return true if there are any tombstone entries recorded.
    pub async fn has_tombstones(&mut self) -> anyhow::Result<bool> {
        let stats = self.tombstone_stats().await?;
        if !stats.tombstone_exists {
            return Ok(false);
        }
        Ok(stats.size > 0)
    }

    /// TombstoneFiles returns any tombstone files associated with Tombstoner's TSM file.
    pub async fn tombstone_stats(&mut self) -> anyhow::Result<TombstoneStat> {
        {
            let _tx = self.tx.read().await;
            if self.stats_loaded {
                return Ok(self.tombstone_stats.clone());
            }
        }

        let _tx = self.tx.write().await;

        let tombstone_path = self.tombstone_path.to_str().unwrap();

        let exist = self.op.is_exist(tombstone_path).await?;
        if !exist {
            // The file doesn't exist so record that we tried to load it so
            // we don't continue to keep trying.  This is the common case.
            self.stats_loaded = true;
            self.tombstone_stats.tombstone_exists = false;
            return Ok(self.tombstone_stats.clone());
        }

        let meta = self.op.stat(self.tombstone_path.to_str().unwrap()).await?;
        self.tombstone_stats = TombstoneStat {
            tombstone_exists: true,
            path: tombstone_path.to_string(),
            last_modified: meta
                .last_modified()
                .map(|t| t.unix_timestamp_nanos())
                .unwrap_or_default() as i64,
            size: meta.content_length() as u32,
        };
        Ok(self.tombstone_stats.clone())
    }

    /// Walk calls fn for every Tombstone under the Tombstoner.
    /// TODO 使用iterator方式实现
    pub async fn walk<F>(&self, cb: F) -> anyhow::Result<()>
    where
        F: Fn(Tombstone) -> anyhow::Result<()>,
    {
        let _tx = self.tx.write().await;

        let tombstone_path = self.tombstone_path.to_str().unwrap();

        if !self.op.is_exist(tombstone_path).await? {
            return Ok(());
        }

        let mut reader = self.op.reader(tombstone_path).await?;
        reader.seek(SeekFrom::Start(0)).await?;

        let header = reader.read_u32().await?;
        if header != V4HEADER {
            return Err(anyhow!("unsupported Tombstone version: {}", header));
        }

        Ok(())
    }

    async fn read_tombstone_v4<F>(&self, reader: &mut Reader, cb: F) -> anyhow::Result<()>
    where
        F: Fn(Tombstone) -> anyhow::Result<()>,
    {
        let seek_from = if self.last_applied_offset > 0 {
            SeekFrom::Start(self.last_applied_offset)
        } else {
            SeekFrom::Start(HEADER_SIZE as u64)
        };
        reader.seek(seek_from).await.map_err(|e| anyhow!(e))?;

        let mut b = Vec::with_capacity(4096);

        let mut gr = GzipDecoder::new(tokio::io::BufReader::new(reader));
        loop {
            gr.multiple_members(false);

            loop {
                let key_len = gr.read_u32().await?;

                b.clear();
                if b.capacity() < key_len as usize {
                    b.reserve_exact(key_len as usize);
                }
                b.resize(key_len as usize, 0);

                gr.read_exact(b.as_mut_slice()).await?;

                let min = gr.read_u64().await? as i64;
                let max = gr.read_u64().await? as i64;

                cb(Tombstone {
                    key: b.to_vec(),
                    time_range: TimeRange { min, max },
                })?;
            }
        }

        Ok(())
    }
}

struct TombstoneTransaction {
    op: Operator,
    tombstone_path: String,
    tmp_path: String,

    tmp_gz: GzipEncoder<Writer>,
}

impl TombstoneTransaction {
    pub async fn begin(op: Operator, tombstone_path: PathBuf) -> anyhow::Result<Self> {
        let tmp_path = {
            let parent = tombstone_path.parent().unwrap();
            let file_name = tombstone_path.file_name().unwrap().to_str().unwrap();

            parent.join(format!("{}.{}", file_name, CompactionTempExtension))
        };

        let tmp_path = tmp_path.to_str().unwrap();
        let tombstone_path = tombstone_path.to_str().unwrap();

        let tmp_writer = Self::prepare(&op, tombstone_path, tmp_path).await?;
        let tmp_gz = GzipEncoder::new(tmp_writer);

        Ok(Self {
            op,
            tombstone_path: tombstone_path.to_string(),
            tmp_path: tmp_path.to_string(),
            tmp_gz,
        })
    }

    async fn prepare(op: &Operator, tombstone_path: &str, tmp_path: &str) -> io::Result<Writer> {
        match Self::prepare_v4(op, tombstone_path, tmp_path).await {
            Ok(writer) => Ok(writer),
            Err(e) => {
                op.delete(tmp_path).await?;
                Err(e)
            }
        }
    }
    async fn prepare_v4(op: &Operator, tombstone_path: &str, tmp_path: &str) -> io::Result<Writer> {
        let mut tmp_writer = {
            // ignore the old content in tmp
            let writer = op.writer(tmp_path).await?;
            Writer::new(writer)
        };

        let exist = op.is_exist(tombstone_path).await?;
        if exist {
            let mut reader = op.reader(tombstone_path).await?;

            // There is an existing tombstone on disk, and it's not a v3.  Just rewrite it as a v3
            // version again.
            let header = reader.read_u32().await?;
            if header != V4HEADER {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "incompatible v4 version",
                ));
            }

            let _ = reader.seek(SeekFrom::Start(0)).await?;
            tokio::io::copy(&mut reader, &mut tmp_writer).await?;
        } else {
            tmp_writer.write_u32(V4HEADER).await?;
        }

        Ok(tmp_writer)
    }

    pub async fn write_tombstone(&mut self, ts: Tombstone) -> anyhow::Result<()> {
        self.tmp_gz.write_u32(ts.key.len() as u32).await?;
        self.tmp_gz.write(ts.key.as_slice()).await?;
        self.tmp_gz.write_u64(ts.time_range.min as u64).await?;
        self.tmp_gz.write_u64(ts.time_range.max as u64).await?;
        Ok(())
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        self.tmp_gz.flush().await?;
        // fsync the file
        // opendal `writer.close().await` will execute `fsync`
        self.tmp_gz
            .get_mut()
            .close()
            .await
            .map_err(|e| anyhow!(e))?;

        // TODO rename file , waiting opendal update ...
        // self.tmp_gz.get_mut().rename().await?;

        // TODO sync dir
        // file.SyncDir(filepath.Dir(t.tombstonePath()));

        self.op.delete(self.tmp_path.as_str()).await?;
        Ok(())
    }

    pub async fn rollback(mut self) -> anyhow::Result<()> {
        self.tmp_gz.get_mut().close().await?;
        self.op.delete(self.tmp_path.as_str()).await?;
        Ok(())
    }
}
