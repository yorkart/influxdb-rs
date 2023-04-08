use async_compression::tokio::write::GzipEncoder;
use std::io;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};

use influxdb_storage::opendal::{Builder, Operator};
use influxdb_storage::Writer;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::engine::tsm1::file_store::reader::tsm_reader::TimeRange;
use crate::engine::tsm1::file_store::TMP_TSMFILE_EXTENSION;
use crate::engine::CompactionTempExtension;

const TOMBSTONE_FILE_EXTENSION: &'static str = "tombstone";

const headerSize: usize = 4;
const v2header: u32 = 0x1502;
const v3header: u32 = 0x1503;
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
#[derive(Default)]
pub struct TombstoneStat {
    tombstone_exists: bool,
    path: String,
    last_modified: i64,
    size: u32,
}

// Tombstoner records tombstones when entries are deleted.
pub struct Tombstoner {
    op: Operator,

    // Path is the location of the file to record tombstone. This should be the
    // full path to a TSM file.
    tsm_path: PathBuf,

    filter_fn: Box<dyn Fn(&[u8]) -> bool>,

    // Tombstones that have been written but not flushed to disk yet.
    tombstones: Vec<Tombstone>,
    // cache of the stats for this tombstone
    tombstone_stats: TombstoneStat,
    // indicates that the stats may be out of sync with what is on disk and they
    // should be refreshed.
    stats_loaded: bool,

    tmp_gz: GzipEncoder<Writer>,
    tmp: [u8; 8],
    last_applied_offset: i64,
}

impl Tombstoner {
    pub async fn new(
        op: Operator,
        tsm_path: impl AsRef<Path>,
        filter_fn: Box<dyn Fn(&[u8]) -> bool>,
    ) -> anyhow::Result<Self> {
        let tombstone_path = Self::tombstone_path(tsm_path.as_ref().to_owned());
        let tmp_writer = Self::prepare(tombstone_path, &op).await?;
        let tmp_gz = GzipEncoder::new(tmp_writer);
        Ok(Self {
            op,
            tsm_path: tsm_path.as_ref().to_owned(),
            filter_fn,
            tombstones: Vec::new(),
            tombstone_stats: TombstoneStat::default(),
            stats_loaded: false,
            tmp_gz,
            tmp: [0; 8],
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

    async fn prepare(tombstone_path: PathBuf, op: &Operator) -> io::Result<Writer> {
        let tmp_path = {
            let parent = tombstone_path.parent().unwrap();
            let file_name = tombstone_path.file_name().unwrap().to_str().unwrap();

            parent.join(format!("{}.{}", file_name, CompactionTempExtension))
        };
        let tmp_path = tmp_path.to_str().unwrap();
        let tombstone_path = tombstone_path.to_str().unwrap();

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

        self.stats_loaded = false;

        // if self.tombstones.capacity() < self.tombstones.len() + filter_keys.len() {
        //     self.tombstones
        //         .reserve_exact(self.tombstones.len() + filter_keys.len() - self.tombstones.len());
        // }

        for k in filter_keys {
            if !(self.filter_fn)(k) {
                continue;
            }
            self.write_tombstone(Tombstone::new(k.to_vec(), time_range.clone()))
                .await?;
        }

        Ok(())
    }

    async fn write_tombstone(&mut self, ts: Tombstone) -> anyhow::Result<()> {
        self.tmp_gz.write_u32(ts.key.len() as u32).await?;
        self.tmp_gz.write(ts.key.as_slice()).await?;
        self.tmp_gz.write_u64(ts.time_range.min as u64).await?;
        self.tmp_gz.write_u64(ts.time_range.max as u64).await?;
        Ok(())
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
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

        Ok(())
    }

    async fn rollback(&mut self) -> anyhow::Result<()> {
        self.tmp_gz.get_mut().close().await?;
        // self.op.delete(self.tmp_gz.get_mut().pa)
    }
}

struct TombstoneTransaction {
    op: Operator,
    tombstone_path: String,
    tmp_path: String,

    tmp_gz: GzipEncoder<Writer>,
}

impl TombstoneTransaction {
    pub async fn begin(op: Operator, tombstone_path: PathBuf) -> Self {
        let tmp_path = {
            let parent = tombstone_path.parent().unwrap();
            let file_name = tombstone_path.file_name().unwrap().to_str().unwrap();

            parent.join(format!("{}.{}", file_name, CompactionTempExtension))
        };

        let tmp_path = tmp_path.to_str().unwrap();
        let tombstone_path = tombstone_path.to_str().unwrap();

        let tmp_writer = Self::prepare(&op, tombstone_path, tmp_path).await?;
        let tmp_gz = GzipEncoder::new(tmp_writer);

        Self {
            op,
            tombstone_path: tombstone_path.to_string(),
            tmp_path: tmp_path.to_string(),
            tmp_gz,
        }
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

    pub async fn commit(mut self) -> anyhow::Result<()> {
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
        self.op.delete(self.tmp_gz.get_mut().pa)
    }
}
