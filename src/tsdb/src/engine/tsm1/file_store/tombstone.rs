use std::io;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};

use influxdb_storage::opendal;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::engine::tsm1::file_store::reader::tsm_reader::TimeRange;
use crate::engine::CompactionTempExtension;

const TombstoneFileExtension: &'static str = "tombstone";

const headerSize: usize = 4;
const v2header: u32 = 0x1502;
const v3header: u32 = 0x1503;
const v4header: u32 = 0x1504;

// Tombstone represents an individual deletion.
pub struct Tombstone {
    // Key is the tombstoned series key.
    key: Vec<u8>,

    // time_range are the min and max unix nanosecond time ranges of Key that are deleted.  If
    // the full range is deleted, both values are -1.
    time_range: TimeRange,
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

    f: Option<File>,
    tmp: [u8; 8],
    last_applied_offset: i64,
}

impl Tombstoner {
    pub fn new(tsm_path: impl AsRef<Path>, filter_fn: Box<dyn Fn(&[u8]) -> bool>) -> Self {
        let tombstone_path = Self::tombstone_path(tsm_path.as_ref().to_owned());
        Self {
            tsm_path: tsm_path.as_ref().to_owned(),
            filter_fn,
            tombstones: Vec::new(),
            tombstone_stats: TombstoneStat::default(),
            stats_loaded: false,
            f: None,
            tmp: [0; 8],
            last_applied_offset: 0,
        }
    }

    fn tombstone_path(tsm_path: PathBuf) -> PathBuf {
        // Filename is 0000001.tsm1
        let mut filename = tsm_path.file_name().unwrap().to_str().unwrap();

        if filename.ends_with(TombstoneFileExtension) {
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
            .join(format!("{}.{}", filename, TombstoneFileExtension))
    }

    async fn prepare_v4(tombstone_path: PathBuf) -> io::Result<()> {
        let parent = tombstone_path.parent().unwrap();
        let file_name = tombstone_path.file_name().unwrap().to_str().unwrap();

        let operator = influxdb_storage::operator()?;

        let mut tmp_writer = {
            let tmp_path = parent.join(format!("{}.{}", file_name, CompactionTempExtension));
            let writer = operator.writer(tmp_path.to_str().unwrap()).await?;
            writer
        };

        let tombstone_path = tombstone_path.to_str().unwrap();
        let exist = operator.is_exist(tombstone_path).await?;
        if exist {
            let mut reader = operator.reader(tombstone_path).await?;

            // There is an existing tombstone on disk, and it's not a v3.  Just rewrite it as a v3
            // version again.
            let header = reader.read_u32().await?;
            if header != v4header {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "incompatible v4 version",
                ));
            }

            let _ = reader.seek(SeekFrom::Start(0)).await?;
            influxdb_storage::copy(&mut reader, &mut tmp_writer).await?;
        } else {
            tmp_writer.tmp_writer.put_u32().await;
        }

        // let mut reader = {
        //     let mut builder = opendal::services::Fs::default();
        //     builder.enable_path_check();
        //
        //     let op = opendal::Operator::new(builder)?
        //         .layer(opendal::layers::LoggingLayer::default())
        //         .finish();
        //
        //     let exist = op.is_exist(tombstone_path.to_str().unwrap()).await?;
        //     if !exist {
        //         return Err(io::Error::new(
        //             ErrorKind::NotFound,
        //             "tombstone file not exist",
        //         ));
        //     }
        //
        //     let reader = op.reader(tombstone_path.to_str().unwrap()).await?;
        //     reader
        // };
        //
        // let header = reader.read_u32().await?;
        // // There is an existing tombstone on disk, and it's not a v3.  Just rewrite it as a v3
        // // version again.
        // if header != v4header {
        //     return Err(io::Error::new(
        //         ErrorKind::InvalidData,
        //         "incompatible v4 version",
        //     ));
        // }
        //
        // let _ = reader.seek(SeekFrom::Start(0)).await?;
        // influxdb_storage::copy(&mut reader, &mut tmp_writer).await?;

        // let tmp_path = parent.join(format!("{}.{}", file_name, CompactionTempExtension));
        // let mut f = OpenOptions::new()
        //     .create_new(true)
        //     .truncate(true)
        //     .write(true)
        //     .read(true)
        //     .open(tmp_path)
        //     .await?;
        // let metadata = f.metadata().await?;
        // if metadata.len() > 0 {
        //     let header = f.read_u32().await?;
        //
        //     // There is an existing tombstone on disk, and it's not a v3.  Just rewrite it as a v3
        //     // version again.
        //     if header != v4header {
        //         return Err(std::io::Error::new(
        //             ErrorKind::InvalidData,
        //             "incompatible v4 version",
        //         ));
        //     }
        //
        //     let _ = f.seek(SeekFrom::Start(0)).await?;
        // }
        Ok(())
    }

    pub fn add(&mut self, keys: &[&[u8]]) -> anyhow::Result<()> {
        self.add_range(keys, TimeRange::unbound())
    }

    pub fn add_range(&mut self, keys: &[&[u8]], time_range: TimeRange) -> anyhow::Result<()> {
        let mut filter_keys = keys;
        while filter_keys.len() > 0 && (self.filter_fn)(filter_keys[0]) {
            filter_keys = &filter_keys[1..];
        }

        if filter_keys.len() == 0 {
            return Ok(());
        }

        Ok(())
    }
}
