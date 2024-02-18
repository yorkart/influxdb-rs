use std::ops::Range;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_base::iterator::AsyncIterator;
use influxdb_storage::StorageOperator;

use crate::engine::tsm1::file_store::reader::tsm_reader::{new_default_tsm_reader, TSMReader};

struct Group {
    series: Arc<Vec<Vec<u8>>>,
    series_range: Range<usize>,

    fields: Arc<Vec<Vec<u8>>>,
}

pub struct FileStoreIterator {
    tsm_files: Vec<Box<dyn AsyncIterator<Item = Chunk<Arc<dyn Array>>>>>,
}

impl FileStoreIterator {
    pub async fn new(
        tsm_readers: Vec<impl TSMReader>,
        _series: &[u8],
        _fields: &[Vec<u8>],
    ) -> anyhow::Result<Self> {
        for tsm_reader in &tsm_readers {
            let builder = tsm_reader.block_iterator_builder().await?;
            // builder.build()
        }

        Ok(Self { tsm_files: vec![] })
    }
}

pub struct FileStoreReader {
    tsm_files: Vec<StorageOperator>,
    tsm_readers: Vec<Box<dyn TSMReader>>,
}

impl FileStoreReader {
    pub async fn new(tsm_files: Vec<StorageOperator>) -> anyhow::Result<Self> {
        let mut tsm_readers = Vec::with_capacity(tsm_files.len());
        for tsm_file in &tsm_files {
            let tsm_reader = new_default_tsm_reader(tsm_file.clone()).await?;
            let tsm_reader: Box<dyn TSMReader> = Box::new(tsm_reader);
            tsm_readers.push(tsm_reader);
        }

        Ok(Self {
            tsm_files,
            tsm_readers,
        })
    }

    pub async fn query(&self, series: Vec<Vec<u8>>, fields: Vec<Vec<u8>>) {
        // Set parallelism by number of logical cpus.
        let mut parallelism = num_cpus::get();
        if parallelism > series.len() {
            parallelism = series.len();
        }

        let series = Arc::new(series);
        let fields = Arc::new(fields);

        // Group series keys.
        let mut groups = Vec::with_capacity(parallelism);
        let n = series.len() / parallelism;
        for i in 0..parallelism {
            let series_range = if i < parallelism - 1 {
                Range {
                    start: i * n,
                    end: (i + 1) * n,
                }
            } else {
                Range {
                    start: i * n,
                    end: series.len(),
                }
            };

            groups.push(Group {
                series: series.clone(),
                series_range,
                fields: fields.clone(),
            });
        }

        // // Read series groups in parallel.
        // for group in groups {
        //     tokio::spawn()
        // }
    }
}
