use std::str::from_utf8_unchecked;

use clap::Parser;
use common_arrow::arrow::io::print::write;
use common_base::point::series_field_key;
use influxdb_storage::StorageOperator;
use influxdb_tsdb::engine::tsm1::file_store::reader::tsm_reader::new_default_tsm_reader;
use influxdb_tsdb::engine::tsm1::file_store::reader::tsm_reader::TSMReader;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
struct Config {
    #[clap(long)]
    pub path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    println!("config: {:?}", config);
    if config.path.is_empty() {
        println!("path MUST not be empty!");
        return Ok(());
    }

    let op = StorageOperator::root(config.path.as_str())?;
    let tsm_reader = new_default_tsm_reader(op).await?;
    let b = tsm_reader.block_iterator_builder().await?;

    let series = "cpu,host=server-0,location=us-west";
    let fields = vec!["value"];

    for field in fields.as_slice() {
        let key = series_field_key(series.as_bytes(), field.as_bytes());
        let typ = tsm_reader.block_type(key.as_slice()).await?;

        let key = unsafe { from_utf8_unchecked(key.as_slice()) };
        println!("{}: {}", key, typ);
    }

    let fs: Vec<&[u8]> = fields.iter().map(|f| f.as_bytes()).collect();
    let mut chunk_itr = b.build(series.as_bytes(), fs.as_slice()).await?;
    let mut chunks = Vec::new();
    while let Some(chunk) = chunk_itr.try_next().await? {
        println!("chunk len: {:?}", chunk.len());
        chunks.push(chunk);
    }
    let table_str = write(chunks.as_slice(), &["timestamp", &fields[0]]);
    println!("{}", &table_str[..1000]);

    Ok(())
}
