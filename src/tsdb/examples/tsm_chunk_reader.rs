use std::str::from_utf8_unchecked;

use clap::Parser;
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
    let mut b = tsm_reader.block_iterator_builder().await?;

    let series = "cpu,host=server-09,region=uswest-00".as_bytes();
    let fields = vec!["value".as_bytes(), "value_1".as_bytes()];

    for field in fields.as_slice() {
        let key = series_field_key(series, field);
        let typ = tsm_reader.block_type(key.as_slice()).await?;

        let key = unsafe { from_utf8_unchecked(key.as_slice()) };
        println!("{}: {}", key, typ);
    }

    let mut v_itr = b.build(series, fields.as_slice()).await?;
    while let Some(chunk) = v_itr.try_next().await? {
        println!("{:?}", chunk);
    }

    Ok(())
}
