use clap::Parser;
use influxdb_storage::StorageOperator;
use influxdb_tsdb::engine::tsm1::file_store::reader::tsm_reader::new_default_tsm_reader;
use influxdb_tsdb::engine::tsm1::file_store::reader::tsm_reader::TSMReader;
use influxdb_tsdb::engine::tsm1::value::{Array, FloatValues};
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
    let field_reader = tsm_reader.block_iterator_builder().await?;

    let key = "cpu,host=server-0,location=us-west#!~#value";
    let typ = tsm_reader.block_type(key.as_bytes()).await?;
    println!("{}: {}", key, typ);

    let array = FloatValues::new();
    let mut array: Box<dyn Array> = Box::new(array);
    let mut chunk_itr = field_reader.read(key.as_bytes()).await?;
    while let Some(_) = chunk_itr.try_next(&mut array).await? {
        println!("chunk len: {:?}, {:?}", array.len(), array,);
    }

    Ok(())
}
