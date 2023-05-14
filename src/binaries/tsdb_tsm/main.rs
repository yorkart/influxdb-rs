use std::str::from_utf8_unchecked;

use clap::Parser;
use common_base::iterator::AsyncIterator;
use influxdb_storage::StorageOperator;
use influxdb_tsdb::engine::tsm1::block::BLOCK_FLOAT64;
use influxdb_tsdb::engine::tsm1::file_store::index::IndexEntries;
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

    let key = "cpu,host=server-09,region=uswest-00#!~#value";
    let typ = tsm_reader.block_type(key.as_bytes()).await?;
    println!("{}", typ);

    // value iterator
    {
        if typ == BLOCK_FLOAT64 {
            let mut v_itr = b.build_f64(key.as_bytes()).await?;
            while let Some(values) = v_itr.try_next().await? {
                for v in values {
                    println!("| {} | {:.2}|", v.unix_nano, v.value);
                }
            }
        }
    }

    // read_entries
    {
        let mut ie = IndexEntries::new(typ);
        tsm_reader.read_entries(key.as_bytes(), &mut ie).await?;
    }

    // key_iterator
    {
        let mut itr = tsm_reader.key_iterator().await?;
        let mut i = 0;
        while let Some(key) = itr.try_next().await? {
            let key = unsafe { from_utf8_unchecked(key.as_slice()) };
            println!("{:010}>{}", i, key);
            i += 1;
        }
    }

    Ok(())
}
