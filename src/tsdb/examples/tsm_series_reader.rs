use clap::Parser;
use common_base::iterator::AsyncIterator;
use influxdb_storage::StorageOperator;
use influxdb_tsdb::series::series_segment::SeriesSegment;
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
    let segment = SeriesSegment::open(0, op, false).await?;

    let mut itr = segment.series_iterator(0).await?;
    let mut i = 0;
    while let Some((entry, offset, size)) = itr.try_next().await? {
        println!(
            "{:?}:{:06}>{:?} @ {}, {}",
            segment.version(),
            i,
            entry,
            offset,
            size
        );
        i += 1;
    }

    Ok(())
}
