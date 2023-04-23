#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde;

pub mod file;
pub mod wrapper;

pub mod opendal {
    pub use opendal::{
        Builder, Entry, EntryMode, Error, ErrorKind, Lister, Metadata, Operator, Reader, Result,
        Writer,
    };

    pub mod services {
        pub use opendal::services::Fs;
    }

    pub mod layers {
        pub use opendal::layers::*;
    }

    pub mod raw {
        pub use opendal::raw::*;
    }
}

pub type Writer = crate::wrapper::TokioWriter;

pub fn operator() -> std::io::Result<crate::opendal::Operator> {
    let mut builder = opendal::services::Fs::default();
    builder.root("/").enable_path_check();

    let operator = opendal::Operator::new(builder)?
        .layer(opendal::layers::LoggingLayer::default())
        .finish();

    Ok(operator)
}

/// Config for storage backend fs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFsConfig {
    pub root: String,
}

impl Default for StorageFsConfig {
    fn default() -> Self {
        Self {
            root: "_data".to_string(),
        }
    }
}

impl StorageFsConfig {
    /// init_fs_operator will init a opendal fs operator.
    pub fn to_operator(&self) -> std::io::Result<impl crate::opendal::Builder> {
        let mut builder = crate::opendal::services::Fs::default();

        let mut path = self.root.clone();
        if !path.starts_with('/') {
            path = std::env::current_dir()
                .unwrap()
                .join(path)
                .display()
                .to_string();
        }
        builder.root(&path);

        Ok(builder)
    }
}

pub fn build_operator<B: crate::opendal::Builder>(
    builder: B,
) -> std::io::Result<crate::opendal::Operator> {
    let ob = crate::opendal::Operator::new(builder)?;

    let op = ob
        // NOTE
        //
        // Magic happens here. We will add a layer upon original
        // storage operator so that all underlying storage operations
        // will send to storage runtime.
        // .layer(crate::opendal::layers::RuntimeLayer::new(GlobalIORuntime::instance().inner()))
        // Add retry
        .layer(crate::opendal::layers::RetryLayer::new().with_jitter())
        // Add metrics
        .layer(crate::opendal::layers::MetricsLayer)
        // Add logging
        .layer(crate::opendal::layers::LoggingLayer::default())
        // Add tracing
        .layer(crate::opendal::layers::TracingLayer)
        .finish();

    Ok(op)
}

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageParams {
    // Azblob(StorageAzblobConfig),
    Fs(StorageFsConfig),
    // Ftp(StorageFtpConfig),
    // Gcs(StorageGcsConfig),
    // #[cfg(feature = "storage-hdfs")]
    // Hdfs(StorageHdfsConfig),
    // Http(StorageHttpConfig),
    // Ipfs(StorageIpfsConfig),
    // Memory,
    // Moka(StorageMokaConfig),
    // Obs(StorageObsConfig),
    // Oss(StorageOssConfig),
    // S3(StorageS3Config),
    // Redis(StorageRedisConfig),
    // Webhdfs(StorageWebhdfsConfig),
    //
    // /// None means this storage type is none.
    // ///
    // /// This type is mostly for cache which mean bypass the cache logic.
    // None,
}

#[derive(Clone, Debug)]
pub struct StorageOperator {
    operator: crate::opendal::Operator,
    path: String,
}

impl StorageOperator {
    pub fn new(operator: crate::opendal::Operator, path: &str) -> Self {
        Self {
            operator,
            path: path.to_string(),
        }
    }

    pub fn operator(&self) -> crate::opendal::Operator {
        self.operator.clone()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub async fn reader(&self) -> crate::opendal::Result<crate::opendal::Reader> {
        self.operator.reader(self.path.as_str()).await
    }

    pub async fn writer(&self) -> crate::opendal::Result<crate::opendal::Writer> {
        self.operator.writer(self.path.as_str()).await
    }

    pub async fn delete(&self) -> crate::opendal::Result<()> {
        self.operator.delete(self.path.as_str()).await
    }

    pub async fn rename(&self, to: &str) -> crate::opendal::Result<()> {
        self.operator.rename(self.path.as_str(), to).await
    }

    pub async fn stat(&self) -> crate::opendal::Result<crate::opendal::Metadata> {
        self.operator.stat(self.path.as_str()).await
    }

    pub async fn exist(&self) -> crate::opendal::Result<bool> {
        if let Err(e) = self.stat().await {
            if let crate::opendal::ErrorKind::NotFound = e.kind() {
                Ok(false)
            } else {
                Err(e)
            }
        } else {
            Ok(true)
        }
    }

    pub async fn list(&self) -> crate::opendal::Result<crate::opendal::Lister> {
        self.operator.list(self.path.as_str()).await
    }

    pub async fn create_dir(&self) -> crate::opendal::Result<()> {
        self.operator.create_dir(self.path.as_str()).await
    }

    pub fn to_op(&self, new_path: &str) -> Self {
        Self {
            operator: self.operator.clone(),
            path: new_path.to_string(),
        }
    }

    pub fn to_tmp(&self, suffix: &str) -> Self {
        Self::new(
            self.operator(),
            format!("{}.{}", self.path.as_str(), suffix).as_str(),
        )
    }
}

pub type SharedStorageOperator = std::sync::Arc<StorageOperator>;

/// DataOperator is the operator to access persist data services.
///
/// # Notes
///
/// All data accessed via this operator will be persisted.
#[derive(Clone, Debug)]
pub struct DataOperator {
    operator: crate::opendal::Operator,
    params: StorageParams,
}

impl DataOperator {
    /// Get the operator from PersistOperator
    pub fn operator(&self) -> crate::opendal::Operator {
        self.operator.clone()
    }

    pub fn params(&self) -> StorageParams {
        self.params.clone()
    }
}

pub fn path_join(path1: &str, path2: &str) -> String {
    let path1 = if path1.ends_with("/") {
        &path1[0..path1.len() - 1]
    } else {
        path1
    };

    let path2 = if path2.starts_with("/") {
        &path2[1..path1.len()]
    } else {
        path2
    };

    format!("{}/{}", path1, path2)
}
// pub async fn copy(
//     reader: &mut crate::opendal::Reader,
//     writer: &mut crate::Writer,
// ) -> std::io::Result<u64> {
//     let mut writer = TokioWriter::new(writer);
//     tokio::io::copy(reader, &mut writer).await
// }

#[async_trait]
pub trait RandomAccess: Send + Sync {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize>;
    async fn close(self) -> std::io::Result<()>;
}

#[async_trait]
pub trait RandomAccessExt: RandomAccess {
    async fn read_u8(&self, offset: u64) -> std::io::Result<u8> {
        let mut buf = [0; 1];
        self.read(offset, &mut buf).await?;
        Ok(buf[0])
    }

    async fn read_u16(&self, offset: u64) -> std::io::Result<u16> {
        let mut buf = [0; 2];
        self.read(offset, &mut buf).await?;
        Ok(u16::from_be_bytes(buf))
    }

    async fn read_u32(&self, offset: u64) -> std::io::Result<u32> {
        let mut buf = [0; 4];
        self.read(offset, &mut buf).await?;
        Ok(u32::from_be_bytes(buf))
    }

    async fn read_u64(&self, offset: u64) -> std::io::Result<u64> {
        let mut buf = [0; 8];
        self.read(offset, &mut buf).await?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl<R: RandomAccess + ?Sized> RandomAccessExt for R {}

#[async_trait]
pub trait Writable: Send + Sync {
    async fn append(&mut self, data: &[u8]) -> std::io::Result<usize>;
    async fn flush(&mut self) -> std::io::Result<()>;
    async fn sync(&self) -> std::io::Result<()>;
}

#[async_trait]
pub trait WritableExt: Writable {
    async fn put_u8(&mut self, v: u8) -> std::io::Result<usize> {
        let data = [v];
        self.append(&data).await
    }

    async fn put_u16(&mut self, v: u16) -> std::io::Result<usize> {
        let data = v.to_be_bytes();
        self.append(&data).await
    }

    async fn put_u32(&mut self, v: u32) -> std::io::Result<usize> {
        let data = v.to_be_bytes();
        self.append(&data).await
    }

    async fn put_u64(&mut self, v: u64) -> std::io::Result<usize> {
        let data = v.to_be_bytes();
        self.append(&data).await
    }
}
