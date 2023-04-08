#[macro_use]
extern crate async_trait;

use crate::wrapper::TokioWriter;

pub mod file;
pub mod wrapper;

pub mod opendal {
    pub use opendal::{Builder, Error, Operator, Reader, Result, Writer};

    pub mod services {
        pub use opendal::services::Fs;
    }

    pub mod layers {
        pub use opendal::layers::*;
    }
}

pub fn operator() -> std::io::Result<crate::opendal::Operator> {
    let mut builder = opendal::services::Fs::default();
    builder.enable_path_check();

    let operator = opendal::Operator::new(builder)?
        .layer(opendal::layers::LoggingLayer::default())
        .finish();

    Ok(operator)
}

pub async fn copy(
    reader: &mut crate::opendal::Reader,
    writer: &mut crate::opendal::Writer,
) -> std::io::Result<u64> {
    let mut writer = TokioWriter::new(writer);
    tokio::io::copy(reader, &mut writer).await
}

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
