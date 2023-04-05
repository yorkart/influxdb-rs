#[macro_use]
extern crate async_trait;

use std::io;

pub mod mmap;

#[async_trait]
pub trait RandomAccessFile: Send + Sync {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    async fn close(self) -> io::Result<()>;
}

#[async_trait]
pub trait RandomAccessFileExt: RandomAccessFile {
    async fn read_u8(&self, offset: u64) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.read(offset, &mut buf).await?;
        Ok(buf[0])
    }

    async fn read_u16(&self, offset: u64) -> io::Result<u16> {
        let mut buf = [0; 2];
        self.read(offset, &mut buf).await?;
        Ok(u16::from_be_bytes(buf))
    }

    async fn read_u32(&self, offset: u64) -> io::Result<u32> {
        let mut buf = [0; 4];
        self.read(offset, &mut buf).await?;
        Ok(u32::from_be_bytes(buf))
    }

    async fn read_u64(&self, offset: u64) -> io::Result<u64> {
        let mut buf = [0; 8];
        self.read(offset, &mut buf).await?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl<R: RandomAccessFile + ?Sized> RandomAccessFileExt for R {}
