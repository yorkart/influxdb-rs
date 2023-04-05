#[macro_use]
extern crate async_trait;

use std::io;

pub mod mmap;

#[async_trait]
pub trait RandomAccessFile: Send + Sync {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    async fn close(self) -> io::Result<()>;
}
