use std::io::ErrorKind;
use std::path::Path;
use std::{io, ptr};

use memmap2::{Mmap, MmapOptions};
use tokio::fs::File;

use crate::RandomAccessFile;

pub struct MmapReadableFile {
    f: File,
    len: usize,
    mmap: Mmap,
}

impl MmapReadableFile {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let f = File::open(path).await?;

        let meta = f.metadata().await?;
        let len = meta.len() as usize;

        let mmap = unsafe { MmapOptions::new().offset(0).len(len).map(&f)? };

        Ok(Self { f, len, mmap })
    }
}

#[async_trait]
impl RandomAccessFile for MmapReadableFile {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let size = buf.len();
        if size == 0 {
            return Ok(0);
        }

        let offset = offset as usize;
        let upper = offset + size;
        if upper > self.len {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, ""));
        }

        let data = &self.mmap[offset..offset + size];
        unsafe {
            ptr::copy(data.as_ptr(), buf.as_mut_ptr(), size);
        }

        Ok(size)
    }

    async fn close(self) -> io::Result<()> {
        drop(self.f);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::fs::File;
    use tokio::io;
    use tokio::io::AsyncWriteExt;

    use crate::mmap::MmapReadableFile;
    use crate::RandomAccessFile;

    #[tokio::test]
    async fn test_mmap_readable_file() -> io::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let tsm_file = dir.as_ref().join("tsm1_test");

        let data = "0123456789".as_bytes();
        let len = {
            let mut f = File::create(&tsm_file).await?;
            let len = f.write(data).await?;
            f.sync_all().await?;
            drop(f);
            len
        };

        let accessor = MmapReadableFile::open(&tsm_file).await?;

        let mut buf = Vec::with_capacity(len);
        buf.resize(len, 0_u8);

        accessor.read(0, &mut buf[..]).await?;
        assert_eq!(buf.as_slice(), data);

        Ok(())
    }
}
