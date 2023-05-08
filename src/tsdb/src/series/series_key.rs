use std::io::SeekFrom;
use std::ops::Deref;

use influxdb_common::point::{Tag, Tags};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::engine::tsm1::codec::varint::{VarInt, MAX_VARINT_LEN64};

#[derive(Clone)]
pub struct SeriesKey {
    name: Vec<u8>,
    tags: Tags,
}

impl SeriesKey {
    pub fn new(name: Vec<u8>, tags: Tags) -> Self {
        Self { name, tags }
    }

    pub fn size(&self) -> usize {
        let tags_len = self.tags.len();

        // Size of name/tags. Does not include total length.
        2 + // size of measurement
            self.name.len() + // measurement
            tags_len.required_space() + // size of number of tags
            (4 * tags_len) + // length of each tag key and value
            self.tags.size() // size of tag keys/values
    }

    pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
        let mut buf = [0; MAX_VARINT_LEN64];

        let size = self.size();
        let n = size.encode_var(&mut buf);
        w.write(&buf[..n]).await?;

        w.write_u16(self.name.len() as u16).await?;
        w.write(self.name.as_slice()).await?;

        let n = self.tags.len().encode_var(&mut buf);
        w.write(&buf[..n]).await?;

        for tag in self.tags.deref() {
            w.write_u16(tag.key.len() as u16).await?;
            w.write(tag.key.as_slice()).await?;

            w.write_u16(tag.value.len() as u16).await?;
            w.write(tag.value.as_slice()).await?;
        }

        Ok(())
    }

    pub async fn read_from<R: AsyncRead + AsyncSeek + Send + Unpin>(
        mut r: R,
    ) -> anyhow::Result<(Self, usize)> {
        let offset = r.seek(SeekFrom::Current(0)).await?;

        let mut buf = [0; MAX_VARINT_LEN64];
        r.read(buf.as_mut()).await?;

        let (sz, v_len) = u64::decode_var(buf.as_slice()).ok_or(anyhow!("varint parse error"))?;

        r.seek(SeekFrom::Start(offset + v_len as u64)).await?;

        let name_len = r.read_u16().await? as usize;
        let mut name = Vec::with_capacity(name_len);
        name.resize(name_len, 0);
        r.read(name.as_mut_slice()).await?;

        // todo 这里应该是varint类型， 但tags基本上1byte就可表示，不支持超过256个tag
        let tags_len = r.read_u8().await? as usize;

        let mut n = 0;
        let mut tags = Vec::with_capacity(tags_len);
        for i in 0..tags_len {
            n = r.read_u16().await? as usize;
            let mut key = Vec::with_capacity(n);
            key.resize(n, 0);
            r.read(key.as_mut_slice()).await?;

            n = r.read_u16().await? as usize;
            let mut value = Vec::with_capacity(n);
            value.resize(n, 0);
            r.read(value.as_mut_slice()).await?;

            tags.push(Tag::new(key, value));
        }

        Ok((Self {
            name,
            tags: Tags::new(tags),
        },))
    }
}
