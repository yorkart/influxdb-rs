use std::fmt::{Debug, Formatter};
use std::io::{Cursor, SeekFrom};
use std::str::from_utf8_unchecked;

use bytes::Buf;
use crc32fast::Hasher;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::engine::tsm1::codec::varint::{VarInt, MAX_VARINT_LEN64};

/// read_series_key returns the series key from the beginning of the buffer.
pub async fn read_series_key<R: AsyncRead + AsyncSeek + Send + Unpin>(
    r: &mut R,
    h: &mut Hasher,
) -> anyhow::Result<(Vec<u8>, usize)> {
    let offset = r.seek(SeekFrom::Current(0)).await?;

    let mut buf = [0; MAX_VARINT_LEN64];
    r.read(buf.as_mut()).await?;

    let (sz, v_len) = u64::decode_var(buf.as_slice()).ok_or(anyhow!("varint parse error"))?;
    h.update(&buf[..v_len]);

    r.seek(SeekFrom::Start(offset + v_len as u64)).await?;

    let mut key = Vec::with_capacity(sz as usize);
    key.resize(sz as usize, 0);
    let k_len = r.read(key.as_mut()).await?;
    if k_len != sz as usize {
        return Err(anyhow!("not enough data for series key"));
    }
    h.update(&key[..k_len]);

    Ok((key, v_len + k_len))
}

#[derive(Clone)]
pub struct SeriesKeyDecoder<'a> {
    name: &'a [u8],

    tag_size: usize,
    tags: &'a [u8],
}

impl<'a> SeriesKeyDecoder<'a> {
    pub fn new(series_key: &'a [u8]) -> Self {
        let mut n = 0_usize;

        let mut cur = Cursor::new(series_key);
        if cur.remaining() < 2 {
            panic!("xxx")
        }
        let name_len = cur.get_u16() as usize;
        n += 2;
        let name = &series_key[n..n + name_len];
        n += name_len;

        let (tag_size, v_len) = u64::decode_var(&series_key[n..]).unwrap();
        n += v_len;
        let tags = &series_key[n..];

        Self {
            name,
            tag_size: tag_size as usize,
            tags,
        }
    }

    pub fn tags_iterator(&self) -> TagsIterator {
        TagsIterator::new(self.tag_size, self.tags)
    }
}

impl<'a> Debug for SeriesKeyDecoder<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut itr = self.tags_iterator();
        let mut n = f.debug_list();

        let name = unsafe { from_utf8_unchecked(&self.name) };
        n.entry(&format!("__name__: {}", name));
        while let Some((k, v)) = itr.next().unwrap() {
            let k = unsafe { from_utf8_unchecked(k) };
            let v = unsafe { from_utf8_unchecked(v) };
            if k.len() == 0 || v.len() == 0 {
                println!("for debug: k or v is empty");
                panic!("k or v is empty");
            }
            n.entry(&format!("{}: {}", k, v));
        }

        n.finish()
    }
}

pub struct TagsIterator<'a> {
    i: usize,
    tag_size: usize,
    tags: Cursor<&'a [u8]>,
}

impl<'a> TagsIterator<'a> {
    pub fn new(tag_size: usize, tags: &'a [u8]) -> Self {
        Self {
            i: 0,
            tag_size,
            tags: Cursor::new(tags),
        }
    }

    pub fn next(&mut self) -> anyhow::Result<Option<(&[u8], &[u8])>> {
        if self.i >= self.tag_size {
            return Ok(None);
        }

        let key_len = self.tags.get_u16() as usize;
        if self.tags.remaining() < key_len {
            return Err(anyhow!("not enough key to read"));
        }
        let pos = self.tags.position() as usize;
        let advance = pos + key_len;
        let key = &self.tags.get_ref()[pos..advance];
        self.tags.set_position(advance as u64);

        let value_len = self.tags.get_u16() as usize;
        if self.tags.remaining() < value_len {
            return Err(anyhow!("not enough value to read"));
        }
        let pos = self.tags.position() as usize;
        let advance = pos + value_len;
        let value = &self.tags.get_ref()[pos..advance];
        self.tags.set_position(advance as u64);

        self.i += 1;

        Ok(Some((key, value)))
    }
}

// #[derive(Clone)]
// pub struct SeriesKey {
//     name: Bytes,
//     tags: Tags,
// }
//
// impl Debug for SeriesKey {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         let name = unsafe { from_utf8_unchecked(self.name.deref()) };
//         f.debug_struct("SeriesKey")
//             .field("name", &name)
//             .field("tags", &self.tags)
//             .finish()
//     }
// }
//
// impl SeriesKey {
//     pub fn new(name: Bytes, tags: Tags) -> Self {
//         Self { name, tags }
//     }
//
//     pub fn size(&self) -> usize {
//         let tags_len = self.tags.len();
//
//         // Size of name/tags. Does not include total length.
//         2 + // size of measurement
//             self.name.len() + // measurement
//             tags_len.required_space() + // size of number of tags
//             (4 * tags_len) + // length of each tag key and value
//             self.tags.size() // size of tag keys/values
//     }
//
//     pub async fn write_to<W: AsyncWrite + Send + Unpin>(&self, mut w: W) -> anyhow::Result<()> {
//         let mut buf = [0; MAX_VARINT_LEN64];
//
//         let size = self.size();
//         let n = size.encode_var(&mut buf);
//         w.write(&buf[..n]).await?;
//
//         w.write_u16(self.name.len() as u16).await?;
//         w.write(self.name.deref()).await?;
//
//         let n = self.tags.len().encode_var(&mut buf);
//         w.write(&buf[..n]).await?;
//
//         for tag in self.tags.deref() {
//             w.write_u16(tag.key.len() as u16).await?;
//             w.write(tag.key.deref()).await?;
//
//             w.write_u16(tag.value.len() as u16).await?;
//             w.write(tag.value.deref()).await?;
//         }
//
//         Ok(())
//     }
//
//     pub async fn read_from<R: AsyncRead + AsyncSeek + Send + Unpin>(
//         mut r: R,
//     ) -> anyhow::Result<(Self, usize)> {
//         let offset = r.seek(SeekFrom::Current(0)).await?;
//
//         let mut buf = [0; MAX_VARINT_LEN64];
//         r.read(buf.as_mut()).await?;
//
//         let (sz, v_len) = u64::decode_var(buf.as_slice()).ok_or(anyhow!("varint parse error"))?;
//
//         r.seek(SeekFrom::Start(offset + v_len as u64)).await?;
//         let mut body = Vec::with_capacity(sz as usize);
//         body.resize(sz as usize, 0);
//         r.read(body.as_mut_slice()).await?;
//
//         let mut body = Bytes::from(body);
//         let name_len = body.get_u16() as usize;
//         let name = body.split_to(name_len);
//
//         let (tags_len, v_len) =
//             u64::decode_var(body.deref()).ok_or(anyhow!("varint parse error"))?;
//         let _ = body.split_to(v_len);
//
//         let tags_len = tags_len as usize;
//         let mut tags = Vec::with_capacity(tags_len);
//         for _ in 0..tags_len {
//             let key_len = body.get_u16() as usize;
//             let key = body.split_to(key_len);
//
//             let value_len = body.get_u16() as usize;
//             let value = body.split_to(value_len);
//
//             tags.push(Tag::new(key, value));
//         }
//
//         Ok((
//             Self {
//                 name,
//                 tags: Tags::new(tags),
//             },
//             sz as usize + v_len,
//         ))
//     }
// }
