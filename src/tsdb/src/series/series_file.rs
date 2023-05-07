use std::io::SeekFrom;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::engine::tsm1::codec::varint::{VarInt, MAX_VARINT_LEN64};

// SERIES_FILE_PARTITION_N is the number of partitions a series file is split into.
pub(crate) const SERIES_FILE_PARTITION_N: usize = 8;

/// read_series_key returns the series key from the beginning of the buffer.
pub async fn read_series_key<R: AsyncRead + AsyncSeek + Send + Unpin>(
    mut r: R,
) -> anyhow::Result<(Vec<u8>, usize)> {
    let offset = r.seek(SeekFrom::Current(0)).await?;

    let mut buf = [0; MAX_VARINT_LEN64];
    r.read(buf.as_mut()).await?;

    let (sz, v_len) = u64::decode_var(buf.as_slice()).ok_or(anyhow!("varint parse error"))?;

    r.seek(SeekFrom::Start(offset + v_len as u64)).await?;

    let mut key = Vec::with_capacity(sz as usize);
    key.resize(sz as usize, 0);
    let k_len = r.read(key.as_mut()).await?;
    if k_len != sz as usize {
        return Err(anyhow!("not enough data for series key"));
    }

    Ok((key, v_len + k_len))
}
