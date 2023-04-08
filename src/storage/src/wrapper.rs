use bytes::Bytes;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::AsyncWrite;

pub struct TokioWriter {
    w: crate::opendal::Writer,
}

impl TokioWriter {
    pub fn new(w: crate::opendal::Writer) -> Self {
        Self { w }
    }

    pub async fn append(&mut self, bs: impl Into<Bytes>) -> opendal::Result<()> {
        self.w.append(bs).await
    }

    pub async fn close(&mut self) -> opendal::Result<()> {
        self.w.close().await
    }
}

impl tokio::io::AsyncWrite for TokioWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.w).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.w).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.w).poll_close(cx)
    }
}
