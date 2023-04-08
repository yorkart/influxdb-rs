use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::AsyncWrite;

pub(crate) struct TokioWriter<'a> {
    w: &'a mut crate::opendal::Writer,
}

impl<'a> TokioWriter<'a> {
    pub fn new(w: &'a mut crate::opendal::Writer) -> Self {
        Self { w }
    }
}

impl<'a> tokio::io::AsyncWrite for TokioWriter<'a> {
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
