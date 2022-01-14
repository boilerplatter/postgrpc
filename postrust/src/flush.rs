use crate::{protocol::backend, tcp};
use futures_core::Stream;
use futures_util::stream::SplitStream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    /// Backend message stream that terminates at the next Pending state
    pub struct Flush<'a> {
        #[pin]
        backend_stream: &'a mut SplitStream<tcp::Connection<backend::Codec>>,
    }
}

impl<'a> Flush<'a> {
    pub fn new(backend_stream: &'a mut SplitStream<tcp::Connection<backend::Codec>>) -> Self {
        Self { backend_stream }
    }
}

impl<'a> Stream for Flush<'a> {
    type Item = Result<backend::Message, tcp::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();

        match projected.backend_stream.poll_next(context) {
            Poll::Ready(next) => Poll::Ready(next),
            Poll::Pending => Poll::Ready(None),
        }
    }
}
