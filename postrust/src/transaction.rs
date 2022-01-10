use crate::{
    protocol::backend::{self, TransactionStatus},
    tcp,
};
use futures_core::Stream;
use futures_util::stream::SplitStream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    /// Backend message stream that terminates at the end of a top-level transaction block
    pub struct Transaction<'a> {
        is_complete: bool,
        #[pin]
        backend_stream: &'a mut SplitStream<tcp::Connection<backend::Codec>>,
    }
}

impl<'a> Transaction<'a> {
    pub fn new(backend_stream: &'a mut SplitStream<tcp::Connection<backend::Codec>>) -> Self {
        Self {
            is_complete: false,
            backend_stream,
        }
    }
}

impl<'a> Stream for Transaction<'a> {
    type Item = Result<backend::Message, tcp::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_complete {
            return Poll::Ready(None);
        }

        let projected = self.project();

        match projected.backend_stream.poll_next(context) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Some(Ok(message))) => {
                if matches!(
                    message,
                    backend::Message::ReadyForQuery {
                        transaction_status: TransactionStatus::Idle
                    }
                ) {
                    *projected.is_complete = true;
                }

                Poll::Ready(Some(Ok(message)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
