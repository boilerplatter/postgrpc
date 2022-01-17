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
    pub struct Transaction<'a, S = SplitStream<tcp::Connection<backend::Codec>>>
    where
        S: Stream<Item = Result<backend::Message, tcp::Error>>,
        S: Unpin
    {
        is_complete: bool,
        #[pin]
        backend_stream: &'a mut S,
    }
}

impl<'a, S> Transaction<'a, S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    pub fn new(backend_stream: &'a mut S) -> Self {
        Self {
            is_complete: false,
            backend_stream,
        }
    }
}

impl<'a, S> Stream for Transaction<'a, S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
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
