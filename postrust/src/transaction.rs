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

#[cfg(test)]
mod test {
    use super::Transaction;
    use crate::{
        protocol::backend::{self, TransactionStatus},
        tcp,
    };
    use bytes::Bytes;
    use futures_util::{stream, TryStreamExt};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn terminates_on_idle_transaction_status() {
        // create a mock transaction
        let first_message = backend::Message::Forward(Bytes::from_static(b"foo"));
        let termination_message = backend::Message::ReadyForQuery {
            transaction_status: TransactionStatus::Idle,
        };
        let extra_message = backend::Message::Forward(Bytes::from_static(b"extra"));

        let mut messages = stream::iter([
            Ok::<_, tcp::Error>(first_message.clone()),
            Ok::<_, tcp::Error>(termination_message.clone()),
            Ok::<_, tcp::Error>(extra_message),
        ]);

        let mut transaction = Transaction::new(&mut messages);

        // verify a properly-framed transaction
        let message = transaction
            .try_next()
            .await
            .expect("Error fetching message from Transaction")
            .expect("Transaction closed before ReadyForQuery");

        assert_eq!(
            message, first_message,
            "Expected {first_message:?} first, but found {message:?}"
        );

        let message = transaction
            .try_next()
            .await
            .expect("Error fetching message from Transaction")
            .expect("Transaction closed before ReadyForQuery");

        assert_eq!(
            message, termination_message,
            "Expected {first_message:?} next, but found {message:?}"
        );

        let transaction_end = transaction
            .try_next()
            .await
            .expect("Error fetching message from Transaction");

        assert!(
            transaction_end.is_none(),
            "Expected transaction termination, but found {transaction_end:?}"
        );
    }
}
