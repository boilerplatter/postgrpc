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
    /// Backend message stream that terminates at any ReadyForQuery synchronization point
    pub struct Sync<'a, S = SplitStream<tcp::Connection<backend::Codec>>>
    where
        S: Stream<Item = Result<backend::Message, tcp::Error>>,
        S: Unpin
    {
        transaction_status: Option<TransactionStatus>,
        #[pin]
        backend_stream: &'a mut S,
    }
}

impl<'a, S> Sync<'a, S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    pub fn new(backend_stream: &'a mut S) -> Self {
        Self {
            transaction_status: None,
            backend_stream,
        }
    }

    pub fn transaction_status(&self) -> Option<TransactionStatus> {
        self.transaction_status
    }
}

impl<'a, S> Stream for Sync<'a, S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    type Item = Result<backend::Message, tcp::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.transaction_status.is_some() {
            return Poll::Ready(None);
        }

        let projected = self.project();

        match projected.backend_stream.poll_next(context) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(Some(Ok(message))) => {
                if let backend::Message::ReadyForQuery { transaction_status } = &message {
                    *projected.transaction_status = Some(*transaction_status);
                }

                Poll::Ready(Some(Ok(message)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Sync;
    use crate::{
        protocol::backend::{self, TransactionStatus},
        tcp,
    };
    use bytes::Bytes;
    use futures_util::{stream, TryStreamExt};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn terminates_on_ready_for_query() {
        // create a mock sync
        let first_message = backend::Message::Forward(Bytes::from_static(b"foo"));
        let termination_message = backend::Message::ReadyForQuery {
            transaction_status: TransactionStatus::Transaction,
        };
        let extra_message = backend::Message::Forward(Bytes::from_static(b"extra"));

        let mut messages = stream::iter([
            Ok::<_, tcp::Error>(first_message.clone()),
            Ok::<_, tcp::Error>(termination_message.clone()),
            Ok::<_, tcp::Error>(extra_message),
        ]);

        let mut sync = Sync::new(&mut messages);

        // verify a properly-framed Sync point
        let message = sync
            .try_next()
            .await
            .expect("Error fetching message from Sync")
            .expect("Transaction closed before ReadyForQuery");

        assert_eq!(
            message, first_message,
            "Expected {first_message:?} first, but found {message:?}"
        );

        let message = sync
            .try_next()
            .await
            .expect("Error fetching message from Sync")
            .expect("Sync closed before ReadyForQuery");

        assert_eq!(
            message, termination_message,
            "Expected {first_message:?} next, but found {message:?}"
        );

        let sync_end = sync
            .try_next()
            .await
            .expect("Error fetching message from Sync");

        assert!(
            sync_end.is_none(),
            "Expected sync termination, but found {sync_end:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn tracks_final_transaction_status() {
        // create a mock sync
        let transaction_status = TransactionStatus::Idle;
        let first_message = backend::Message::Forward(Bytes::from_static(b"foo"));
        let termination_message = backend::Message::ReadyForQuery { transaction_status };
        let extra_message = backend::Message::Forward(Bytes::from_static(b"extra"));

        let mut messages = stream::iter([
            Ok::<_, tcp::Error>(first_message),
            Ok::<_, tcp::Error>(termination_message),
            Ok::<_, tcp::Error>(extra_message),
        ]);

        let mut sync = Sync::new(&mut messages);

        // verify that transaction status is properly tracked
        assert!(
            sync.transaction_status().is_none(),
            "Unexpected transaction status"
        );

        while let Some(message) = sync
            .try_next()
            .await
            .expect("Error fetching message from Sync")
        {
            tracing::debug!(?message, "Message drained from Sync");
        }

        let tracked_transaction_status = sync
            .transaction_status()
            .expect("Failed to track transaction status");

        assert_eq!(
            tracked_transaction_status, transaction_status,
            "Expected {transaction_status:?}, but found {tracked_transaction_status:?}"
        );
    }
}
