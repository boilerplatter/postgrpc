use crate::{protocol::backend, tcp};
use futures_core::Stream;
use futures_util::stream::SplitStream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    /// Backend message stream that terminates at the next Pending state
    pub struct Flush<'a, S = SplitStream<tcp::Connection<backend::Codec>>>
    where
        S: Stream<Item = Result<backend::Message, tcp::Error>>,
        S: Unpin,
    {
        #[pin]
        backend_stream: &'a mut S,
    }
}

impl<'a, S> Flush<'a, S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    pub fn new(backend_stream: &'a mut S) -> Self {
        Self { backend_stream }
    }
}

impl<'a, S> Stream for Flush<'a, S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    type Item = Result<backend::Message, tcp::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();

        match projected.backend_stream.poll_next(context) {
            Poll::Ready(next) => Poll::Ready(next),
            Poll::Pending => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Flush;
    use crate::{protocol::backend, tcp};
    use bytes::Bytes;
    use futures_core::Stream;
    use futures_util::{StreamExt, TryStreamExt};
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    // the point at which the mock stream should return Poll::Pending
    static PENDING_POINT: usize = 2;

    pin_project_lite::pin_project! {
        struct PendingStream {
            message: backend::Message,
            iteration: usize,
        }
    }

    impl Default for PendingStream {
        fn default() -> Self {
            let message = backend::Message::Forward(Bytes::from_static(b"foo"));
            let iteration = 0;

            Self { message, iteration }
        }
    }

    impl Stream for PendingStream {
        type Item = Result<backend::Message, tcp::Error>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context) -> Poll<Option<Self::Item>> {
            let projection = self.project();

            let next = if *projection.iteration == PENDING_POINT {
                Poll::Pending
            } else {
                Poll::Ready(Some(Ok(projection.message.clone())))
            };

            *projection.iteration += 1;

            next
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn terminates_on_pending() {
        // create a mock flush
        let mut messages = PendingStream::default();
        let flush = Flush::new(&mut messages);

        // verify that Flush terminates on Poll::Pending
        let flush_end = flush
            .skip(PENDING_POINT)
            .try_next()
            .await
            .expect("Error fetching message from Flush");

        assert!(
            flush_end.is_none(),
            "Expected flush termination, but found {flush_end:?}"
        );

        // verify that the original stream is unaffected
        let next_message = messages
            .try_next()
            .await
            .expect("Error fetching next message after Poll::Pending")
            .expect("Test stream closed unexpectedly after Flush");

        let expected = messages.message;

        assert_eq!(
            expected, next_message,
            "Expected {expected:?} from the test stream, found {next_message:?}",
        );
    }
}
