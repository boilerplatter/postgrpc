use crate::{
    flush::Flush,
    protocol::{
        backend::{self, TransactionStatus},
        frontend,
    },
    tcp,
    transaction::Transaction,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{
    stream::{poll_fn, SplitStream},
    Sink, SinkExt, StreamExt,
};
use std::{
    collections::HashSet,
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Error syncing proxied connection")]
    Sync,
}

pin_project_lite::pin_project! {
    /// Proxied database connection for use with the Pool
    pub struct Connection<S = SplitStream<tcp::Connection<backend::Codec>>>
    where
        S: Stream<Item = Result<backend::Message, tcp::Error>>,
        S: Unpin
    {
        has_flushed_startup_messages: bool,
        startup_messages: Vec<backend::Message>,
        frontend_sink: UnboundedSender<frontend::Message>,
        prepared_statements: HashSet<Bytes>,
        last_used: Instant,
        #[pin]
        backend_stream: S,
    }
}

impl<S> Connection<S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    /// Initialize the startup message cache by running the Connection stream to a ready state
    #[tracing::instrument(skip(self))]
    async fn initialize_startup_messages(&mut self) -> Result<(), Error> {
        tracing::trace!("Initializing startup messages");

        // start a watch stream that waits until this Connection has finished initializing
        let watcher = poll_fn(|context| {
            if self.has_flushed_startup_messages {
                return Poll::Ready(None);
            }

            if self.poll_next_unpin(context).is_ready() {
                return Poll::Pending;
            }

            Poll::Ready(Some(()))
        });

        watcher.for_each(futures_util::future::ready).await;

        Ok(())
    }

    /// Fetch the startup messages associated with the connection
    #[tracing::instrument(skip(self))]
    pub async fn startup_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = backend::Message>, Error> {
        self.initialize_startup_messages().await?;

        let messages = self.startup_messages.clone().into_iter();

        Ok(messages)
    }

    /// Fetch the Instant when the Connection was last used
    pub fn last_used(&self) -> Instant {
        self.last_used
    }

    /// Update the tracked last use of the Connection
    pub fn update_last_used(&mut self) {
        self.last_used = Instant::now()
    }

    /// Subscribe to backend messages for an entire top-level transaction
    #[tracing::instrument(skip(self))]
    pub async fn transaction(&mut self) -> Result<Transaction<'_, S>, Error> {
        self.initialize_startup_messages().await?;

        Ok(Transaction::new(&mut self.backend_stream))
    }

    /// Flush the messages in the connection until the next Pending state
    #[tracing::instrument(skip(self))]
    pub async fn flush(&mut self) -> Result<Flush<'_, S>, Error> {
        self.initialize_startup_messages().await?;

        Ok(Flush::new(&mut self.backend_stream))
    }

    #[cfg(test)]
    /// Construct a new Connection for testing
    fn new(backend_stream: S) -> Self {
        let (frontend_sink, _) = tokio::sync::mpsc::unbounded_channel();

        Self {
            frontend_sink,
            backend_stream,
            has_flushed_startup_messages: false,
            startup_messages: Vec::new(),
            prepared_statements: HashSet::new(),
            last_used: Instant::now(),
        }
    }
}

impl From<tcp::Connection<backend::Codec>> for Connection {
    fn from(connection: tcp::Connection<backend::Codec>) -> Self {
        let remote_address = connection.peer();
        let (mut backend_sink, backend_stream) = connection.split();
        let (frontend_sink, mut frontend_receiver) = tokio::sync::mpsc::unbounded_channel();

        // proxy messages from the frontend to the backend through the TCP connection
        tokio::spawn(async move {
            while let Some(message) = frontend_receiver.recv().await {
                if backend_sink.send(message).await.is_err() {
                    return tracing::warn!(
                        %remote_address,
                        error = %Error::Sync,
                        "Closing proxied connection due to unrecoverable error"
                    );
                }
            }

            tracing::debug!(%remote_address, "Closing proxied connection");
        });

        Self {
            frontend_sink,
            backend_stream,
            has_flushed_startup_messages: false,
            startup_messages: Vec::new(),
            prepared_statements: HashSet::new(),
            last_used: Instant::now(),
        }
    }
}

impl Sink<frontend::Message> for Connection {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: frontend::Message) -> Result<(), Self::Error> {
        // FIXME: handle Query with "prepare" commands, too
        // should this be a part of the Sink implementation, or something that can be tested from
        // the caller's perspective?
        if let frontend::Message::Parse(ref body) = &item {
            let is_first_insert = self.prepared_statements.insert(body.name());

            // skip forwarding Parse messages after the first time
            if !is_first_insert {
                return Ok(());
            }
        }

        self.frontend_sink.send(item).map_err(|_| Error::Sync)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<S> Stream for Connection<S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
{
    type Item = Result<backend::Message, tcp::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        let next = projected.backend_stream.poll_next(context);

        // forward backend messages if the startup messages have been flushed
        if *projected.has_flushed_startup_messages {
            return next;
        }

        // cache startup-specific messages for later
        if let Poll::Ready(Some(Ok(message))) = next {
            if matches!(
                &message,
                backend::Message::ReadyForQuery {
                    transaction_status: TransactionStatus::Idle
                }
            ) {
                *projected.has_flushed_startup_messages = true;
            }

            projected.startup_messages.push(message);

            context.waker().wake_by_ref();

            return Poll::Pending;
        }

        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.backend_stream.size_hint();
        (0, upper)
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("startup_messages", &self.startup_messages.len())
            .field("prepared_statements", &self.prepared_statements.len())
            .field("last_used", &self.last_used)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::Connection;
    use crate::protocol::backend::{self, TransactionStatus};
    use bytes::Bytes;
    use futures_util::{stream, StreamExt, TryStreamExt};

    static STARTUP_MESSAGES: [backend::Message; 3] = [
        backend::Message::Forward(Bytes::from_static(b"foo")),
        backend::Message::Forward(Bytes::from_static(b"bar")),
        backend::Message::ReadyForQuery {
            transaction_status: TransactionStatus::Idle,
        },
    ];

    static FIRST_MESSAGE: backend::Message =
        backend::Message::Forward(Bytes::from_static(b"first message"));

    #[tokio::test]
    async fn caches_startup_messages() {
        let messages = stream::iter(STARTUP_MESSAGES.clone())
            .chain(stream::iter([FIRST_MESSAGE.clone()]))
            .map(Result::Ok);

        let mut connection = Connection::new(messages);
        let first_message = connection
            .try_next()
            .await
            .expect("Error in Connection stream")
            .expect("Connection closed unexpectedly");

        assert_eq!(
            first_message, FIRST_MESSAGE,
            "Connection failed to transparently cache startup messages before initial message"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn initializes_with_only_startup_messages() {
        let messages = stream::iter(STARTUP_MESSAGES.clone()).map(Result::Ok);

        Connection::new(messages)
            .startup_messages()
            .await
            .expect("Error initializing startup messages")
            .for_each(drop)
    }

    #[tokio::test]
    async fn returns_startup_messages() {
        let messages = stream::iter(STARTUP_MESSAGES.clone())
            .chain(stream::iter([FIRST_MESSAGE.clone()]))
            .map(Result::Ok);

        let mut connection = Connection::new(messages);
        let startup_messages: Vec<_> = connection
            .startup_messages()
            .await
            .expect("Error retrieving startup messages")
            .collect();

        assert_eq!(
            startup_messages,
            STARTUP_MESSAGES.to_vec(),
            "Connection failed to return correct startup messages"
        );
    }

    #[tokio::test]
    async fn returns_transaction_stream() {
        let messages = stream::iter(STARTUP_MESSAGES.clone())
            .chain(stream::iter([FIRST_MESSAGE.clone()]))
            .map(Result::Ok);

        Connection::new(messages)
            .transaction()
            .await
            .expect("Error retrieving Transaction stream");
    }

    #[tokio::test]
    async fn returns_flush_stream() {
        let messages = stream::iter(STARTUP_MESSAGES.clone())
            .chain(stream::iter([FIRST_MESSAGE.clone()]))
            .map(Result::Ok);

        Connection::new(messages)
            .flush()
            .await
            .expect("Error retrieving Flush stream");
    }
}
