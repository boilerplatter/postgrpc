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
use futures_util::{stream::SplitStream, Sink, SinkExt, StreamExt, TryStreamExt};
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
    #[error("Connection closed unexpectedly during startup")]
    Startup,
}

pin_project_lite::pin_project! {
    /// Proxied database connection for use with the Pool
    pub struct Connection {
        has_flushed_startup_messages: bool,
        startup_messages: Vec<backend::Message>,
        frontend_sink: UnboundedSender<frontend::Message>,
        prepared_statements: HashSet<Bytes>,
        last_used: Instant,
        #[pin]
        backend_stream: SplitStream<tcp::Connection<backend::Codec>>,
    }
}

impl Connection {
    /// Initialize the startup message cache by running the Connection stream to a ready state
    #[tracing::instrument]
    async fn initialize_startup_messages(&mut self) -> Result<(), Error> {
        tracing::trace!("Initializing startup messages");

        while !self.has_flushed_startup_messages {
            self.try_next().await?.ok_or(Error::Startup)?;
        }

        Ok(())
    }

    /// Fetch the startup messages associated with the connection
    #[tracing::instrument]
    pub async fn startup_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = backend::Message>, Error> {
        self.initialize_startup_messages().await?;

        tracing::info!(messages = ?&self.startup_messages, "Copying startup messages");

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
    #[tracing::instrument]
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.initialize_startup_messages().await?;

        Ok(Transaction::new(&mut self.backend_stream))
    }

    /// Flush the messages in the connection until the next Pending state
    #[tracing::instrument]
    pub async fn flush(&mut self) -> Result<Flush<'_>, Error> {
        self.initialize_startup_messages().await?;

        Ok(Flush::new(&mut self.backend_stream))
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

impl Stream for Connection {
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
