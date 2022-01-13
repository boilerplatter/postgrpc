use crate::{
    protocol::{backend, frontend},
    tcp,
    transaction::Transaction,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{stream::SplitStream, Sink, SinkExt, StreamExt, TryStreamExt};
use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error syncing proxied connection")]
    Sync,
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
}

pin_project_lite::pin_project! {
    /// Proxied database connection for use with the Pool
    pub struct Connection {
        frontend_sink: UnboundedSender<frontend::Message>,
        startup_messages: Vec<backend::Message>,
        prepared_statements: HashSet<Bytes>,
        last_used: Instant,
        #[pin]
        backend_stream: SplitStream<tcp::Connection<backend::Codec>>,
    }
}

impl Connection {
    /// Fetch the startup messages associated with the connection
    pub async fn startup_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = backend::Message>, Error> {
        if self.startup_messages.is_empty() {
            self.startup_messages = Transaction::new(&mut self.backend_stream)
                .try_collect()
                .await?;
        }

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
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        // lazily update the startup message cache on first interaction
        if self.startup_messages.is_empty() {
            self.startup_messages = Transaction::new(&mut self.backend_stream)
                .try_collect()
                .await?;
        }

        Ok(Transaction::new(&mut self.backend_stream))
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
                        remote_address = %remote_address,
                        error = %Error::Sync,
                        "Closing proxied connection due to unrecoverable error"
                    );
                }
            }

            tracing::debug!(
                remote_address = %remote_address,
                "Closing proxied connection"
            );
        });

        Self {
            frontend_sink,
            backend_stream,
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
        self.project().backend_stream.poll_next(context)
    }
}
