use crate::{
    protocol::{backend, frontend},
    tcp,
    transaction::Transaction,
};
use futures_util::{stream::SplitStream, Sink, SinkExt, StreamExt};
use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
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
/// Proxied database connection for use with the Pool
pub struct Connection {
    /// set of prepared statements on this connection for routing of dependent messages
    prepared_statements: HashSet<String>,
    /// sink for clients to send frontend messages
    frontend_sink: UnboundedSender<frontend::Message>,
    /// backend messages to be proxied to subscriptions
    backend_stream: SplitStream<tcp::Connection<backend::Codec>>,
}

impl Connection {
    /// Subscribe to backend messages for an entire top-level transaction
    pub fn transaction(&mut self) -> Transaction {
        Transaction::new(&mut self.backend_stream)
    }

    /// Check if a Connection has a prepared statement
    pub fn has_prepared(&self, statement: &str) -> bool {
        self.prepared_statements.get(statement).is_some()
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

            tracing::warn!(
                remote_address = %remote_address,
                "Closing proxied connection due to disconnection"
            );
        });

        Self {
            prepared_statements: HashSet::new(),
            frontend_sink,
            backend_stream,
        }
    }
}

impl Sink<frontend::Message> for Connection {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: frontend::Message) -> Result<(), Self::Error> {
        self.frontend_sink.send(item).map_err(|_| Error::Sync)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
