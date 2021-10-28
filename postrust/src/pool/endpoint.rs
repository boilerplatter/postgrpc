#![allow(clippy::mutable_key_type)]
use crate::{
    connections::{self, Connection},
    protocol::{
        backend::{self, TransactionStatus},
        frontend,
    },
};
use futures_core::Stream;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::atomic::{AtomicUsize, Ordering},
};
use thiserror::Error;
use tokio::sync::{broadcast::error::SendError, mpsc::UnboundedSender, RwLock};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error reading messages from backend connection: {0}")]
    BroadcastRead(#[from] BroadcastStreamRecvError),
    #[error("Error sending message from backend connection: {0}")]
    BroadcastWrite(#[from] SendError<backend::Message>),
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error("Error syncing proxied connection")]
    Sync,
}

/// Postrust's only supported protocol version
const SUPPORTED_PROTOCOL_VERSION: i32 = 196608;

// TODO:
// set up a cleanup process for expired pools
// send auth requests (if configured) to gRPC or HTTP service
// if there are leaders and followers, test AST with postguard for read versus writes
//
// TODO low-priority:
// add transaction-level pooling
// add session-level pooling

/// Database connection endpoint configuration
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Endpoint {
    pub user: String,
    pub password: String,
    pub database: String,
    host: IpAddr,
    port: u16,
    protocol_version: i32,
}

impl Endpoint {
    pub fn new(user: String, password: String, database: String, host: IpAddr, port: u16) -> Self {
        Self {
            user,
            password,
            database,
            host,
            port,
            protocol_version: SUPPORTED_PROTOCOL_VERSION,
        }
    }

    pub fn address(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Endpoint")
            .field("user", &self.user)
            .field("password", &"******")
            .field("database", &self.database)
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

/// Database connection represented by a sink/stream pair
/// orderable by the number of active receivers
pub struct ProxiedConnection {
    frontend_sink: UnboundedSender<frontend::Message>,
    backend_broadcast: tokio::sync::broadcast::Sender<backend::Message>,
}

impl ProxiedConnection {
    /// Subscribe to the next ReadyForQuery-terminated chunk of messages from the backend
    pub fn subscribe(&self) -> impl Stream<Item = Result<backend::Message, Error>> {
        BroadcastStream::new(self.backend_broadcast.subscribe())
            .try_take_while(|message| {
                futures_util::future::ok(!matches!(
                    message,
                    backend::Message::ReadyForQuery {
                        transaction_status: TransactionStatus::Idle,
                    }
                ))
            })
            .map_err(Error::BroadcastRead)
    }

    /// Send a frontend message to the connection
    pub fn send(&self, message: frontend::Message) -> Result<(), Error> {
        self.frontend_sink.send(message).map_err(|_| Error::Sync)
    }

    /// Check to see if the connection is currently handling messages
    pub fn is_idle(&self) -> bool {
        self.backend_broadcast.receiver_count() == 0
    }
}

impl From<Connection<backend::Codec>> for ProxiedConnection {
    fn from(connection: Connection<backend::Codec>) -> Self {
        let (mut backend_sink, mut backend_stream) = connection.split();
        let (frontend_sink, mut frontend_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (backend_broadcast, _) = tokio::sync::broadcast::channel(128);
        let backend_broadcast_transmitter = backend_broadcast.clone();

        // send messages from the frontend to the backend through the bounded sender
        tokio::spawn(async move {
            while let Some(message) = frontend_receiver.recv().await {
                backend_sink.send(message).await.map_err(|_| Error::Sync)?;
            }

            Ok::<_, Error>(())
        });

        // broadcast messages from the backend to listeners
        tokio::spawn(async move {
            while let Some(message) = backend_stream.try_next().await? {
                backend_broadcast_transmitter.send(message)?;
            }

            Ok::<_, Error>(())
        });

        Self {
            frontend_sink,
            backend_broadcast,
        }
    }
}

impl PartialEq for ProxiedConnection {
    fn eq(&self, other: &Self) -> bool {
        self.backend_broadcast.receiver_count() == other.backend_broadcast.receiver_count()
    }
}

impl Eq for ProxiedConnection {}

impl PartialOrd for ProxiedConnection {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProxiedConnection {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let size = self.backend_broadcast.receiver_count();
        let other_size = other.backend_broadcast.receiver_count();

        // reverse-ordered by size
        size.cmp(&other_size)
    }
}

/// Set of load-balance-ready connections proxied over a channel
pub struct ProxiedConnections {
    /// Sink-like and Stream-like pairs for each connection
    pub connections: RwLock<Vec<ProxiedConnection>>,
    /// Endpoint configuration for spawning new connections
    endpoint: Endpoint,
}

impl ProxiedConnections {
    /// Create a new set of ProxiedConnections
    pub fn new<C>(endpoint: Endpoint, connections: C) -> Self
    where
        C: IntoIterator<Item = Connection<backend::Codec>>,
    {
        let connections = connections
            .into_iter()
            .map(ProxiedConnection::from)
            .collect();

        Self {
            connections: RwLock::new(connections),
            endpoint,
        }
    }

    /// Grow the ProxiedConnections pool using the existing endpoint configuration
    #[tracing::instrument]
    pub async fn add_connection(
        &self,
        init_message: frontend::Message,
    ) -> Result<impl Stream<Item = Result<backend::Message, Error>>, Error> {
        tracing::info!("Adding connection for endpoint");

        let address = self.endpoint.address();

        let proxied_connection = Connection::<backend::Codec>::connect(
            address,
            self.endpoint.user.to_string(),
            self.endpoint.password.to_string(),
            self.endpoint.database.to_string(),
        )
        .await
        .map(ProxiedConnection::from)?;

        tracing::debug!("Proxied Connection created for endpoint");

        // drain the entire proxied_connection stream
        proxied_connection
            .subscribe()
            .try_for_each(|_| async { Ok(()) })
            .await?;

        tracing::debug!("Proxied Connection drained of startup messages");

        // set up the backend message stream from the drained connection
        let backend_messages = proxied_connection.subscribe();

        // send the initial message to the new connection
        proxied_connection.send(init_message)?;

        {
            // store the connection again for later
            self.connections.write().await.push(proxied_connection);
        }

        Ok(backend_messages)
    }
}

impl fmt::Debug for ProxiedConnections {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxiedConnections")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Collection of endpoints that cycles endlessly
pub struct RoundRobinEndpoints {
    index: AtomicUsize,
    endpoints: Vec<ProxiedConnections>,
}

impl RoundRobinEndpoints {
    /// Create a new round-robin endpoint balancer over proxied connections
    pub fn new(endpoints: Vec<ProxiedConnections>) -> Self {
        Self {
            index: AtomicUsize::new(0),
            endpoints,
        }
    }

    /// Iter-like next for selecting the next endpoint in the Round Robin queue
    pub fn next(&self) -> Option<&ProxiedConnections> {
        let index = self.index.fetch_add(1, Ordering::Relaxed);

        self.endpoints.get(index % self.endpoints.len())
    }
}
