use crate::{
    connection::{self, Connection},
    endpoint::Endpoint,
    protocol::{
        backend,
        frontend::{self, QueryBody},
    },
    tcp,
};
use futures_util::{SinkExt, TryStreamExt};
use std::{
    fmt,
    num::ParseIntError,
    ops::{Deref, DerefMut},
    str::Utf8Error,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, AcquireError, Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Connection(#[from] connection::Error),
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Error limiting connections to MAX_CONNECTIONS: {0}")]
    Limit(#[from] AcquireError),
    #[error("Error fetching the MAX_CONNECTIONS value for this endpoint")]
    MaxConnections,
    #[error("Error parsing the MAX_CONNECTIONS value for this endpoint: {0}")]
    MaxConnectionsParseInt(ParseIntError),
    #[error("Error parsing the MAX_CONNECTIONS value for this endpoint: {0}")]
    MaxConnectionsParseUtf8(Utf8Error),
}

// FIXME: make these configurable
/// Length of time that a Connection can be idle before getting cleaned up
const IDLE_CONNECTION_DURATION: Duration = Duration::from_secs(5);

/// Maxiumum Pool size default where the connection size cannot be otherwise queried
// this should match the default MAX_CONNECTIONS settings for Postgres
const MAX_CONNECTIONS_DEFAULT: usize = 100;

/// Asynchronous pool for proxied database connections
pub struct Pool {
    /// Shared endpoint configuration for all Connections in the Pool
    endpoint: Endpoint,

    /// All pooled connections
    connections: Arc<Mutex<Vec<Connection>>>,

    /// Queue of Connections to return to the Pool
    returning_connections: UnboundedSender<Connection>,

    /// Permit system for making sure that the pool does not exceed its connection capacity
    permits: Arc<Semaphore>,
}

impl Pool {
    /// Create a new Pool for an endpoint from existing connections
    #[tracing::instrument]
    pub async fn new(endpoint: Endpoint) -> Result<Self, Error> {
        // initialize a connection for startup queries
        let mut connection = tcp::Connection::<backend::Codec>::connect(
            endpoint.address(),
            endpoint.user.to_string(),
            endpoint.password.to_string(),
            endpoint.database.to_string(),
        )
        .await
        .map(Connection::from)?;

        // get the MAX_CONNECTIONS for this endpoint, if possible
        // FIXME: handle this more dynamically in response to connections from other sources,
        // e.g. gracefully handle "too many clients" errors from the backend when establishing new
        // connections (which would mean no more Semaphore required)
        let mut max_connections: Option<usize> = None;

        connection
            .send(frontend::Message::Query(QueryBody::new(
                "show max_connections;",
            )))
            .await?;

        let mut transaction = connection.transaction().await?;

        while let Some(message) = transaction.try_next().await? {
            if let backend::Message::DataRow { mut columns } = message {
                max_connections = columns
                    .pop()
                    .ok_or(Error::MaxConnections)?
                    .map(|bytes| {
                        std::str::from_utf8(&bytes)
                            .map_err(Error::MaxConnectionsParseUtf8)?
                            .parse()
                            .map_err(Error::MaxConnectionsParseInt)
                    })
                    .transpose()?;
            }
        }

        // initialize the connection pool
        let capacity = max_connections.unwrap_or(MAX_CONNECTIONS_DEFAULT);

        let mut connections = Vec::with_capacity(capacity);

        connections.push(connection);

        let connections = Arc::new(Mutex::new(connections));

        // periodically clean up idle connections
        tokio::spawn({
            let connections = connections.clone();

            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let now = Instant::now();

                    let mut connections = connections.lock().await;

                    connections.retain(|connection: &Connection| {
                        now.duration_since(connection.last_used()) < IDLE_CONNECTION_DURATION
                    });
                }
            }
        });

        // periodically return connections to the pool
        let (returning_connections, mut returning_connections_receiver) =
            tokio::sync::mpsc::unbounded_channel();

        tokio::spawn({
            let connections = connections.clone();

            async move {
                while let Some(connection) = returning_connections_receiver.recv().await {
                    let mut connections = connections.lock().await;

                    connections.push(connection);

                    tracing::debug!(
                        connections = connections.len(),
                        "Connection returned to the Pool"
                    );
                }
            }
        });

        tracing::info!(capacity, "Connection pool initialized");

        Ok(Self {
            endpoint,
            connections,
            returning_connections,
            permits: Arc::new(Semaphore::new(capacity)),
        })
    }

    /// Fetch an existing idle connection from the pool (LIFO) or initialize a new one
    #[tracing::instrument]
    pub async fn get(&self) -> Result<PooledConnection, Error> {
        let permit = self.permits.clone().acquire_owned().await?;
        let mut connections = self.connections.lock().await;

        tracing::debug!(
            connections = connections.len(),
            "Fetching Connection from the Pool"
        );

        let connection = connections.pop();

        drop(connections);

        let connection = match connection {
            Some(connection) => connection,
            None => {
                tracing::info!("Adding Connection for Endpoint");

                // create a database connection from a TCP connection
                tcp::Connection::<backend::Codec>::connect(
                    self.endpoint.address(),
                    self.endpoint.user.to_string(),
                    self.endpoint.password.to_string(),
                    self.endpoint.database.to_string(),
                )
                .await
                .map(Connection::from)?
            }
        };

        Ok(PooledConnection {
            connection: Some(connection),
            return_sender: self.returning_connections.clone(),
            _permit: permit,
        })
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("Pool")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Wrapper around a pooled Connection for lifecycle management
// FIXME: avoid references here
pub struct PooledConnection {
    connection: Option<Connection>,
    return_sender: UnboundedSender<Connection>,
    _permit: OwnedSemaphorePermit,
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(mut connection) = self.connection.take() {
            connection.update_last_used();

            if let Err(error) = self.return_sender.send(connection) {
                tracing::warn!(%error, "Error returning Connection to Pool");
            }
        }
    }
}

impl fmt::Debug for PooledConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("PooledConnection")
            .field("connection", &self.connection)
            .finish()
    }
}
