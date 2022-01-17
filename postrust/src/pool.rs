use crate::{connection::Connection, endpoint::Endpoint, protocol::backend, tcp};
use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc::UnboundedSender, Mutex};

// FIXME: make this configurable
/// Length of time that a Connection can be idle before getting cleaned up
const IDLE_CONNECTION_DURATION: Duration = Duration::from_secs(5);

/// Asynchronous pool for proxied database connections
pub struct Pool {
    /// Shared endpoint configuration for all Connections in the Pool
    endpoint: Endpoint,

    /// All pooled connections
    connections: Arc<Mutex<Vec<Connection>>>,

    /// Queue of Connections to return to the Pool
    returning_connections: UnboundedSender<Connection>,
}

impl Pool {
    /// Create a new Pool for an endpoint from existing connections
    #[tracing::instrument]
    pub async fn new(endpoint: Endpoint) -> Result<Self, tcp::Error> {
        let connections = Arc::new(Mutex::new(vec![]));

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

        tracing::info!("Connection pool initialized");

        Ok(Self {
            endpoint,
            connections,
            returning_connections,
        })
    }

    /// Fetch an existing idle connection from the pool (LIFO) or initialize a new one
    #[tracing::instrument]
    pub async fn get(&self) -> Result<PooledConnection, tcp::Error> {
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
pub struct PooledConnection {
    connection: Option<Connection>,
    return_sender: UnboundedSender<Connection>,
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
