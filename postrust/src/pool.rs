use crate::{connection::Connection, endpoint::Endpoint, protocol::backend, tcp};
use parking_lot::Mutex;
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

/// Asynchronous pool for proxied database connections
// FIXME: implement idle connection cleanup from the connection pool
pub struct Pool {
    /// Shared endpoint for all Connections in the Pool
    endpoint: Endpoint,

    /// All pooled connections
    connections: Mutex<Vec<Connection>>,
}

impl Pool {
    /// Create a new Pool for an endpoint from existing connections
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Mutex::new(Vec::new()),
        }
    }

    /// Fetch an existing idle connection from the pool (LIFO) or initialize a new one
    #[tracing::instrument]
    pub async fn get(&self) -> Result<PooledConnection<'_>, tcp::Error> {
        let mut connections = self.connections.lock();

        tracing::debug!(
            connections = connections.len(),
            "Fetching Connection from the Pool"
        );

        let connection = match connections.pop() {
            Some(connection) => connection,
            None => {
                drop(connections);

                let address = self.endpoint.address();

                tracing::info!("Adding Connection for Endpoint");

                // create a database connection from a TCP connection
                tcp::Connection::<backend::Codec>::connect(
                    address,
                    self.endpoint.user.to_string(),
                    self.endpoint.password.to_string(),
                    self.endpoint.database.to_string(),
                )
                .await
                .map(Connection::from)?
            }
        };

        Ok(PooledConnection {
            pool: self,
            connection: Some(connection),
        })
    }

    /// Fetch the Connection associated with a prepared statement, if one exists
    // FIXME: benchmark this. We should instead use a data structure that allows us
    // to search for connections using any prepared statement (but only if it's fast for
    // reasonable numbers of connections)
    pub fn get_by_statement(&self, statement: &str) -> Option<PooledConnection<'_>> {
        let mut connections = self.connections.lock();

        if let Some(index) = connections
            .iter()
            .rposition(|connection| connection.has_prepared(statement))
        {
            let connection = connections.swap_remove(index);

            return Some(PooledConnection {
                pool: self,
                connection: Some(connection),
            });
        }

        None
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
pub struct PooledConnection<'a> {
    connection: Option<Connection>,
    pool: &'a Pool,
}

impl Deref for PooledConnection<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            let mut connections = self.pool.connections.lock();

            connections.push(connection);

            tracing::debug!(
                connections = connections.len(),
                "Connection returned to the Pool"
            );
        }
    }
}
