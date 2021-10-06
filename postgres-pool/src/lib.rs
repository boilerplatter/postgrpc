//! Connection-pooling traits for databases (like Postgres) that handle custom
//! connection-fetching logic, query parameterization, and custom streaming output types.
#![deny(missing_docs, unreachable_pub)]

pub use async_trait::async_trait;
use futures_core::TryStream;
use std::fmt;

/// Connection behavior across database connection types. This trait is generic over
/// its query parameter types, allowing for custom input types or limited subsets of otherwise acceptable inputs.
/// Connections also define their own output types (i.e. `RowStream`), which can also be
/// customized as needed.
///
/// ## Example:
///
/// ```
/// use postgres_pool::Connection;
/// use tokio_postgres::{Client, RowStream, types::Type};
///
/// // a toy client wrapping tokio_postgres's built-in Client
/// // that uses static strings as parameters (i.e. requiring query-time type hints)
/// struct MyClient(tokio_postgres::Client);
///
/// #[postgres_pool::async_trait]
/// impl Connection for MyClient {
///     type Parameter: &'static str;
///     type RowStream: RowStream;
///     type Error: tokio_postgres::Error;
///
///     async fn query(
///         &self,
///         statement: &str,
///         parameters: &[Self::Parameter],
///     ) -> Result<Self::RowStream, Self::Error> {
///         self.0.query_raw(prepared, parameters).await
///     }
///
///     async fn batch(&self, query: &str) -> Result<(), Self::Error> {
///         self.0.batch_execute(statement).await
///     }
/// }
/// ```
#[async_trait]
pub trait Connection: Send + Sync {
    /// Accepted parameter type for this connection to use during queries
    type Parameter: fmt::Debug + Send + Sync;

    /// A fallible stream of data returned from query (usually rows from the database)
    type RowStream: TryStream<Error = Self::Error> + Send + Sync;

    /// Error type on the connection encompassing top-level errors (i.e. "bad connection") and
    /// errors within a RowStream
    type Error: std::error::Error;

    /// Run a query parameterized by the Connection's associated Parameter, returning a RowStream
    async fn query(
        &self,
        statement: &str,
        parameters: &[Self::Parameter],
    ) -> Result<Self::RowStream, Self::Error>;

    /// Run a set of SQL statements using the simple query protocol
    async fn batch(&self, query: &str) -> Result<(), Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations
///
/// ## Example:
///
/// ```
/// use postgres_pool::Pool
/// use std::collections::BTreeMap;
/// use tokio::sync::RwLock;
/// use uuid::Uuid;
///
/// // a toy pool wrapping a collection of tokio_postgres::Clients
/// // accessible by unique IDs that are provided by the caller
/// struct MyPool {
///     connections: RwLock<BTreeMap<Uuid, Arc<tokio_postgres::Client>>>,
///     config: tokio_postgres::config::Config
/// }
///
/// #[postgres_pool::async_trait]
/// impl Pool for MyPool {
///     type Key: Uuid;
///     type Connection: Arc<tokio_postgres::Client>;
///     type Error: anyhow::Error; // not required, but nice to use
///
///     async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error> {
///         // get a connection from the pool or store one for later
///         let connections = self.connections.read().await;
///
///         match connections.get(&key) {
///             Some(connection) => Ok(Arc::clone(connection)),
///             None => {
///                 // drop the previous lock on the connections
///                 drop(connections);
///
///                 // connect to the database using the configuration
///                 let (connection, raw_connection) = self.config.connect(tokio_postgres::NoTls)?;
///                 
///                 // spawn the raw connection off onto an executor
///                 tokio::spawn(async move {
///                     if let Err(error) = raw_connection.await {
///                         eprintln!("connection error: {}", error);
///                     }
///                 });
///
///                 // store a reference to the connection for later
///                 let connection = Arc::new(connection);
///                 self.connections.write().await.insert(key, Arc::clone(&connection));
///
///                 // return another reference to the connection for use
///                 Ok(connection)
///             }
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Pool: Send + Sync {
    /// The key by which connections are selected from the Pool, allowing for custom
    /// connection-fetching logic in Pool implementations
    type Key: fmt::Debug;

    /// The underlying connection type returned from the Pool
    type Connection: Connection;

    /// Errors related to fetching Connections from the Pool
    type Error: std::error::Error;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error>;
}
