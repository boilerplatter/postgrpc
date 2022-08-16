use futures_util::TryStream;
use std::fmt;
use tonic::{async_trait, Status};

#[cfg_attr(doc, doc(cfg(feature = "deadpool")))]
#[cfg(any(doc, feature = "deadpool"))]
pub mod deadpool;
mod protocol;
#[cfg_attr(doc, doc(cfg(feature = "shared-connection-pool")))]
#[cfg(any(doc, feature = "shared-connection-pool"))]
pub mod shared;
#[cfg_attr(doc, doc(cfg(feature = "transaction")))]
#[cfg(any(doc, feature = "transaction"))]
pub mod transaction;

/// Newtype wrapper around dynamically-typed, JSON-compatible protobuf values
#[derive(Debug, Clone)]
pub struct Parameter(pbjson_types::Value);

/// gRPC-compatible connection behavior across database connection types. All inputs and outputs
/// are based on protobuf's JSON-compatible well-known-types `Struct` and `Value`
#[async_trait]
pub trait Connection: Send + Sync {
    /// A fallible stream of rows returned from the database as protobuf structs
    type RowStream: TryStream<Ok = pbjson_types::Struct, Error = Self::Error> + Send + Sync;

    /// Error type on the connection encompassing top-level errors (i.e. "bad connection") and
    /// errors within a RowStream
    type Error: std::error::Error + Into<Status> + Send + Sync;

    /// Run a query parameterized by the Connection's associated Parameter, returning a RowStream
    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<Self::RowStream, Self::Error>;

    /// Run a set of SQL statements using the simple query protocol
    async fn batch(&self, query: &str) -> Result<(), Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations
///
/// ## Example:
///
/// ```
/// use postgrpc::pools::{Pool, Connection};
/// use std::collections::BTreeMap;
/// use tokio::sync::RwLock;
/// use uuid::Uuid;
/// use tonic::Status;
///
/// // a simple connection wrapper
/// // (implementing postgrpc::Connection is an exercise for the reader)
/// #[derive(Clone)]
/// struct MyConnection(Arc<tokio_postgres::Client>);
///
/// // a toy pool wrapping a collection of tokio_postgres::Clients
/// // accessible by unique IDs that are provided by the caller
/// struct MyPool {
///     connections: RwLock<BTreeMap<Uuid, MyConnection>>,
///     config: tokio_postgres::config::Config
/// }
///
/// #[postgrpc::async_trait]
/// impl Pool for MyPool {
///     type Key: Uuid;
///     type Connection: MyConnection;
///     type Error: Status;
///
///     async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error> {
///         // get a connection from the pool or store one for later
///         let connections = self.connections.read().await;
///
///         match connections.get(&key) {
///             Some(connection) => Ok(Arc::clone(connection.0)),
///             None => {
///                 // drop the previous lock on the connections
///                 drop(connections);
///
///                 // connect to the database using the configuration
///                 let (client, connection) = self
///                     .config
///                     .connect(tokio_postgres::NoTls)
///                     .map_error(|error| Status::internal(error.to_string()))?;
///
///                 // spawn the raw connection off onto an executor
///                 tokio::spawn(async move {
///                     if let Err(error) = connection.await {
///                         eprintln!("connection error: {}", error);
///                     }
///                 });
///
///                 // store a reference to the connection for later
///                 let connection = MyConnection(Arc::new(client));
///                 self.connections.write().await.insert(key, connection);
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
    type Key: fmt::Debug + Send + Sync;

    /// The underlying connection type returned from the Pool
    type Connection: Connection;

    /// Errors related to fetching Connections from the Pool
    type Error: std::error::Error
        + From<<Self::Connection as Connection>::Error>
        + Into<Status>
        + Send
        + Sync;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error>;
}
