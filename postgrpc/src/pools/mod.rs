use futures_util::TryStream;
use std::fmt;
use tokio_postgres::types::ToSql;
use tonic::{async_trait, Status};

#[cfg_attr(doc, doc(cfg(feature = "deadpool")))]
#[cfg(any(doc, feature = "deadpool"))]
pub mod deadpool;
mod protocol;
#[cfg_attr(doc, doc(cfg(feature = "transaction")))]
#[cfg(any(doc, feature = "transaction"))]
pub mod transaction;

/// Newtype wrapper around dynamically-typed, JSON-compatible [`pbjson_types::Value`]s.
///
/// This implements [`tokio_postgres::types::ToSql`] as a means of interfacing between the loose types of
/// JSON and the richer type system encoded in the Postgres protocol.
#[derive(Debug, Clone)]
pub struct Parameter(pbjson_types::Value);

/// gRPC-compatible connection behavior across database connection types. All inputs and outputs
/// are based on protobuf's JSON-compatible well-known-types [`pbjson_types::Struct`] and
/// [`pbjson_types::Value`].
///
/// #### Example:
///
/// ```rust
/// use postgrpc::pools::{Connection, Parameter};
/// use tokio_postgres::Row;
/// use tonic::Status;
///
/// // implementing a real StructStream (a fallible stream of Structs)
/// // is an exercise left to the reader
/// struct StructStream;
///
/// impl From<Vec<Row>> for StructStream {
///   // also an exercise for the reader
/// }
///
/// #[async_trait]
/// impl Connection for tokio_postgres::Client {
///   type Error = Status;
///   type RowStream = StructStream;
///
///   async fn query(
///     &self,
///     statement: &str,
///     parameters: &[Parameter],
///   ) -> Result<Self::RowStream, Self::Error> {
///     // Parameter implements ToSql, so can be used directly in query()
///     let rows: Vec<_> = self
///       .query(statement, parameters)
///       .await
///       .map_error(|error| Status::invalid_argument(error.to_string()))?;
///
///     // it's best to stream rows instead of collecting them into a Vec
///     // but this is only meant to be a quick example
///      Ok(StructStream::from(rows))
///   }
///
///  #[tracing::instrument(skip(self))]
///  async fn batch(&self, query: &str) -> Result<(), Self::Error> {
///    tracing::trace!("Executing batch query on Connection");
///    self.batch_execute(query).await.map_err(|error| Status::invalid_argument(error.to_string()))
///    }
/// }
/// ```

#[async_trait]
pub trait Connection<R>
where
    Self: Send + Sync,
    R: Send,
{
    /// A fallible stream of generic rows returned from the database
    type RowStream: TryStream<Ok = R, Error = Self::Error> + Send + Sync;

    /// Error type on the connection encompassing top-level errors (i.e. "bad connection") and
    /// errors within a RowStream
    type Error: std::error::Error + Into<Status> + Send + Sync;

    /// Run a query parameterized by the Connection's associated Parameter, returning a RowStream
    async fn query(
        &self,
        statement: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<Self::RowStream, Self::Error>;

    /// Run a set of SQL statements using the simple query protocol
    async fn batch(&self, query: &str) -> Result<(), Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations.
///
/// The key difference between a [`Pool`] and most other connection pools is the way new
/// connections are accessed: by building connection logic around a `Key` that can be derived from
/// a [`tonic::Request`], all connection isolation and preparation can be handled internally to the
/// pool. Furthermore, pools don't _have_ to be traditional pools, but can hand out shared access
/// to a single [`Connection`].
///
/// #### Example:
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
pub trait Pool<R>
where
    Self: Send + Sync,
    R: Send,
{
    /// The key by which connections are selected from the Pool, allowing for custom
    /// connection-fetching logic in Pool implementations
    type Key: fmt::Debug + Send + Sync;

    /// The underlying connection type returned from the Pool
    type Connection: Connection<R>;

    /// Errors related to fetching Connections from the Pool
    type Error: std::error::Error
        + From<<Self::Connection as Connection<R>>::Error>
        + Into<Status>
        + Send
        + Sync;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error>;
}
