use futures_util::TryStream;
use prost_types::value::Kind;
use std::fmt;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use tonic::{async_trait, Request, Status};

#[cfg(feature = "deadpool")]
pub mod deadpool;
#[cfg(feature = "transaction")]
pub mod transaction;

// FIXME: update the documentation in this module

/// Newtype wrapper around gRPC values
#[derive(Debug, Clone)]
pub struct Parameter(prost_types::Value);

/// Binary encoding for Parameters
impl ToSql for Parameter {
    fn to_sql(
        &self,
        type_: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self.0.kind {
            // FIXME: handle lists and structs separately from NULL
            Some(Kind::ListValue(..) | Kind::StructValue(..) | Kind::NullValue(..)) | None => {
                Ok(IsNull::Yes)
            }
            Some(Kind::BoolValue(boolean)) => boolean.to_sql(type_, out),
            Some(Kind::StringValue(text)) => text.to_sql(type_, out),
            Some(Kind::NumberValue(number)) => match *type_ {
                Type::INT2 => (*number as i16).to_sql(type_, out),
                Type::INT4 => (*number as i32).to_sql(type_, out),
                Type::INT8 => (*number as i64).to_sql(type_, out),
                Type::FLOAT4 => (*number as f32).to_sql(type_, out),
                Type::FLOAT8 => (*number as f64).to_sql(type_, out),
                // ToSql should not be used for type-inferred parameters of format text
                _ => Err(format!("Cannot encode number as type {}", type_).into()),
            },
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl From<prost_types::Value> for Parameter {
    fn from(value: prost_types::Value) -> Self {
        Self(value)
    }
}

/// gRPC-compatible connection behavior across database connection types. All inputs and outputs
/// are based on prost well-known-types.
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
    /// A fallible stream of rows returned from the database as protobuf structs
    type RowStream: TryStream<Ok = prost_types::Struct, Error = Self::Error> + Send + Sync;

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

/// Helper trait to encapsulate logic for deriving values from gRPC requests
pub trait FromRequest
where
    Self: Sized,
{
    /// Errors associated with deriving a value from a gRPC response
    type Error: std::error::Error + Into<Status>;

    /// Derive a value from a gRPC request
    fn from_request<T>(request: &mut Request<T>) -> Result<Self, Self::Error>;
}

/// Dummy error for default impl of derivation of unit structs from requests
#[derive(Debug)]
pub struct UnitConversion;

impl fmt::Display for UnitConversion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for UnitConversion {}

impl From<UnitConversion> for Status {
    fn from(_: UnitConversion) -> Self {
        Self::internal("Infallible unit conversion somehow failed")
    }
}

impl FromRequest for () {
    type Error = UnitConversion;

    fn from_request<T>(_: &mut Request<T>) -> Result<Self, Self::Error> {
        Ok(())
    }
}
