use futures::TryStream;
use std::fmt;

pub mod default;
pub mod transaction;

/// Connection behavior across abstract database connection types
#[async_trait::async_trait]
pub trait Connection: Send + Sync {
    type Error: std::error::Error;
    type Parameter: fmt::Debug + Send + Sync;
    type RowStream: TryStream<Error = Self::Error> + Send + Sync;

    /// Run a query parameterized by a scalar subset of proto-JSON Values, returning a stream of rows
    async fn query(
        &self,
        statement: &str,
        parameters: &[Self::Parameter],
    ) -> Result<Self::RowStream, Self::Error>;

    /// Run a set of SQL statements using the simple query protocol
    async fn batch(&self, query: &str) -> Result<(), Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations
#[async_trait::async_trait]
pub trait Pool: Send + Sync {
    type Key: fmt::Debug;
    type Connection: Connection;
    type Error: std::error::Error;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error>;
}
