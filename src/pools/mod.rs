use super::protocol::Parameter;
use tokio_postgres::RowStream;
use tonic::Status;

pub mod default;
pub mod transaction;

/// Connection behavior across abstract database connection types
#[tonic::async_trait]
pub trait Connection {
    type Error: std::error::Error + Into<Status>;

    /// Run a query parameterized by a scalar subset of proto-JSON Values, returning a stream of rows
    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<RowStream, Self::Error>;

    /// Run a set of SQL statements using the simple query protocol
    async fn batch(&self, query: &str) -> Result<(), Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations
#[tonic::async_trait]
pub trait Pool<K = String> {
    type Connection: Connection;
    type Error: std::error::Error + Into<Status>;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: K) -> Result<Self::Connection, Self::Error>;
}
