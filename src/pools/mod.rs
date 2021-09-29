use super::protocol::Parameter;
use std::task::{Context, Poll};
use tokio_postgres::RowStream;
use tonic::Status;

pub mod default;
pub mod transaction;

/// Connection behavior across abstract database connection types
#[tonic::async_trait]
pub trait Connection {
    type Error: Into<Status>;

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
    type Error: Into<Status>;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: K) -> Result<Self::Connection, Self::Error>;
}

/// Message behavior for parsing payloads and origin channels
pub trait Message: Send + Sync {
    /// Get the channel associated with the message
    fn get_channel(&self) -> &str;

    /// Get the payload of a message
    fn get_payload(&self) -> prost_types::Value;
}

/// Raw connection behavior for spawning custom behavior on async executors
pub trait RawConnection: Send {
    type Message: Message;
    type Error: Into<Status>;

    /// Poll a raw connection for asynchronous messages (e.g. NOTIFY notifications)
    fn poll_messages(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Message, Self::Error>>>;
}

/// Pool-bypassing bare connection behavior for maximum flexibility
#[tonic::async_trait]
pub trait RawConnect<K>: Pool<K> {
    type Client: Connection;
    type RawConnection: RawConnection;

    /// Get a connection + raw_connection pair for an underlying pool
    async fn connect(&self, key: K) -> Result<(Self::Client, Self::RawConnection), Self::Error>;
}
