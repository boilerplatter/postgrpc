use postgres_pool::{Connection, Pool};
use std::sync::Arc;

/// Protocol-agnostic Postgres handlers for any connection pool
// TODO: regular old comment
// NOTE: this is a note
#[derive(Clone)]
pub struct Postgres<P> {
    pool: Arc<P>,
}

impl<P> Postgres<P>
where
    P: Pool,
    P::Error: From<<P::Connection as Connection>::Error>,
{
    /// Create a new Postgres service from a reference-counted Pool
    pub fn new(pool: Arc<P>) -> Self {
        Self { pool }
    }

    /// Query a Postgres database
    #[tracing::instrument(skip(self))]
    pub async fn query(
        &self,
        key: P::Key,
        statement: &str,
        parameters: &[<P::Connection as Connection>::Parameter],
    ) -> Result<<P::Connection as Connection>::RowStream, P::Error> {
        tracing::info!("Querying postgres");

        let rows = self
            .pool
            .get_connection(key)
            .await?
            .query(statement, parameters)
            .await?;

        Ok(rows)
    }
}
