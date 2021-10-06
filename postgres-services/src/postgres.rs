use postgres_pool::{Connection, Pool};
use std::sync::Arc;

/// Protocol-agnostic Postgres handlers for any connection pool
#[derive(Clone)]
pub struct Postgres<P> {
    pool: Arc<P>,
}

impl<P> Postgres<P>
where
    P: Pool,
    P::Error: From<<P::Connection as Connection>::Error>,
{
    pub fn new(pool: Arc<P>) -> Self {
        Self { pool }
    }

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
            .query(statement, &parameters)
            .await?;

        Ok(rows)
    }
}
