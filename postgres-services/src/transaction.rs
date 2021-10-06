use postgres_pool::{Connection, Pool};
use std::{hash::Hash, sync::Arc};
use uuid::Uuid;

/// Type alias representing a bubbled-up error from the transaction pool
pub type Error<P> =
    postgres_transaction_pool::Error<<<P as Pool>::Connection as Connection>::Error>;

/// Protocol-agnostic Transaction handlers for any connection pool
#[derive(Clone)]
pub struct Transaction<P>
where
    P: Pool,
    P::Key: Hash + Eq + Clone,
{
    pool: postgres_transaction_pool::Pool<P>,
}

impl<P> Transaction<P>
where
    P: Pool + 'static,
    P::Key: Hash + Eq + Send + Sync + Clone + 'static,
    P::Connection: 'static,
    <P::Connection as Connection>::Error: Send + Sync + 'static,
{
    /// Create a new Postgres transaction service from a reference-counted Pool
    pub fn new(pool: Arc<P>) -> Self {
        Self {
            pool: postgres_transaction_pool::Pool::new(pool),
        }
    }

    /// Begin a Postgres transaction, returning a unique ID for the transaction
    #[tracing::instrument(skip(self))]
    pub async fn begin(&self, key: P::Key) -> Result<Uuid, Error<P>> {
        tracing::info!("Beginning transaction");

        let transaction_id = self.pool.begin(key).await?;

        Ok(transaction_id)
    }

    /// Query an active Postgres transaction by ID and connection pool key
    #[tracing::instrument(skip(self))]
    pub async fn query(
        &self,
        id: Uuid,
        key: P::Key,
        statement: &str,
        parameters: &[<P::Connection as Connection>::Parameter],
    ) -> Result<<P::Connection as Connection>::RowStream, Error<P>> {
        tracing::info!("Querying transaction");

        let transaction_key = postgres_transaction_pool::Key::new(key, id);

        let rows = self
            .pool
            .get_connection(transaction_key)
            .await?
            .query(statement, &parameters)
            .await
            .map_err(postgres_transaction_pool::Error::Connection)?;

        Ok(rows)
    }

    /// Commit an active Postgres transaction by ID and connection pool key
    #[tracing::instrument(skip(self))]
    pub async fn commit(&self, id: Uuid, key: P::Key) -> Result<(), Error<P>> {
        tracing::info!("Committing transaction");

        self.pool.commit(id, key).await?;

        Ok(())
    }

    /// Roll back an active Postgres transaction by ID and connection pool key
    #[tracing::instrument(skip(self))]
    pub async fn rollback(&self, id: Uuid, key: P::Key) -> Result<(), Error<P>> {
        tracing::info!("Rolling back transaction");

        self.pool.rollback(id, key).await?;

        Ok(())
    }
}
