use crate::pools::{self, transaction, Connection, Pool};
use std::{hash::Hash, sync::Arc};
use uuid::Uuid;

/// Type alias representing a bubbled-up error from the transaction pool
pub type Error<P> = transaction::Error<<<P as Pool>::Connection as Connection>::Error>;

/// Protocol-agnostic Transaction handlers for any connection pool
#[derive(Clone)]
pub struct Transaction<P = pools::default::Pool>
where
    P: Pool,
    P::Key: Hash + Eq + Clone,
{
    pool: transaction::Pool<P>,
}

impl<P> Transaction<P>
where
    P: Pool + 'static,
    P::Key: Hash + Eq + Send + Sync + Clone + 'static,
    P::Connection: 'static,
    <P::Connection as Connection>::Error: Send + Sync + 'static,
{
    pub fn new(pool: Arc<P>) -> Self {
        Self {
            pool: transaction::Pool::new(pool),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn begin(&self, key: P::Key) -> Result<Uuid, Error<P>> {
        tracing::info!("Beginning transaction");

        let transaction_id = self.pool.begin(key).await?;

        Ok(transaction_id)
    }

    #[tracing::instrument(skip(self))]
    pub async fn query(
        &self,
        id: Uuid,
        key: P::Key,
        statement: &str,
        parameters: &[<P::Connection as Connection>::Parameter],
    ) -> Result<<P::Connection as Connection>::RowStream, Error<P>> {
        tracing::info!("Querying transaction");

        let transaction_key = transaction::TransactionKey::new(key, id);

        let rows = self
            .pool
            .get_connection(transaction_key)
            .await?
            .query(statement, &parameters)
            .await
            .map_err(transaction::Error::Connection)?;

        Ok(rows)
    }

    #[tracing::instrument(skip(self))]
    pub async fn commit(&self, id: Uuid, key: P::Key) -> Result<(), Error<P>> {
        tracing::info!("Committing transaction");

        self.pool.commit(id, key).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn rollback(&self, id: Uuid, key: P::Key) -> Result<(), Error<P>> {
        tracing::info!("Rolling back transaction");

        self.pool.rollback(id, key).await?;

        Ok(())
    }
}
