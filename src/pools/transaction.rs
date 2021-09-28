use crate::{pools::Connection, protocol::Parameter};
use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_postgres::RowStream;
use tonic::Status;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum Error<C>
where
    C: Into<Status> + std::error::Error + 'static,
{
    #[error(transparent)]
    Connection(C),
    #[error("Error retrieving connection from transaction pool")]
    ConnectionFailure,
    #[error("SQL Error: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("Requested transaction has not been initialized or was cleaned up due to inactivity")]
    Uninitialized,
}

impl<C> From<Error<C>> for Status
where
    C: Into<Status> + std::error::Error + 'static,
{
    fn from(error: Error<C>) -> Self {
        let message = format!("{}", &error);

        match error {
            Error::Connection(error) => error.into(),
            Error::ConnectionFailure => Self::resource_exhausted(message),
            Error::Query(..) => Self::invalid_argument(message),
            Error::Uninitialized => Self::not_found(message),
        }
    }
}

/// Polling interval in seconds for cleanup operations
const VACUUM_POLLING_INTERVAL_SECONDS: u64 = 1;

/// Threshold in seconds for marking transactions as inactive
const INACTIVE_THRESHOLD_SECONDS: u64 = 30;

/// Time limit in seconds for any transaction, regardless of usage
const TRANSACTION_LIFETIME_LIMIT_SECONDS: u64 = 30 * 60;

/// Cached transaction data for an individual active transaction
pub struct Transaction<C>
where
    C: Connection,
{
    connection: Arc<C>,
    created_at: Instant,
    last_used_at: Arc<RwLock<Instant>>,
}

impl<C> Transaction<C>
where
    C: Connection,
{
    fn new(connection: Arc<C>) -> Self {
        let now = Instant::now();

        Self {
            connection,
            created_at: now,
            last_used_at: Arc::new(RwLock::new(now)),
        }
    }
}

impl<C> Clone for Transaction<C>
where
    C: Connection,
{
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            created_at: self.created_at,
            last_used_at: Arc::clone(&self.last_used_at),
        }
    }
}

#[tonic::async_trait]
impl<C> super::Connection for Transaction<C>
where
    C: Connection + Send + Sync + 'static,
{
    type Error = C::Error;

    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        self.connection.batch(query).await?;
        let mut last_used_at = self.last_used_at.write().await;
        *last_used_at = Instant::now();
        Ok(())
    }

    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<RowStream, Self::Error> {
        let rows = self.connection.query(statement, parameters).await?;
        let mut last_used_at = self.last_used_at.write().await;
        *last_used_at = Instant::now();
        Ok(rows)
    }
}

/// Key for interacting with active transactions in the cache,
/// checking access against the original connection pool key
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct TransactionKey<K>
where
    K: Hash + Eq,
{
    key: K,
    transaction_id: Uuid,
}

impl<K> TransactionKey<K>
where
    K: Hash + Eq,
{
    pub fn new(key: K, transaction_id: Uuid) -> Self {
        Self {
            key,
            transaction_id,
        }
    }
}

/// Type alias for the internal map of shared transactions
pub type TransactionMap<K, C> = HashMap<TransactionKey<K>, Transaction<C>>;

/// Pool of active transactions that wraps a lower-level Pool implementation
pub struct Pool<P, K>
where
    K: Hash + Eq + Clone,
    P: super::Pool<K>,
{
    pool: Arc<P>,
    transactions: Arc<RwLock<TransactionMap<K, P::Connection>>>,
}

impl<P, K> Clone for Pool<P, K>
where
    K: Hash + Eq + Clone,
    P: super::Pool<K>,
{
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            transactions: Arc::clone(&self.transactions),
        }
    }
}

impl<P, K> Pool<P, K>
where
    K: Hash + Eq + Send + Sync + fmt::Debug + Clone + 'static,
    P: super::Pool<K> + Send + Sync + 'static,
    P::Connection: Send + Sync + 'static,
    <P::Connection as Connection>::Error: std::error::Error + Send + Sync + 'static,
{
    /// Initialize a new shared transaction pool
    pub fn new(pool: Arc<P>) -> Self {
        let transactions = Arc::new(RwLock::new(HashMap::new()));

        let cache = Self {
            pool,
            transactions: Arc::clone(&transactions),
        };

        let shared_cache = cache.clone();
        let polling_interval = Duration::from_secs(VACUUM_POLLING_INTERVAL_SECONDS);
        let inactive_limit = Duration::from_secs(INACTIVE_THRESHOLD_SECONDS);
        let created_at_limit = Duration::from_secs(TRANSACTION_LIFETIME_LIMIT_SECONDS);

        // vacuum old and inactive transactions
        tokio::spawn(async move {
            loop {
                // set up polling interval
                tokio::time::sleep(polling_interval).await;

                let now = Instant::now();

                let mut rollback_queue = vec![];

                // find stale transactions in the cache
                for (transaction_key, transaction) in transactions.read().await.iter() {
                    let last_used_at = transaction.last_used_at.read().await;
                    let is_inactive = (now - *last_used_at) > inactive_limit;
                    let is_too_old = (now - transaction.created_at) > created_at_limit;

                    // queue stale transactions for cleanup
                    if is_inactive || is_too_old {
                        rollback_queue.push(transaction_key.clone());
                    }
                }

                // clean up stale transactions
                for transaction_key in rollback_queue.into_iter() {
                    if let Err(error) = shared_cache
                        .rollback(transaction_key.transaction_id, transaction_key.key)
                        .await
                    {
                        tracing::error!(error = ?&error, "Error removing stale transaction from cache");
                    }
                }
            }
        });

        cache
    }

    /// Begin a transaction, storing the associated connection in the cache
    #[tracing::instrument(skip(self))]
    pub async fn begin(&self, key: K) -> Result<Uuid, Error<<P::Connection as Connection>::Error>> {
        // generate a unique transaction ID to be included in subsequent requests
        let transaction_id = Uuid::new_v4();

        tracing::info!(transaction = %&transaction_id, "Beginning transaction");

        let transaction_key = TransactionKey {
            key: key.clone(),
            transaction_id,
        };

        // convert a pool connection into a transaction
        let connection = self
            .pool
            .get_connection(key)
            .await
            .map_err(|_| Error::ConnectionFailure)?;

        connection.batch("BEGIN").await.map_err(Error::Connection)?;

        let transaction = Transaction::new(Arc::new(connection));

        // save the transaction to the cache
        self.transactions
            .write()
            .await
            .insert(transaction_key, transaction);

        // return the transaction's unique ID for later use
        tracing::info!(transaction = %&transaction_id, "Transaction succesfully cached");

        Ok(transaction_id)
    }

    /// Remove a transaction from the cache, committing its changeset in postgres
    #[tracing::instrument(skip(self))]
    pub async fn commit(
        &self,
        transaction_id: Uuid,
        key: K,
    ) -> Result<(), Error<<P::Connection as Connection>::Error>> {
        tracing::info!("Committing active transaction");

        self.remove(transaction_id, key)
            .await?
            .connection
            .batch("COMMIT")
            .await
            .map_err(Error::Connection)?;

        Ok(())
    }

    /// Remove a transaction from the cache, rolling back all intermediate changes
    #[tracing::instrument(skip(self))]
    pub async fn rollback(
        &self,
        transaction_id: Uuid,
        key: K,
    ) -> Result<(), Error<<P::Connection as Connection>::Error>> {
        tracing::info!("Rolling back active transaction");

        self.remove(transaction_id, key)
            .await?
            .connection
            .batch("ROLLBACK")
            .await
            .map_err(Error::Connection)?;

        Ok(())
    }

    async fn remove(
        &self,
        transaction_id: Uuid,
        key: K,
    ) -> Result<Transaction<P::Connection>, Error<<P::Connection as Connection>::Error>> {
        tracing::info!("Removing transaction from the cache");

        let transaction = self
            .transactions
            .write()
            .await
            .remove(&TransactionKey {
                key,
                transaction_id,
            })
            .ok_or(Error::Uninitialized)?;

        Ok(transaction)
    }
}

#[tonic::async_trait]
impl<P, K> super::Pool<TransactionKey<K>> for Pool<P, K>
where
    K: Hash + Eq + Send + Sync + Clone,
    P: super::Pool<K> + Send + Sync,
    P::Connection: Send + Sync + 'static,
    <P::Connection as Connection>::Error: std::error::Error + Send + Sync + 'static,
{
    type Connection = Transaction<P::Connection>;
    type Error = Error<<P::Connection as Connection>::Error>;

    async fn get_connection(
        &self,
        key: TransactionKey<K>,
    ) -> Result<Self::Connection, Self::Error> {
        let transaction = self
            .transactions
            .read()
            .await
            .get(&key)
            .cloned()
            .ok_or(Error::Uninitialized)?;

        Ok(transaction)
    }
}
