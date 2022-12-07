//! A database transaction meta-pool. This pool handles auto-vaccuming of
//! inactive transactions at configurable thresholds.

use super::Connection;
use futures_util::TryStream;
use std::{
    collections::HashMap,
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_postgres::types::ToSql;
use tonic::{async_trait, Status};
use tracing::Instrument;
use uuid::Uuid;

/// Transaction pool errors
#[derive(Error, Debug)]
pub enum Error<C>
where
    C: std::error::Error + 'static,
{
    /// Errors bubbled-up from a single connection drawn from the underlying Pool
    #[error(transparent)]
    Connection(C),
    /// Failure to retrieve a connection from the transaction pool
    #[error("Error retrieving connection from transaction pool")]
    ConnectionFailure,
    /// Transaction was called before `begin` or after `commit`/`rollback`
    #[error("Requested transaction has not been initialized or was cleaned up due to inactivity")]
    Uninitialized,
}

impl<C> From<Error<C>> for Status
where
    C: std::error::Error + Into<Status> + 'static,
{
    fn from(error: Error<C>) -> Self {
        match error {
            Error::Connection(error) => error.into(),
            Error::ConnectionFailure => Status::resource_exhausted(error.to_string()),
            Error::Uninitialized => Status::not_found(error.to_string()),
        }
    }
}

impl<C> From<C> for Error<C>
where
    C: std::error::Error + Into<Status> + 'static,
{
    fn from(connection_error: C) -> Self {
        Self::Connection(connection_error)
    }
}

// TODO: make these values configurable
/// Polling interval in seconds for cleanup operations
const VACUUM_POLLING_INTERVAL_SECONDS: u64 = 1;

/// Threshold in seconds for marking transactions as inactive
const INACTIVE_THRESHOLD_SECONDS: u64 = 30;

/// Time limit in seconds for any transaction, regardless of usage
const TRANSACTION_LIFETIME_LIMIT_SECONDS: u64 = 30 * 60;

// FIXME: add a concurrent transaction limit by key

/// Cached transaction data for an individual active transaction
pub struct Transaction<C, R>
where
    C: Connection<R>,
    R: Send,
{
    connection: Arc<C>,
    created_at: Instant,
    last_used_at: Arc<RwLock<Instant>>,
    _row_type_marker: PhantomData<R>,
}

impl<C, R> Transaction<C, R>
where
    C: Connection<R>,
    R: Send,
{
    fn new(connection: Arc<C>) -> Self {
        let now = Instant::now();

        Self {
            connection,
            created_at: now,
            last_used_at: Arc::new(RwLock::new(now)),
            _row_type_marker: PhantomData,
        }
    }
}

impl<C, R> Clone for Transaction<C, R>
where
    C: Connection<R>,
    R: Send,
{
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            created_at: self.created_at,
            last_used_at: Arc::clone(&self.last_used_at),
            _row_type_marker: self._row_type_marker,
        }
    }
}

#[async_trait]
impl<C, R> Connection<<C::RowStream as TryStream>::Ok> for Transaction<C, R>
where
    C: Connection<R> + Send + Sync + 'static,
    R: Send + Sync,
{
    type RowStream = C::RowStream;
    type Error = C::Error;

    #[tracing::instrument(skip(self, parameters))]
    async fn query<P>(
        &self,
        statement: &str,
        parameters: &[P],
    ) -> Result<Self::RowStream, Self::Error>
    where
        P: ToSql + Sync,
    {
        tracing::trace!("Querying transaction Connection");
        let rows = self.connection.query(statement, parameters).await?;
        let mut last_used_at = self.last_used_at.write().await;
        *last_used_at = Instant::now();
        Ok(rows)
    }

    #[tracing::instrument(skip(self))]
    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        tracing::trace!("Executing batch query on transaction Connection");
        self.connection.batch(query).await?;
        let mut last_used_at = self.last_used_at.write().await;
        *last_used_at = Instant::now();
        Ok(())
    }
}

/// Key for interacting with active transactions in the cache,
/// checking access against the original connection pool key
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct Key<K>
where
    K: Hash + Eq,
{
    key: K,
    transaction_id: Uuid,
}

impl<K> Key<K>
where
    K: Hash + Eq,
{
    /// Pair a connection pool Key with a unique transaction ID
    pub fn new(key: K, transaction_id: Uuid) -> Self {
        Self {
            key,
            transaction_id,
        }
    }
}

/// Type alias for the internal map of shared transactions
type TransactionMap<K, C, R> = HashMap<Key<K>, Transaction<C, R>>;

/// Pool of active transactions that wraps a lower-level Pool implementation
pub struct Pool<P, R>
where
    P: super::Pool<R>,
    P::Key: Hash + Eq + Clone,
    R: Send + Sync,
{
    pool: Arc<P>,
    #[allow(clippy::type_complexity)]
    transactions: Arc<RwLock<TransactionMap<P::Key, P::Connection, R>>>,
}

impl<P, R> Clone for Pool<P, R>
where
    P: super::Pool<R>,
    P::Key: Hash + Eq + Clone,
    R: Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            transactions: Arc::clone(&self.transactions),
        }
    }
}

impl<P, R> Pool<P, R>
where
    P: super::Pool<R> + 'static,
    P::Key: Hash + Eq + Send + Sync + Clone + 'static,
    P::Connection: 'static,
    <P::Connection as Connection<R>>::Error: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    /// Initialize a new shared transaction pool
    #[tracing::instrument(skip(pool))]
    pub fn new(pool: Arc<P>) -> Self {
        tracing::debug!("Creating transaction pool from connection pool");

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
        tokio::spawn(
            async move {
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
                            tracing::error!(%error, "Error removing stale transaction from cache");
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("vacuum")),
        );

        cache
    }

    /// Begin a transaction, storing the associated connection in the cache
    #[tracing::instrument(skip(self))]
    pub async fn begin(
        &self,
        key: P::Key,
    ) -> Result<Uuid, Error<<P::Connection as Connection<R>>::Error>> {
        // generate a unique transaction ID to be included in subsequent requests
        let transaction_id = Uuid::new_v4();

        tracing::trace!(%transaction_id, "Beginning transaction");

        let transaction_key = Key {
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
        tracing::trace!(%transaction_id, "Transaction successfully cached");

        Ok(transaction_id)
    }

    /// Remove a transaction from the cache, committing its changeset in postgres
    #[tracing::instrument(skip(self))]
    pub async fn commit(
        &self,
        transaction_id: Uuid,
        key: P::Key,
    ) -> Result<(), Error<<P::Connection as Connection<R>>::Error>> {
        tracing::trace!("Committing active transaction");

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
        key: P::Key,
    ) -> Result<(), Error<<P::Connection as Connection<R>>::Error>> {
        tracing::trace!("Rolling back active transaction");

        self.remove(transaction_id, key)
            .await?
            .connection
            .batch("ROLLBACK")
            .await
            .map_err(Error::Connection)?;

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    #[tracing::instrument(skip(self))]
    async fn remove(
        &self,
        transaction_id: Uuid,
        key: P::Key,
    ) -> Result<Transaction<P::Connection, R>, Error<<P::Connection as Connection<R>>::Error>> {
        tracing::trace!("Removing transaction from the cache");

        let transaction = self
            .transactions
            .write()
            .await
            .remove(&Key {
                key,
                transaction_id,
            })
            .ok_or(Error::Uninitialized)?;

        Ok(transaction)
    }
}

#[async_trait]
impl<P, R> super::Pool<R> for Pool<P, R>
where
    P: super::Pool<R>,
    P::Key: Hash + Eq + Send + Sync + Clone,
    P::Connection: 'static,
    <P::Connection as Connection<R>>::Error: Send + Sync + Into<Status> + 'static,
    R: Send + Sync + 'static,
{
    type Key = Key<P::Key>;
    type Connection = Transaction<P::Connection, R>;
    type Error = Error<<Self::Connection as Connection<R>>::Error>;

    #[tracing::instrument(
        skip(self, key),
        fields(
            ?key = key.key,
            %transaction_id = key.transaction_id
        )
    )]
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error> {
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
