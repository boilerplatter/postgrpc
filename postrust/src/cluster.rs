use crate::{
    connection::Connection,
    endpoint::Endpoint,
    pool::{Pool, Pooled, PooledObject, Pools},
    protocol::{
        backend,
        errors::{CONNECTION_DOES_NOT_EXIST, TOO_MANY_CONNECTIONS},
    },
    tcp,
};
use futures_util::{stream::FuturesUnordered, TryStreamExt};
use once_cell::sync::Lazy;
use postguard::{AllowedFunctions, AllowedStatements, Guard};
use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Cluster configuration for the current user is missing a leader")]
    MissingLeader,
}

impl From<&Error> for backend::Message {
    fn from(error: &Error) -> Self {
        match error {
            Error::Tcp(error) => Self::from(error),
            Error::MissingLeader => Self::ErrorResponse {
                code: CONNECTION_DOES_NOT_EXIST.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Fatal,
            },
        }
    }
}

/// Timeout for fetching a connection from upstream
// FIXME: make this configurable
static POOL_FETCH_TIMEOUT: Duration = Duration::from_millis(1000);

/// Standard delay between retries when fetching from a pool
// FIXME: make this configurable
static POOL_FETCH_DELAY: Duration = Duration::from_millis(200);

/// Pooled clusters keyed by configurations
pub static CLUSTERS: Lazy<RwLock<HashMap<Key, Arc<Cluster>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Cluster identifier based on the leaders and follower endpoint configurations
type Key = (Vec<Endpoint>, Vec<Endpoint>);

// TODO: turn this into a proto for gRPC handlers, too
/// Configuration for a Cluster of leaders and followers
#[derive(Debug)]
pub struct Configuration {
    pub leaders: Vec<Endpoint>,
    pub followers: Vec<Endpoint>,
    pub statement_guard: Guard,
}

impl Default for Configuration {
    // FIXME: remove this impl
    fn default() -> Self {
        let leader = Endpoint::new(
            "postgres".into(),
            "supersecretpassword".into(),
            "postgres".into(),
            [127, 0, 0, 1].into(),
            5432,
        );

        // FIXME: make this something other than a noop
        let statement_guard = Guard::new(
            // AllowedStatements::List(vec![Command::Select]),
            AllowedStatements::All,
            AllowedFunctions::All,
            // AllowedFunctions::List(vec!["to_json".to_string(), "pg_sleep".to_string()]),
        );

        Self {
            leaders: vec![leader],
            followers: vec![],
            statement_guard,
        }
    }
}

/// Wrapper around the cluster connections for a single auth response
pub struct Cluster<P = Connection>
where
    P: Pooled<Error = tcp::Error>,
    P::Configuration: fmt::Debug,
{
    leaders: Pools<P>,
    followers: Pools<P>,
}

/// Implement methods specific to TCP-based database Connections
impl Cluster {
    #[tracing::instrument]
    pub async fn connect(leaders: Vec<Endpoint>, followers: Vec<Endpoint>) -> Result<Self, Error> {
        // guard against empty leader configurations
        if leaders.is_empty() {
            return Err(Error::MissingLeader);
        }

        // store the endpoints and connections for later use
        let proxied_leaders = Pools::new(
            leaders
                .into_iter()
                .map(Pool::new)
                .collect::<FuturesUnordered<_>>()
                .try_collect()
                .await?,
        );

        let proxied_followers = Pools::new(
            followers
                .into_iter()
                .map(Pool::new)
                .collect::<FuturesUnordered<_>>()
                .try_collect()
                .await?,
        );

        Ok(Self {
            leaders: proxied_leaders,
            followers: proxied_followers,
        })
    }
}

/// Implement methods for all pool types
impl<P> Cluster<P>
where
    P: Pooled<Error = tcp::Error>,
    P::Configuration: fmt::Debug,
{
    #[tracing::instrument]
    pub async fn leader(&self) -> Result<PooledObject<P>, Error> {
        fetch_with_retry(&self.leaders).await
    }

    #[tracing::instrument]
    pub async fn follower(&self) -> Result<PooledObject<P>, Error> {
        if self.followers.is_empty() {
            self.leader().await
        } else {
            fetch_with_retry(&self.followers).await
        }
    }
}

/// Implement test-specific methods
#[cfg(test)]
impl<P> Cluster<P>
where
    P: Pooled<Error = tcp::Error> + 'static,
    P::Configuration: Default + fmt::Debug,
{
    /// Handle creation of test cluster from a poolable object
    pub fn test(object: P) -> Self {
        let leaders = Pools::new(vec![Pool::test(object)]);
        let followers = Pools::new(vec![]);

        Self { leaders, followers }
    }
}

impl<P> fmt::Debug for Cluster<P>
where
    P: Pooled<Error = tcp::Error>,
    P::Configuration: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("Cluster")
            .field("leaders", &self.leaders)
            .field("followers", &self.leaders)
            .finish()
    }
}

/// Fetch a single connection from a set of Endpoints with retries
async fn fetch_with_retry<P>(endpoints: &Pools<P>) -> Result<PooledObject<P>, Error>
where
    P: Pooled<Error = tcp::Error>,
    P::Configuration: fmt::Debug,
{
    let start = Instant::now();

    let connection = loop {
        let connections = endpoints.next().ok_or(Error::MissingLeader)?;

        match connections.get().await {
            Err(tcp::Error::Upstream {
                code,
                message,
                severity,
            }) if code.as_ref() == TOO_MANY_CONNECTIONS => {
                // exit early if the retry timeout has expired
                if Instant::now().duration_since(start) > POOL_FETCH_TIMEOUT {
                    return Err(Error::Tcp(tcp::Error::Upstream {
                        code,
                        message,
                        severity,
                    }));
                }

                // retry after a delay if possible
                tracing::warn!(
                    ?code,
                    ?message,
                    "Retrying connection after recoverable upstream error"
                );

                tokio::time::sleep(POOL_FETCH_DELAY).await;
            }
            Err(error) => return Err(error.into()),
            Ok(connection) => break connection,
        };
    };

    Ok(connection)
}
