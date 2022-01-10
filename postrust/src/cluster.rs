use crate::{
    endpoint::{Endpoint, Endpoints},
    pool::{self, Pool},
};
use futures_util::{stream::FuturesUnordered, TryStreamExt};
use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Pool(#[from] pool::Error),
    #[error("Cluster configuration for the current user is missing a leader")]
    MissingLeader,
}

/// Pooled clusters keyed by configurations
pub static CLUSTERS: Lazy<RwLock<HashMap<Key, Arc<Cluster>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Cluster identifier based on the leaders and follower endpoint configurations
type Key = (Vec<Endpoint>, Vec<Endpoint>);

/// Wrapper around the cluster connections for a single auth response
pub struct Cluster {
    leaders: Endpoints,
    followers: Endpoints,
}

impl Cluster {
    /// Create a new load-balanced cluster from sets of leader and follower endpoints
    pub async fn connect(leaders: Vec<Endpoint>, followers: Vec<Endpoint>) -> Result<Self, Error> {
        // guard against empty leader configurations
        if leaders.is_empty() {
            return Err(Error::MissingLeader);
        }

        // store the endpoints and connections for later use
        let proxied_leaders = Endpoints::new(
            leaders
                .into_iter()
                .map(Pool::new)
                .collect::<FuturesUnordered<_>>()
                .try_collect()
                .await?,
        );

        let proxied_followers = Endpoints::new(
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

    /// Fetch a single leader connection
    pub async fn leader(&self) -> Result<pool::PooledConnection<'_>, Error> {
        let connections = self.leaders.next().ok_or(Error::MissingLeader)?;
        let connection = connections.get().await?;

        Ok(connection)
    }

    /// Fetch a single follower connection, falling back to the leader if no followers have been configured
    pub async fn follower(&self) -> Result<pool::PooledConnection<'_>, Error> {
        match self.followers.next() {
            Some(connections) => {
                let connection = connections.get().await?;

                Ok(connection)
            }
            None => self.leader().await,
        }
    }
}
