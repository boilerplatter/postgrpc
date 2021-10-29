use super::endpoint::{self, Endpoint, ProxiedConnections, RoundRobinEndpoints};
use crate::{
    connections::{self, Connection},
    protocol::backend,
};
use futures_util::TryStreamExt;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error(transparent)]
    Endpoint(#[from] endpoint::Error),
    #[error("Cluster onfiguration for the current user is missing a leader")]
    MissingLeader,
}

/// Pooled clusters keyed by configurations
pub type Clusters = RwLock<HashMap<(Vec<Endpoint>, Vec<Endpoint>), Arc<Cluster>>>;

/// Wrapper around the cluster connections for a single auth response
pub struct Cluster {
    startup_messages: Vec<backend::Message>,
    pub(super) leaders: RoundRobinEndpoints,
    #[allow(unused)]
    pub(super) followers: RoundRobinEndpoints,
}

impl Cluster {
    /// Create a new load-balanced cluster from sets of leader and follower endpoints
    pub async fn connect(
        mut leaders: Vec<Endpoint>,
        followers: Vec<Endpoint>,
    ) -> Result<Self, Error> {
        // guard against empty leader configurations
        if leaders.is_empty() {
            return Err(Error::MissingLeader);
        }

        // connect to the first leader
        let leader = leaders.swap_remove(0);
        let upstream_address = leader.address();

        let proxied_connection = Connection::<backend::Codec>::connect(
            upstream_address,
            leader.user.to_string(),
            leader.password.to_string(),
            leader.database.to_string(),
        )
        .await?;

        // drain the startup messages from the leader
        let proxied_leader_connections = ProxiedConnections::new(leader, [proxied_connection]);

        let startup_messages = proxied_leader_connections
            .subscribe_next_idle(None)
            .await?
            .try_collect()
            .await?;

        // store the endpoints and connections for later use
        let proxied_leaders = RoundRobinEndpoints::new(
            leaders
                .into_iter()
                .map(|leader| ProxiedConnections::new(leader, []))
                .chain(std::iter::once(proxied_leader_connections))
                .collect(),
        );

        let proxied_followers = RoundRobinEndpoints::new(
            followers
                .into_iter()
                .map(|follower| ProxiedConnections::new(follower, []))
                .collect(),
        );

        Ok(Self {
            startup_messages,
            leaders: proxied_leaders,
            followers: proxied_followers,
        })
    }

    /// Get the startup messages for this cluster
    pub fn startup_messages(&self) -> Vec<backend::Message> {
        self.startup_messages.clone()
    }
}
