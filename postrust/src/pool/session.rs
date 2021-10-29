use super::{
    cluster::{self, Cluster},
    endpoint,
};
use crate::protocol::{
    backend::{self, TransactionStatus},
    frontend,
};
use futures_util::TryStreamExt;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error reading messages from backend connection: {0}")]
    Broadcast(#[from] BroadcastStreamRecvError),
    #[error(transparent)]
    Cluster(#[from] cluster::Error),
    #[error(transparent)]
    Endpoint(#[from] endpoint::Error),
    #[error("Cluster onfiguration for the current user is missing a leader")]
    MissingLeader,
    #[error("Error syncing messages between connections")]
    Sync,
}

/// User session wrapper that brokers messages between clients and pooled clusters
#[derive(Clone)]
pub struct Session {
    cluster: Arc<Cluster>,
    transmitter: UnboundedSender<backend::Message>,
}

impl Session {
    /// Create a new session
    pub(super) fn new(
        cluster: Arc<Cluster>,
        transmitter: UnboundedSender<backend::Message>,
    ) -> Self {
        Self {
            cluster,
            transmitter,
        }
    }

    /// Return an exclusive handle to a leader connection
    pub fn leader(&self) -> Leader {
        Leader {
            transmitter: self.transmitter.clone(),
            cluster: Arc::clone(&self.cluster),
        }
    }
}

pub struct Leader {
    transmitter: UnboundedSender<backend::Message>,
    cluster: Arc<Cluster>,
}

impl Leader {
    #[tracing::instrument(skip(self))]
    pub async fn send(&self, message: frontend::Message) -> Result<(), Error> {
        // pick the next endpoint in the round robin endpoint pool
        let leader = &self.cluster.leaders.next().ok_or(Error::MissingLeader)?;

        // initialize a connection and subscribe to its messages
        let mut backend_messages = leader.subscribe_next_idle(Some(message)).await?;

        // forward backend messages back to the clients
        while let Some(message) = backend_messages.try_next().await? {
            self.transmitter.send(message).map_err(|_| Error::Sync)?;
        }

        // send the skipped ReadyForQuery message
        self.transmitter
            .send(backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            })
            .map_err(|_| Error::Sync)?;

        Ok(())
    }
}