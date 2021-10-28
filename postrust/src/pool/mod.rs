use crate::protocol::backend::{self, TransactionStatus};
use cluster::{Cluster, Clusters};
pub use endpoint::Endpoint;
use session::Session;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedReceiver;

mod cluster;
mod endpoint;
pub mod session;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Cluster(#[from] cluster::Error),
    #[error("Error flushing startup messages on session init")]
    Flush,
}

/// Pool of clusters generated for each set of connection credentials
#[derive(Default)]
pub struct Pool {
    clusters: Clusters,
}

impl Pool {
    /// Get or init a new session keyed by leader and follower configurations
    #[tracing::instrument(skip(self))]
    pub async fn get_session(
        &self,
        leaders: Vec<Endpoint>,
        followers: Vec<Endpoint>,
    ) -> Result<(Session, UnboundedReceiver<backend::Message>), Error> {
        tracing::debug!("Retrieving Session for Connection");

        let key = (leaders, followers);
        let clusters = self.clusters.read().await;

        // get or init the cluster for the session
        let cluster = match clusters.get(&key) {
            Some(cluster) => Arc::clone(cluster),
            None => {
                drop(clusters);

                let cluster = Cluster::connect(key.0.clone(), key.1.clone())
                    .await
                    .map(Arc::new)?;

                let session_cluster = Arc::clone(&cluster);

                self.clusters.write().await.insert(key, cluster);

                session_cluster
            }
        };

        // flush the cluster's startup messages on session init
        let (transmitter, receiver) = tokio::sync::mpsc::unbounded_channel();

        for message in cluster.startup_messages() {
            transmitter.send(message).map_err(|_| Error::Flush)?;
        }

        transmitter
            .send(backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            })
            .map_err(|_| Error::Flush)?;

        // return the receiver and active session
        tracing::info!("Session initiated");

        Ok((Session::new(cluster, transmitter), receiver))
    }
}
