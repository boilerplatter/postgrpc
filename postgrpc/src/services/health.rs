use crate::pools::{Connection, Pool};
use futures_util::{pin_mut, stream, StreamExt};
use proto::{
    health_check_response::ServingStatus,
    health_server::{Health as GrpcService, HealthServer},
};
pub use proto::{HealthCheckRequest, HealthCheckResponse};
use std::{hash::Hash, sync::Arc, time::Duration};
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

// FIXME: use tonic_health

#[allow(unreachable_pub, missing_docs)]
mod proto {
    tonic::include_proto!("health");
}

/// Health service implementation that checks the connections associated with each service
pub struct Health<P>
where
    P: Pool,
    P::Key: Hash + Eq + Default + Clone,
{
    pool: Arc<P>,
    #[cfg(feature = "transaction")]
    transactions: crate::pools::transaction::Pool<P>,
}

impl<P> Clone for Health<P>
where
    P: Pool,
    P::Key: Hash + Eq + Default + Clone,
{
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            #[cfg(feature = "transaction")]
            transactions: self.transactions.clone(),
        }
    }
}

impl<P> Health<P>
where
    P: Pool + 'static,
    P::Key: Hash + Eq + Default + Clone + Send + Sync,
    <P::Connection as Connection>::Error: Send + Sync,
{
    /// Create a new health service from a connection pool
    fn new(pool: Arc<P>) -> Self {
        Self {
            #[cfg(feature = "transaction")]
            transactions: crate::pools::transaction::Pool::new(Arc::clone(&pool)),
            pool,
        }
    }

    async fn check_postgres_service(&self, key: P::Key) -> Result<(), Status> {
        // attempt to get a connection from the pool
        let connection = self
            .pool
            .get_connection(key)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;

        // attempt to make a simple query against the pool
        connection
            .query("SELECT 1", &[])
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;

        Ok(())
    }

    #[cfg(feature = "transaction")]
    async fn check_transaction_service(&self, key: P::Key) -> Result<(), Status> {
        // attempt to start a transaction
        let id = self
            .transactions
            .begin(key.clone())
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;

        let transaction_key = crate::pools::transaction::Key::new(key.clone(), id);

        // attempt to retrieve the active transaction
        let transaction = self
            .transactions
            .get_connection(transaction_key)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;

        // attempt to make a simple query against the transaction
        transaction
            .query("SELECT 1", &[])
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;

        // attempt to roll the transaction back
        self.transactions
            .rollback(id, key)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;

        Ok(())
    }
}

#[tonic::async_trait]
impl<P> GrpcService for Health<P>
where
    P: Pool + 'static,
    P::Key: Hash + Eq + Default + Clone + Send + Sync,
    <P::Connection as Connection>::Error: Send + Sync,
{
    #[tracing::instrument(skip(self))]
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        // use the default value of the given key
        let key = P::Key::default();

        // parse the service from the request
        match request.into_inner().service.to_lowercase().as_str() {
            "" => {
                #[cfg(feature = "transaction")]
                self.check_transaction_service(key.clone()).await?;
                self.check_postgres_service(key).await?;
            }
            "postgres" => self.check_postgres_service(key).await?,
            #[cfg(feature = "transaction")]
            "transaction" => self.check_transaction_service(key).await?,
            service => {
                return Err(Status::not_found(format!(
                    "Service '{}' does not exist",
                    service
                )))
            }
        };

        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving.into(),
        }))
    }

    type WatchStream = UnboundedReceiverStream<Result<HealthCheckResponse, Status>>;

    #[tracing::instrument(skip(self))]
    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        // set up streamable clones of health check components
        let health_service = self.clone();
        let request = request.into_inner();
        let count = 1;

        // unfold an infinite stream of health checks
        let watch_stream = stream::unfold(count, move |count| {
            let health_service = health_service.clone();
            let request = Request::new(request.clone());

            async move {
                let response = health_service
                    .check(request)
                    .await
                    .map(|response| response.into_inner());

                Some((response, count + 1))
            }
        });

        // set up transmitters
        let (transmitter, receiver) = tokio::sync::mpsc::unbounded_channel();

        // spawn the watch loop
        tokio::spawn(async move {
            pin_mut!(watch_stream);

            while let Some(response) = watch_stream.next().await {
                transmitter.send(response)?;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            Ok::<_, SendError<_>>(())
        });

        Ok(Response::new(UnboundedReceiverStream::new(receiver)))
    }
}

/// Create a new Health service from a connection pool
pub fn new<P>(pool: Arc<P>) -> HealthServer<Health<P>>
where
    P: Pool + 'static,
    P::Key: Hash + Eq + Default + Clone,
{
    HealthServer::new(Health::new(pool))
}
