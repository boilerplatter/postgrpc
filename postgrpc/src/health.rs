use crate::proto::health::{
    health_check_response::ServingStatus, health_server::Health as GrpcService, HealthCheckRequest,
    HealthCheckResponse,
};
use futures_util::{pin_mut, stream, StreamExt, TryFutureExt};
use postgres_pool::{Connection, Pool};
use std::{sync::Arc, time::Duration};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

pub struct Health<P>
where
    P: Pool,
    P::Key: Default,
{
    pool: Arc<P>,
}

impl<P> Health<P>
where
    P: Pool,
    P::Key: Default,
{
    pub fn new(pool: Arc<P>) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl<P> GrpcService for Health<P>
where
    P: Pool + 'static,
    P::Key: Send + Sync + Default,
{
    #[tracing::instrument(skip(self))]
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        tracing::debug!("Health check received");

        // use the default value of the given key
        let key = P::Key::default();

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

        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving.into(),
        }))
    }

    type WatchStream = UnboundedReceiverStream<Result<HealthCheckResponse, Status>>;

    #[tracing::instrument(skip(self))]
    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        tracing::debug!("Health check stream started");

        // get a reference to the pool
        let pool = Arc::clone(&self.pool);

        let count = 1;
        let watch_stream = stream::unfold(count, move |count| {
            let key = P::Key::default();
            let pool = Arc::clone(&pool);

            async move {
                // attempt to make a simple query against the pool
                let response = pool
                    .get_connection(key)
                    .map_err(|error| Status::unavailable(error.to_string()))
                    .and_then(|connection| async move {
                        connection
                            .query("SELECT 1", &[])
                            .await
                            .map_err(|error| Status::unavailable(error.to_string()))
                    })
                    .await
                    .map(|_| HealthCheckResponse {
                        status: ServingStatus::Serving.into(),
                    });

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

            Ok::<_, tokio::sync::mpsc::error::SendError<_>>(())
        });

        Ok(Response::new(UnboundedReceiverStream::new(receiver)))
    }
}
