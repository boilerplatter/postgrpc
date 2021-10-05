use crate::proto::health::{
    health_check_response::ServingStatus, health_server::Health as GrpcService, HealthCheckRequest,
    HealthCheckResponse,
};
use futures::{stream, StreamExt};
use std::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

pub struct Health;

#[tonic::async_trait]
impl GrpcService for Health {
    #[tracing::instrument(skip(self))]
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        tracing::debug!("Health check received");

        // TODO: actually check the health of the services
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

        let (transmitter, receiver) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let health_check_response = HealthCheckResponse {
                status: ServingStatus::Serving.into(),
            };

            let mut watch_stream = stream::repeat(health_check_response);

            while let Some(response) = watch_stream.next().await {
                transmitter.send(Ok(response))?;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            Ok::<_, tokio::sync::mpsc::error::SendError<_>>(())
        });

        // TODO: actually check the health of the services
        Ok(Response::new(UnboundedReceiverStream::new(receiver)))
    }
}
