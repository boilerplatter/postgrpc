use crate::{
    error_to_status, extensions,
    proto::postgres::{postgres_server::Postgres as GrpcService, QueryRequest},
    protocol::{json, parameter},
};
use futures_util::{pin_mut, StreamExt, TryStreamExt};
use postgres_role_json_pool::Pool;
use postgres_services::postgres::Postgres;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// gRPC service implementation for Postgres service using the default pool
#[tonic::async_trait]
impl GrpcService for Postgres<Pool> {
    type QueryStream = ReceiverStream<Result<prost_types::Struct, Status>>;

    #[tracing::instrument(skip(self))]
    async fn query(
        &self,
        mut request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // derive a role from extensions to use as a connection pool key
        let role = request
            .extensions_mut()
            .remove::<extensions::Postgres>()
            .ok_or_else(|| Status::internal("Failed to load extensions before handling request"))?
            .role;

        // get the request values
        let QueryRequest { statement, values } = request.into_inner();

        // convert values to valid parameters
        let value_count = values.len();

        let parameters: Vec<_> = values
            .into_iter()
            .filter_map(parameter::from_proto_value)
            .collect();

        if parameters.len() < value_count {
            return Err(
                Status::invalid_argument(
                    "Invalid parameter values found. Only numbers, strings, boolean, and null values permitted"
                    )
                );
        }

        // get the rows, converting output to proto-compatible structs and statuses
        let rows = Postgres::query(self, role, &statement, &parameters)
            .await
            .map_err(error_to_status)?
            .map_ok(json::map::to_proto_struct)
            .map_err(error_to_status);

        // create the row stream transmitter and receiver
        let (transmitter, receiver) = tokio::sync::mpsc::channel(100);

        // emit the rows as a Send stream
        tokio::spawn(async move {
            pin_mut!(rows);

            while let Some(row) = rows.next().await {
                transmitter.send(row).await?;
            }

            Ok::<_, SendError<_>>(())
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
