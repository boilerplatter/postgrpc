use crate::{
    error_to_status, get_role,
    proto::transaction::{
        transaction_server::Transaction as GrpcService, BeginResponse, CommitRequest,
        RollbackRequest, TransactionQueryRequest,
    },
    protocol::{json, parameter},
};
use futures::{pin_mut, StreamExt, TryStreamExt};
use postreq::{pools, transaction::Transaction};
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// map transaction pool errors to proper gRPC statuses
fn transaction_error_to_status(error: pools::transaction::Error<pools::default::Error>) -> Status {
    let message = error.to_string();

    match error {
        pools::transaction::Error::ConnectionFailure => Status::resource_exhausted(message),
        pools::transaction::Error::Uninitialized => Status::failed_precondition(message),
        pools::transaction::Error::Connection(error) => error_to_status(error),
    }
}

/// gRPC service implementation for Transaction service
#[tonic::async_trait]
impl GrpcService for Transaction {
    type QueryStream = ReceiverStream<Result<prost_types::Struct, Status>>;

    #[tracing::instrument(skip(self))]
    async fn query(
        &self,
        request: Request<TransactionQueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // derive a role from headers to use as a connection pool key
        let role = get_role(&request)?;

        // get the request values
        let TransactionQueryRequest {
            id,
            statement,
            values,
        } = request.into_inner();

        let id = Uuid::parse_str(&id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

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
        let rows = Transaction::query(self, id, role, &statement, &parameters)
            .await
            .map_err(transaction_error_to_status)?
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

    async fn begin(&self, request: Request<()>) -> Result<Response<BeginResponse>, Status> {
        let role = get_role(&request)?;

        let id = Transaction::begin(self, role)
            .await
            .map_err(transaction_error_to_status)?
            .to_string();

        Ok(Response::new(BeginResponse { id }))
    }

    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<()>, Status> {
        let role = get_role(&request)?;

        let CommitRequest { id } = request.get_ref();

        let id = Uuid::parse_str(&id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        Transaction::commit(self, id, role)
            .await
            .map_err(transaction_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        let role = get_role(&request)?;

        let RollbackRequest { id } = request.get_ref();

        let id = Uuid::parse_str(&id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        Transaction::rollback(self, id, role)
            .await
            .map_err(transaction_error_to_status)?;

        Ok(Response::new(()))
    }
}
