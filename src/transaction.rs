use crate::{
    pools::{self, Connection, Pool},
    proto::transaction::{
        transaction_server::Transaction as GrpcService, BeginResponse, CommitRequest,
        RollbackRequest, TransactionQueryRequest,
    },
    protocol::{self, Parameter},
};
use futures::{pin_mut, StreamExt, TryStream, TryStreamExt};
use std::{fmt, hash::Hash, sync::Arc};
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
    #[error(
        "Invalid parameter values found. Only numbers, strings, boolean, and null values permitted"
    )]
    InvalidValues,
    #[error(transparent)]
    Pool(#[from] pools::transaction::Error),
    #[error("Error sending message through response stream: {0}")]
    Stream(#[from] SendError<Result<prost_types::Struct, Status>>),
    #[error(transparent)]
    Rpc(#[from] Status),
    #[error("SQL Query Error: {0}")]
    Query(#[from] tokio_postgres::Error),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = format!("{}", &error);
        match error {
            Error::Rpc(status) => status,
            Error::InvalidJson | Error::InvalidValues | Error::Query(..) => {
                Self::invalid_argument(message)
            }
            Error::Stream(..) => Self::internal(message),
            Error::Pool(error) => Self::from(error),
        }
    }
}

/// Protocol-agnostic Transaction handlers for any connection pool
#[derive(Clone)]
pub struct Transaction<P, K>
where
    K: Hash + Eq + Clone,
    P: Pool<K>,
{
    pool: pools::transaction::Pool<P, K>,
}

impl<P, K> Transaction<P, K>
where
    K: Hash + Eq + Send + Sync + fmt::Debug + Clone + 'static,
    P: Pool<K> + Send + Sync + 'static,
    P::Connection: Send + Sync + 'static,
{
    pub fn new(pool: Arc<P>) -> Self {
        Self {
            pool: pools::transaction::Pool::new(pool),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn begin(&self, key: K) -> Result<Uuid, Error> {
        tracing::info!("Beginning transaction");

        let transaction_id = self.pool.begin(key).await?;

        Ok(transaction_id)
    }

    #[tracing::instrument(skip(self))]
    async fn query(
        &self,
        id: Uuid,
        key: K,
        statement: &str,
        values: &[prost_types::Value],
    ) -> Result<impl TryStream<Ok = prost_types::Struct, Error = Error>, Error> {
        tracing::info!("Querying transaction");

        // convert values to scalar parameters
        let parameters: Vec<_> = values
            .iter()
            .filter_map(|value| Parameter::from_proto_value(&value))
            .collect();

        if parameters.len() < values.len() {
            return Err(Error::InvalidValues);
        }

        // get a connection from the pool
        let transaction_key = pools::transaction::TransactionKey::new(key, id);
        let connection = self.pool.get_connection(transaction_key).await?;

        // run the query, mapping to prost structs
        let rows = connection
            .query(statement, &parameters)
            .await
            .map_err(|error| Error::Rpc(error.into()))?
            .map_err(Error::Query)
            .and_then(|row| async move {
                let json_value = row.try_get::<_, serde_json::Value>("json")?;

                Ok::<_, Error>(json_value)
            })
            .and_then(|json_value| async move {
                if let serde_json::Value::Object(map) = json_value {
                    Ok(map)
                } else {
                    Err(Error::InvalidJson)
                }
            })
            .map_ok(protocol::json::to_proto_struct);

        // return the row stream
        Ok(rows)
    }

    #[tracing::instrument(skip(self))]
    async fn commit(&self, id: Uuid, key: K) -> Result<(), Error> {
        tracing::info!("Committing transaction");

        self.pool.commit(id, key).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn rollback(&self, id: Uuid, key: K) -> Result<(), Error> {
        tracing::info!("Rolling back transaction");

        self.pool.rollback(id, key).await?;

        Ok(())
    }
}

/// gRPC service implementation for Transaction service
#[tonic::async_trait]
impl<P> GrpcService for Transaction<P, Option<String>>
where
    P: Pool<Option<String>> + Send + Sync + 'static,
    P::Connection: Send + Sync + 'static,
{
    type QueryStream = ReceiverStream<Result<prost_types::Struct, Status>>;

    async fn query(
        &self,
        request: Request<TransactionQueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let role = get_role(&request)?;

        // create the row stream transmitter and receiver
        let (transmitter, receiver) = tokio::sync::mpsc::channel(100);

        // get the row stream
        let TransactionQueryRequest {
            id,
            statement,
            values,
        } = request.get_ref();

        let id = Uuid::parse_str(&id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        let rows = Transaction::query(self, id, role, statement, values)
            .await?
            .map_err(Status::from);

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

        let id = Transaction::begin(self, role).await?.to_string();

        Ok(Response::new(BeginResponse { id }))
    }

    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<()>, Status> {
        let role = get_role(&request)?;

        let CommitRequest { id } = request.get_ref();

        let id = Uuid::parse_str(&id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        Transaction::commit(self, id, role).await?;

        Ok(Response::new(()))
    }

    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        let role = get_role(&request)?;

        let RollbackRequest { id } = request.get_ref();

        let id = Uuid::parse_str(&id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        Transaction::rollback(self, id, role).await?;

        Ok(Response::new(()))
    }
}

/// derive a role from headers to use as a connection pool key
fn get_role<T>(request: &Request<T>) -> Result<Option<String>, Status> {
    let role = request
        .metadata()
        .get("x-postgrpc-role")
        .map(|role| role.to_str())
        .transpose()
        .map_err(|_| Status::invalid_argument("Invalid role in x-postgres-role header"))?
        .map(String::from);

    Ok(role)
}
