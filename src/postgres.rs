use crate::{
    pools::{self, Connection, Pool},
    proto::postgres::{postgres_server::Postgres as GrpcService, QueryRequest},
    protocol::{self, Parameter},
};
use futures::{pin_mut, StreamExt, TryStream, TryStreamExt};
use std::{fmt, marker::PhantomData, sync::Arc};
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
    #[error(
        "Invalid parameter values found. Only numbers, strings, boolean, and null values permitted"
    )]
    InvalidValues,
    #[error(transparent)]
    Pool(#[from] pools::default::Error),
    #[error(transparent)]
    Rpc(#[from] Status),
    #[error("Error sending message through response stream: {0}")]
    Stream(#[from] SendError<Result<prost_types::Struct, Status>>),
    #[error("SQL Query Error: {0}")]
    Query(#[from] tokio_postgres::Error),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = format!("{}", &error);
        match error {
            Error::Rpc(status) => status,
            Error::InvalidJson | Error::InvalidValues | Error::Query(..) => {
                Status::invalid_argument(message)
            }
            Error::Pool(..) | Error::Stream(..) => Status::internal(message),
        }
    }
}

/// Protocol-agnostic Postgres handlers for any connection pool
#[derive(Clone)]
pub struct Postgres<P, K>
where
    P: Pool<K>,
{
    key: PhantomData<K>,
    pool: Arc<P>,
}

impl<P, K> Postgres<P, K>
where
    P: Pool<K>,
    K: fmt::Debug,
{
    pub fn new(pool: Arc<P>) -> Self {
        Self {
            key: PhantomData,
            pool,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn query(
        &self,
        key: K,
        statement: &str,
        values: &[prost_types::Value],
    ) -> Result<impl TryStream<Ok = prost_types::Struct, Error = Error>, Error> {
        tracing::info!("Querying database");

        // convert values to scalar parameters
        let parameters: Vec<_> = values
            .iter()
            .filter_map(|value| Parameter::from_proto_value(&value))
            .collect();

        if parameters.len() < values.len() {
            return Err(Error::InvalidValues);
        }

        // get a connection from the pool
        let connection = self
            .pool
            .get_connection(key)
            .await
            .map_err(|error| Error::Rpc(error.into()))?;

        // run the query, mapping to prost structs
        let rows = connection
            .query(statement, &parameters)
            .await
            .map_err(|error| Error::Rpc(error.into()))?
            .map_err(Error::from)
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
}

/// gRPC service implementation for Postgres service
#[tonic::async_trait]
impl<P> GrpcService for Postgres<P, Option<String>>
where
    P: Pool<Option<String>> + Send + Sync + 'static,
    P::Connection: Send + Sync,
{
    type QueryStream = ReceiverStream<Result<prost_types::Struct, Status>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // derive a role from headers to use as a connection pool key
        let role = request
            .metadata()
            .get("x-postgrpc-role")
            .map(|role| role.to_str())
            .transpose()
            .map_err(|_| Status::invalid_argument("Invalid role in x-postgres-role header"))?
            .map(String::from);

        // create the row stream transmitter and receiver
        let (transmitter, receiver) = tokio::sync::mpsc::channel(100);

        // get the row stream
        let QueryRequest { statement, values } = request.get_ref();

        let rows = Postgres::query(self, role, statement, values)
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
}
