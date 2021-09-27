use crate::{
    pools::{self, Connection, Parameter, Pool},
    proto::postgres::{postgres_server::Postgres as GrpcService, QueryRequest},
};
use futures::{pin_mut, StreamExt, TryStream, TryStreamExt};
use prost_types::value::Kind;
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
    Pool(#[from] pools::Error),
    #[error("Error sending message through response stream: {0}")]
    Stream(#[from] SendError<Result<prost_types::Struct, Status>>),
    #[error("SQL Query Error: {0}")]
    Query(#[from] tokio_postgres::Error),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = format!("{}", &error);
        match error {
            Error::InvalidJson | Error::InvalidValues | Error::Query(..) => {
                Status::invalid_argument(message)
            }
            Error::Pool(..) | Error::Stream(..) => Status::internal(message),
        }
    }
}

/// Shared state for all Postgres services
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
    Error: From<P::Error> + From<<P::Connection as Connection>::Error>,
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
        // TODO: consider relaxing this constraint for specific cases later
        // (e.g. ListValues of a single type and JSON/serializable composite types for StructValues)
        let parameters: Vec<_> = values
            .iter()
            .filter_map(|value| match value.kind.as_ref() {
                Some(Kind::ListValue(..) | Kind::StructValue(..)) | None => None,
                Some(Kind::NullValue(..)) => Some(Parameter::Null),
                Some(Kind::BoolValue(boolean)) => Some(Parameter::Boolean(*boolean)),
                Some(Kind::NumberValue(number)) => Some(Parameter::Number(*number)),
                Some(Kind::StringValue(text)) => Some(Parameter::Text(text)),
            })
            .collect();

        if parameters.len() < values.len() {
            return Err(Error::InvalidValues);
        }

        // get a connection from the pool
        let connection = self.pool.get_connection(key).await?;

        // run the query, mapping to prost structs
        let rows = connection
            .query(statement, &parameters)
            .await?
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
            .map_ok(json_to_proto_struct);

        // map the row stream to prost structs
        Ok(rows)
    }
}

/// Convert a serde_json::Value into a prost_types::Value
fn json_to_proto_value(json: serde_json::Value) -> prost_types::Value {
    let kind = match json {
        serde_json::Value::Null => prost_types::value::Kind::NullValue(0),
        serde_json::Value::Bool(boolean) => prost_types::value::Kind::BoolValue(boolean),
        serde_json::Value::Number(number) => match number.as_f64() {
            Some(number) => prost_types::value::Kind::NumberValue(number),
            None => prost_types::value::Kind::StringValue(number.to_string()),
        },
        serde_json::Value::String(string) => prost_types::value::Kind::StringValue(string),
        serde_json::Value::Array(array) => {
            prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: array.into_iter().map(json_to_proto_value).collect(),
            })
        }
        serde_json::Value::Object(map) => {
            prost_types::value::Kind::StructValue(json_to_proto_struct(map))
        }
    };

    prost_types::Value { kind: Some(kind) }
}

/// Convert a serde_json::Map into a prost_types::Struct
fn json_to_proto_struct(map: serde_json::Map<String, serde_json::Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: map
            .into_iter()
            .map(|(key, value)| (key, json_to_proto_value(value)))
            .collect(),
    }
}

/// gRPC service implementation for Postgres service
#[tonic::async_trait]
impl<P> GrpcService for Postgres<P, Option<String>>
where
    P: Pool<Option<String>> + Send + Sync + 'static,
    P::Connection: Send + Sync,
    Error: From<P::Error> + From<<P::Connection as Connection>::Error>,
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
