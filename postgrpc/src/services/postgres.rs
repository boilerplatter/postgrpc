use crate::pools::{Connection, FromRequest, Parameter, Pool};
use futures_util::{pin_mut, StreamExt, TryStreamExt};
use proto::postgres_server::{Postgres as GrpcService, PostgresServer};
pub use proto::QueryRequest;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::InterceptedService, service::Interceptor, Request, Response, Status};

/// Compiled protocol buffers for the Postgres service
#[allow(unreachable_pub, missing_docs)]
mod proto {
    tonic::include_proto!("postgres.v1");
}

/// Protocol-agnostic Postgres handlers for any connection pool
#[derive(Clone)]
pub struct Postgres<P> {
    pool: Arc<P>,
}

impl<P> Postgres<P>
where
    P: Pool,
{
    /// Create a new Postgres service from a reference-counted Pool
    fn new(pool: Arc<P>) -> Self {
        Self { pool }
    }

    /// Query a Postgres database, returning a stream of rows
    #[tracing::instrument(skip(self))]
    async fn query(
        &self,
        key: P::Key,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<<P::Connection as Connection>::RowStream, P::Error> {
        tracing::info!("Querying postgres");

        let rows = self
            .pool
            .get_connection(key)
            .await?
            .query(statement, parameters)
            .await?;

        Ok(rows)
    }
}

/// gRPC service implementation for Postgres service using the default pool
#[tonic::async_trait]
impl<P> GrpcService for Postgres<P>
where
    P: Pool + 'static,
    P::Key: FromRequest,
{
    type QueryStream = ReceiverStream<Result<pbjson_types::Struct, Status>>;

    #[tracing::instrument(skip(self))]
    async fn query(
        &self,
        mut request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // derive a key from extensions to use as a connection pool key
        let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;

        // get the request values
        let QueryRequest { statement, values } = request.into_inner();

        // convert values to valid parameters
        let value_count = values.len();

        let parameters: Vec<_> = values.into_iter().map(Parameter::from).collect();

        if parameters.len() < value_count {
            return Err(
                Status::invalid_argument(
                    "Invalid parameter values found. Only numbers, strings, boolean, and null values permitted"
                )
            );
        }

        // get the rows, converting output to proto-compatible structs and statuses
        let rows = Postgres::query(self, key, &statement, &parameters)
            .await
            .map_err(Into::<Status>::into)?
            .map_ok(Into::into)
            .map_err(Into::<Status>::into);

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

/// Create a new Postgres service from a connection pool
pub fn new<P>(pool: Arc<P>) -> PostgresServer<Postgres<P>>
where
    P: Pool + 'static,
    P::Key: FromRequest,
{
    PostgresServer::new(Postgres::new(pool))
}

/// Create a new Postgres service from a connection pool and an interceptor
pub fn with_interceptor<P, I>(
    pool: Arc<P>,
    interceptor: I,
) -> InterceptedService<PostgresServer<Postgres<P>>, I>
where
    P: Pool + 'static,
    P::Key: FromRequest,
    I: Interceptor,
{
    PostgresServer::with_interceptor(Postgres::new(pool), interceptor)
}
