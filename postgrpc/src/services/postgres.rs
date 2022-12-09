use crate::{
    extensions::FromRequest,
    pools::{Connection, Parameter, Pool},
};
use futures_util::{pin_mut, StreamExt, TryStream, TryStreamExt};
use proto::postgres_server::{Postgres as GrpcService, PostgresServer};
pub use proto::QueryRequest;
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::mpsc::error::SendError;
use tokio_postgres::types::ToSql;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::InterceptedService, service::Interceptor, Request, Response, Status};

/// Compiled protocol buffers for the Postgres service
#[allow(unreachable_pub, missing_docs, clippy::derive_partial_eq_without_eq)]
mod proto {
    tonic::include_proto!("postgres.v1");
}

/// Protocol-agnostic Postgres handlers for any connection pool
#[derive(Clone)]
pub struct Postgres<P, R> {
    pool: Arc<P>,
    _row_type_marker: PhantomData<R>,
}

impl<P, R> Postgres<P, R>
where
    P: Pool<R>,
    R: Send,
{
    /// Create a new Postgres service from a reference-counted Pool
    fn new(pool: Arc<P>) -> Self {
        Self {
            pool,
            _row_type_marker: PhantomData,
        }
    }

    /// Query a Postgres database, returning a stream of rows
    #[tracing::instrument(skip(self, parameters), err)]
    async fn query(
        &self,
        key: P::Key,
        statement: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<<P::Connection as Connection<R>>::RowStream, P::Error> {
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
impl<P, R> GrpcService for Postgres<P, R>
where
    P: Pool<R> + 'static,
    P::Key: FromRequest,
    <<P::Connection as Connection<R>>::RowStream as TryStream>::Ok: Into<pbjson_types::Struct>,
    R: Send + Sync + 'static,
{
    type QueryStream = ReceiverStream<Result<pbjson_types::Struct, Status>>;

    #[tracing::instrument(skip(self, request), err)]
    async fn query(
        &self,
        mut request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // derive a key from extensions to use as a connection pool key
        let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;

        // get the request values
        let QueryRequest { statement, values } = request.into_inner();

        // convert values to valid parameters
        let values = values.into_iter().map(Parameter::from).collect::<Vec<_>>();
        let mut parameters: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(values.len());

        for parameter in values.iter() {
            parameters.push(parameter);
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
pub fn new<P, R>(pool: Arc<P>) -> PostgresServer<Postgres<P, R>>
where
    P: Pool<R> + 'static,
    P::Key: FromRequest,
    <<P::Connection as Connection<R>>::RowStream as TryStream>::Ok: Into<pbjson_types::Struct>,
    R: Send + Sync + 'static,
{
    PostgresServer::new(Postgres::new(pool))
}

/// Create a new Postgres service from a connection pool and an interceptor
pub fn with_interceptor<P, I, R>(
    pool: Arc<P>,
    interceptor: I,
) -> InterceptedService<PostgresServer<Postgres<P, R>>, I>
where
    P: Pool<R> + 'static,
    P::Key: FromRequest,
    <<P::Connection as Connection<R>>::RowStream as TryStream>::Ok: Into<pbjson_types::Struct>,
    I: Interceptor,
    R: Send + Sync + 'static,
{
    PostgresServer::with_interceptor(Postgres::new(pool), interceptor)
}
