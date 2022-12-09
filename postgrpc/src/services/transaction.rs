use crate::{
    extensions::FromRequest,
    pools::{transaction, Connection, Parameter, Pool},
};
use futures_util::{pin_mut, StreamExt, TryStream, TryStreamExt};
use proto::transaction_server::{Transaction as GrpcService, TransactionServer};
pub use proto::{BeginResponse, CommitRequest, RollbackRequest, TransactionQueryRequest};
use std::{hash::Hash, sync::Arc};
use tokio::sync::mpsc::error::SendError;
use tokio_postgres::types::ToSql;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::InterceptedService, service::Interceptor, Request, Response, Status};
use uuid::Uuid;

/// Compiled protocol buffers for the Transaction service
#[allow(unreachable_pub, missing_docs, clippy::derive_partial_eq_without_eq)]
mod proto {
    tonic::include_proto!("transaction.v1");
}

/// Type alias representing a bubbled-up error from the transaction pool
type Error<P, R> = transaction::Error<<<P as Pool<R>>::Connection as Connection<R>>::Error>;

/// Protocol-agnostic Transaction handlers for any connection pool
#[derive(Clone)]
pub struct Transaction<P, R>
where
    P: Pool<R>,
    P::Key: Hash + Eq + Clone,
    R: Send + Sync,
{
    pool: transaction::Pool<P, R>,
}

impl<P, R> Transaction<P, R>
where
    P: Pool<R> + 'static,
    P::Key: Hash + Eq + Send + Sync + Clone + 'static,
    P::Connection: 'static,
    <P::Connection as Connection<R>>::Error: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    /// Create a new Postgres transaction service from a reference-counted Pool
    pub fn new(pool: Arc<P>) -> Self {
        Self {
            pool: transaction::Pool::new(pool),
        }
    }

    /// Begin a Postgres transaction, returning a unique ID for the transaction
    #[tracing::instrument(skip(self), err)]
    pub async fn begin(&self, key: P::Key) -> Result<Uuid, Error<P, R>> {
        tracing::debug!("Beginning transaction");

        let transaction_id = self.pool.begin(key).await?;

        Ok(transaction_id)
    }

    /// Query an active Postgres transaction by ID and connection pool key
    #[tracing::instrument(skip(self, parameters), err)]
    pub async fn query(
        &self,
        id: Uuid,
        key: P::Key,
        statement: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<<P::Connection as Connection<R>>::RowStream, Error<P, R>> {
        tracing::info!("Querying transaction");

        let transaction_key = transaction::Key::new(key, id);

        let rows = self
            .pool
            .get_connection(transaction_key)
            .await?
            .query(statement, parameters)
            .await
            .map_err(transaction::Error::Connection)?;

        Ok(rows)
    }

    /// Commit an active Postgres transaction by ID and connection pool key
    #[tracing::instrument(skip(self), err)]
    pub async fn commit(&self, id: Uuid, key: P::Key) -> Result<(), Error<P, R>> {
        tracing::debug!("Committing transaction");

        self.pool.commit(id, key).await?;

        Ok(())
    }

    /// Roll back an active Postgres transaction by ID and connection pool key
    #[tracing::instrument(skip(self), err)]
    pub async fn rollback(&self, id: Uuid, key: P::Key) -> Result<(), Error<P, R>> {
        tracing::debug!("Rolling back transaction");

        self.pool.rollback(id, key).await?;

        Ok(())
    }
}

/// gRPC service implementation for Transaction service
#[tonic::async_trait]
impl<P, R> GrpcService for Transaction<P, R>
where
    P: Pool<R> + 'static,
    P::Key: FromRequest + Hash + Eq + Clone,
    <<P::Connection as Connection<R>>::RowStream as TryStream>::Ok: Into<pbjson_types::Struct>,
    R: Send + Sync + 'static,
{
    type QueryStream = ReceiverStream<Result<pbjson_types::Struct, Status>>;

    #[tracing::instrument(skip(self, request), err)]
    async fn query(
        &self,
        mut request: Request<TransactionQueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        // derive a key from extensions to use as a connection pool key
        let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;

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
        let values = values.into_iter().map(Parameter::from).collect::<Vec<_>>();
        let mut parameters: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(values.len());

        for parameter in values.iter() {
            parameters.push(parameter);
        }

        // get the rows, converting output to proto-compatible structs and statuses
        let rows = Transaction::query(self, id, key, &statement, &parameters)
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

    #[tracing::instrument(skip(self, request), err)]
    async fn begin(
        &self,
        mut request: Request<pbjson_types::Empty>,
    ) -> Result<Response<BeginResponse>, Status> {
        // derive a key from extensions to use as a connection pool key
        let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;
        let id = Transaction::begin(self, key).await?.to_string();

        Ok(Response::new(BeginResponse { id }))
    }

    #[tracing::instrument(skip(self, request), err)]
    async fn commit(
        &self,
        mut request: Request<CommitRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        // derive a key from extensions to use as a connection pool key
        let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;

        let CommitRequest { id } = request.get_ref();

        let id = Uuid::parse_str(id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        Transaction::commit(self, id, key).await?;

        Ok(Response::new(pbjson_types::Empty::default()))
    }

    #[tracing::instrument(skip(self, request), err)]
    async fn rollback(
        &self,
        mut request: Request<RollbackRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        // derive a key from extensions to use as a connection pool key
        let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;

        let RollbackRequest { id } = request.get_ref();

        let id = Uuid::parse_str(id).map_err(|_| {
            Status::invalid_argument("Transaction ID in request had unrecognized format")
        })?;

        Transaction::rollback(self, id, key).await?;

        Ok(Response::new(pbjson_types::Empty::default()))
    }
}

/// Create a new Transaction service from a connection pool
pub fn new<P, R>(pool: Arc<P>) -> TransactionServer<Transaction<P, R>>
where
    P: Pool<R> + 'static,
    P::Key: FromRequest + Hash + Eq + Clone,
    <<P::Connection as Connection<R>>::RowStream as TryStream>::Ok: Into<pbjson_types::Struct>,
    R: Send + Sync + 'static,
{
    TransactionServer::new(Transaction::new(pool))
}

/// Create a new Postgres service from a connection pool and an interceptor
pub fn with_interceptor<P, I, R>(
    pool: Arc<P>,
    interceptor: I,
) -> InterceptedService<TransactionServer<Transaction<P, R>>, I>
where
    P: Pool<R> + 'static,
    P::Key: FromRequest + Hash + Eq + Clone,
    <<P::Connection as Connection<R>>::RowStream as TryStream>::Ok: Into<pbjson_types::Struct>,
    I: Interceptor,
    R: Send + Sync + 'static,
{
    TransactionServer::with_interceptor(Transaction::new(pool), interceptor)
}
