#[cfg(feature = "json-transcoding")]
use futures_util::{future::Either, TryFutureExt};
use health::Health;
#[cfg(feature = "json-transcoding")]
use hyper::service::{make_service_fn, Service};
use postgres_role_json_pool::{
    configuration::{self, Configuration},
    Pool,
};
use postgres_services::postgres::Postgres;
#[cfg(feature = "transaction")]
use postgres_services::transaction::Transaction;
#[cfg(feature = "transaction")]
use proto::transaction::transaction_server::TransactionServer;
use proto::{health::health_server::HealthServer, postgres::postgres_server::PostgresServer};
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Status};

mod extensions;
mod health;
mod logging;
mod postgres;
mod protocol;
#[cfg(feature = "transaction")]
mod transaction;
mod proto {
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("routes");

    pub mod postgres {
        tonic::include_proto!("postgres");
    }

    #[cfg(feature = "transaction")]
    pub mod transaction {
        tonic::include_proto!("transaction");
    }

    pub mod health {
        tonic::include_proto!("health");
    }
}

#[derive(Error, Debug)]
enum Error {
    #[error(transparent)]
    Configuration(#[from] configuration::Error),
    #[error("Error reading configuration from environment: {0}")]
    Environment(#[from] envy::Error),
    #[error("Tracing error: {0}")]
    Logging(#[from] tracing::subscriber::SetGlobalDefaultError),
    #[error(transparent)]
    Pool(#[from] postgres_role_json_pool::Error),
    #[error("Error configuring gRPC reflection: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("Error setting up SIGTERM handler: {0}")]
    SigTerm(#[from] std::io::Error),
    #[error("Error in gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[cfg(feature = "json-transcoding")]
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
}

/// map default pool errors to proper gRPC statuses
fn error_to_status(error: postgres_role_json_pool::Error) -> Status {
    let message = error.to_string();

    match error {
        postgres_role_json_pool::Error::Params { .. }
        | postgres_role_json_pool::Error::Role(..)
        | postgres_role_json_pool::Error::Query(..) => Status::invalid_argument(message),
        postgres_role_json_pool::Error::Pool(..) => Status::resource_exhausted(message),
        postgres_role_json_pool::Error::InvalidJson => Status::internal(message),
    }
}

/// Run the app in a Result-contained function
async fn run_service() -> Result<(), Error> {
    // configure logging
    tracing_subscriber::fmt::init();

    // handle SIGTERM-based termination gracefully
    let configuration: Configuration = envy::from_env()?;
    let grace_period = configuration.termination_period;
    let mut termination = signal(SignalKind::terminate())?;

    let shutdown = async move {
        termination.recv().await;
        tracing::info!("SIGTERM heard in PostgRPC service");
        tracing::info!(
            grace_period_seconds = grace_period.as_secs(),
            "Waiting for graceful termination period before shutdown"
        );
        tokio::time::sleep(grace_period).await;
        tracing::info!("Shutting down PostgRPC service");
    };

    // parse the service address from configuration
    let address = SocketAddr::from(&configuration);

    // build a shared connection pool from configuration
    let pool = Pool::try_from(configuration).map(Arc::new)?;

    // set up the gRPC reflection service
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;

    // configure individual services
    #[cfg(feature = "json-transcoding")]
    let http_service = postgres_services::http::service(Arc::clone(&pool));

    let postgres =
        PostgresServer::with_interceptor(Postgres::new(Arc::clone(&pool)), extensions::role);

    #[cfg(feature = "transaction")]
    let transaction = Some(TransactionServer::with_interceptor(
        Transaction::new(Arc::clone(&pool)),
        extensions::role,
    ));

    #[cfg(not(feature = "transaction"))]
    let transaction = None;

    // start the server with configured services
    tracing::info!(address = %&address, "PostgRPC service starting");

    let grpc_server = Server::builder()
        .layer(logging::create())
        .add_service(reflection)
        .add_service(HealthServer::new(Health::new(pool)))
        .add_service(postgres)
        .add_optional_service(transaction);

    #[cfg(not(feature = "json-transcoding"))]
    grpc_server.serve_with_shutdown(address, shutdown).await?;

    #[cfg(feature = "json-transcoding")]
    let grpc_service = grpc_server.into_service();

    #[cfg(feature = "json-transcoding")]
    hyper::Server::bind(&address)
        .serve(make_service_fn(move |_connection| {
            let grpc_service = grpc_service.clone();

            async move {
                Ok::<_, std::convert::Infallible>(tower::service_fn(
                    move |request: hyper::Request<hyper::Body>| match request
                        .headers()
                        .get(hyper::header::CONTENT_TYPE)
                        .map(|header| header.to_str())
                    {
                        Some(Ok("application/json")) => Either::Left(
                            http_service
                                .call(request)
                                .map_ok(|next| next.map(EitherBody::Left))
                                .map_err(Error::from),
                        ),
                        _ => Either::Right(
                            grpc_service
                                .call(request)
                                .map_ok(|next| next.map(EitherBody::Right))
                                .map_err(Error::from),
                        ),
                    },
                ))
            }
        }))
        .await
        .expect("hyper goofed");

    tracing::info!(address = %&address, "PostgRPC service stopped");

    Ok(())
}

// FIXME: put this in a module
#[cfg(feature = "json-transcoding")]
enum EitherBody<A, B> {
    Left(A),
    Right(B),
}

#[cfg(feature = "json-transcoding")]
impl<A, B> http_body::Body for EitherBody<A, B>
where
    A: http_body::Body + Send + Unpin,
    B: http_body::Body<Data = A::Data> + Send + Unpin,
    A::Error: Into<Error>,
    B::Error: Into<Error>,
{
    type Data = A::Data;
    type Error = Error;

    fn is_end_stream(&self) -> bool {
        match self {
            EitherBody::Left(b) => b.is_end_stream(),
            EitherBody::Right(b) => b.is_end_stream(),
        }
    }

    fn poll_data(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Self::Data, Self::Error>>> {
        match self.get_mut() {
            EitherBody::Left(b) => std::pin::Pin::new(b)
                .poll_data(cx)
                .map(|next| next.map(|result| result.map_err(Into::into))),
            EitherBody::Right(b) => std::pin::Pin::new(b)
                .poll_data(cx)
                .map(|next| next.map(|result| result.map_err(Into::into))),
        }
    }

    fn poll_trailers(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        match self.get_mut() {
            EitherBody::Left(b) => std::pin::Pin::new(b).poll_trailers(cx).map_err(Into::into),
            EitherBody::Right(b) => std::pin::Pin::new(b).poll_trailers(cx).map_err(Into::into),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_service().await {
        tracing::error!(error = ?&error, "PostgRPC service error! Process stopped");
    }
}
