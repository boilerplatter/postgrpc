use configuration::Configuration;
#[cfg(feature = "health")]
use health::Health;
use postgres::Postgres;
use postgrpc::pools::deadpool; // FIXME: configure pool types through the feature
#[cfg(feature = "transaction")]
use proto::transaction::transaction_server::TransactionServer;
use proto::{health::health_server::HealthServer, postgres::postgres_server::PostgresServer};
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Server;
#[cfg(feature = "transaction")]
use transaction::Transaction;

mod configuration;
mod health;
mod logging;
mod postgres;
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

    #[cfg(feature = "health")]
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
    Pool(#[from] deadpool::Error),
    #[error("Error configuring gRPC reflection: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("Error setting up SIGTERM handler: {0}")]
    SigTerm(#[from] std::io::Error),
    #[error("Error in gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
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

        if let Some(grace_period) = grace_period {
            tracing::info!(
                grace_period_seconds = grace_period.as_secs(),
                "Waiting for graceful termination period before shutdown"
            );

            tokio::time::sleep(grace_period).await;
        }

        tracing::info!("Shutting down PostgRPC service");
    };

    // parse the service address from configuration
    let address = SocketAddr::from(&configuration);

    // build a shared connection pool from configuration
    // FIXME: assign based on features
    let pool = deadpool::Pool::try_from(configuration).map(Arc::new)?;
    let interceptor = deadpool::interceptor;

    // set up the gRPC reflection service
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;

    // set up the server with configured services
    // FIXME: use tonic_health
    let health_service = if cfg!(feature = "health") {
        Some(HealthServer::new(Health::new(Arc::clone(&pool))))
    } else {
        None
    };

    let postgres_service =
        PostgresServer::with_interceptor(Postgres::new(Arc::clone(&pool)), interceptor);

    let transaction_service = if cfg!(feature = "transaction") {
        Some(TransactionServer::with_interceptor(
            Transaction::new(pool),
            interceptor,
        ))
    } else {
        None
    };

    tracing::info!(%address, "PostgRPC service starting");

    Server::builder()
        .layer(logging::create())
        .add_service(reflection)
        .add_service(postgres_service)
        .add_optional_service(health_service)
        .add_optional_service(transaction_service)
        .serve_with_shutdown(address, shutdown)
        .await?;

    tracing::info!(%address, "PostgRPC service stopped");

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_service().await {
        tracing::error!(?error, "PostgRPC service error! Process stopped");
    }
}
