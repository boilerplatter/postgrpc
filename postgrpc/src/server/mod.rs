use configuration::Configuration;
#[cfg(feature = "deadpool")]
use postgrpc::pools::deadpool;
#[cfg(not(feature = "deadpool"))]
use postgrpc::pools::shared;
use postgrpc::services;
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Server;

mod configuration;
mod logging;

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("routes");

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error reading configuration from environment: {0}")]
    Environment(#[from] envy::Error),
    #[error("Tracing error: {0}")]
    Logging(#[from] tracing::subscriber::SetGlobalDefaultError),
    #[cfg(feature = "deadpool")]
    #[error(transparent)]
    Pool(#[from] deadpool::Error),
    #[cfg(not(feature = "deadpool"))]
    #[error(transparent)]
    Pool(#[from] shared::Error),
    #[cfg(feature = "deadpool")]
    #[error(transparent)]
    Config(#[from] deadpool::ConfigurationError),
    #[cfg(not(feature = "deadpool"))]
    #[error(transparent)]
    Config(#[from] shared::ConfigurationError),
    #[error("Error configuring gRPC reflection: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("Error setting up SIGTERM handler: {0}")]
    SigTerm(#[from] std::io::Error),
    #[error("Error in gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Run the app in a Result-contained function
pub(crate) async fn run() -> Result<(), Error> {
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
    #[cfg(feature = "deadpool")]
    let (pool, interceptor) = {
        let configuration: deadpool::Configuration = envy::from_env()?;
        let pool = deadpool::Pool::try_from(configuration)?;
        (pool, deadpool::interceptor)
    };

    let pool = Arc::new(pool);

    // set up the gRPC reflection service
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?;

    // set up the server with configured services
    let postgres_service = services::postgres::with_interceptor(Arc::clone(&pool), interceptor);

    tracing::info!(%address, "PostgRPC service starting");

    #[allow(unused_mut)]
    let mut server = Server::builder()
        .layer(logging::create())
        .add_service(reflection)
        .add_service(postgres_service);

    #[cfg(feature = "health")]
    #[allow(clippy::unnecessary_operation)]
    {
        server = server.add_service(services::health::new(Arc::clone(&pool)))
    };

    #[cfg(feature = "transaction")]
    #[allow(clippy::unnecessary_operation)]
    {
        server = server.add_service(services::transaction::with_interceptor(pool, interceptor));
    }

    server.serve_with_shutdown(address, shutdown).await?;

    tracing::info!(%address, "PostgRPC service stopped");

    Ok(())
}
