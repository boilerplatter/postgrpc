use channel::Channel;
use configuration::Configuration;
use health::Health;
use postgres::Postgres;
use proto::{
    channel::channel_server::ChannelServer, health::health_server::HealthServer,
    postgres::postgres_server::PostgresServer, transaction::transaction_server::TransactionServer,
};
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Status};
use transaction::Transaction;

mod channel;
mod configuration;
mod health;
mod pools;
mod postgres;
mod protocol;
mod transaction;
mod proto {
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("routes");

    pub mod postgres {
        tonic::include_proto!("postgres");
    }

    pub mod transaction {
        tonic::include_proto!("transaction");
    }

    pub mod channel {
        tonic::include_proto!("channel");
    }

    pub mod health {
        tonic::include_proto!("health");
    }
}

#[derive(Error, Debug)]
enum Error {
    #[error("Error reading configuration from environment: {0}")]
    Configuration(#[from] configuration::Error),
    #[error("Tracing error: {0}")]
    Logging(#[from] tracing::subscriber::SetGlobalDefaultError),
    #[error(transparent)]
    Pool(#[from] pools::default::Error),
    #[error("Error configuring gRPC reflection: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("Error setting up SIGTERM handler: {0}")]
    SigTerm(#[from] std::io::Error),
    #[error("Error in gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
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

/// Run the app in a Result-containd function
async fn run_service() -> Result<(), Error> {
    // configure logging
    tracing_subscriber::fmt().init();

    // handle SIGTERM-based termination gracefully
    let configuration = Configuration::new()?;
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

    // configure the application service itself
    let address = SocketAddr::from(&configuration);
    let pool = pools::default::Pool::try_from(configuration).map(Arc::new)?;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;

    let server = Server::builder()
        .trace_fn(|_| tracing::info_span!("postgrpc"))
        .add_service(reflection)
        .add_service(HealthServer::new(Health))
        .add_service(PostgresServer::new(Postgres::new(Arc::clone(&pool))))
        .add_service(TransactionServer::new(Transaction::new(Arc::clone(&pool))))
        .add_service(ChannelServer::new(Channel::new(pool)))
        .serve_with_shutdown(address, shutdown);

    tracing::info!(address = %&address, "PostgRPC service starting");

    server.await?;

    tracing::info!(address = %&address, "PostgRPC service stopped");

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_service().await {
        tracing::error!(error = ?&error, "PostgRPC service error! Process stopped");
    }
}
