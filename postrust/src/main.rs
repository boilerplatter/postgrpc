use configuration::Configuration;
use proxy::Proxy;
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};

mod authentication;
mod configuration;
mod connections;
mod credentials;
mod pool;
mod protocol;
mod proxy;

#[derive(Debug, Error)]
enum Error {
    #[error("Error reading configuration from environment: {0}")]
    Configuration(#[from] envy::Error),
    #[error(transparent)]
    Proxy(#[from] proxy::Error),
    #[error("Error setting up SIGTERM handler: {0}")]
    SigTerm(#[from] std::io::Error),
}

/// Run the proxy in a Result-contained function
async fn run_service() -> Result<(), Error> {
    // configure logging
    tracing_subscriber::fmt::init();

    // generate a proxy with configuration from the environment
    let configuration = Configuration::from_env()?;
    let proxy = Proxy::from(&configuration);

    // handle SIGTERM-based termination gracefully
    let mut termination = signal(SignalKind::terminate())?;

    let shutdown = async move {
        termination.recv().await;

        // TODO: wait for active queries to complete

        tracing::info!("SIGTERM heard in Postrust proxy");
    };

    // run the proxy with SIGTERM shutdown listener
    tokio::select! {
        biased;

        response = proxy.serve() => response?,
        _ = shutdown => {
            tracing::info!("Postrust proxy shutting down")
        },
    };

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_service().await {
        tracing::error!(error = ?&error, "Postrust proxy error, stopping process");
        std::process::exit(1);
    }
}
