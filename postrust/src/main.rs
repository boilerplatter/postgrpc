#![deny(unused_crate_dependencies)]
use configuration::Configuration;
use futures_core::Future;
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use session::Session;
use std::net::SocketAddr;
use tcp::Connections;
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};

mod cluster;
mod configuration;
mod connection;
mod credentials;
mod endpoint;
mod flush;
mod pool;
mod protocol;
mod router;
mod session;
mod tcp;
mod transaction;

#[derive(Debug, Error)]
enum Error {
    #[error("Error reading configuration from environment: {0}")]
    Configuration(#[from] envy::Error),
    #[error(transparent)]
    Session(#[from] session::Error),
    #[error("Error setting up SIGTERM handler: {0}")]
    SigTerm(#[from] std::io::Error),
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
}

/// TODO:
/// 1. Make sure that queries are properly queued (and isolated) within a session
/// 2. Handle unnamed prepared statements and portals
/// 3. Handle deallocation of prepared statements on a Connection
/// 4. Handle explicit transactions (i.e. BEGIN + END)
/// 5. AST-aware plugins (powered by libpg_query): map, filter, route -> perhaps in helix-inspired
///    WASI setup?
/// 6. implement basic cache layer (in conjunction with AST plugin system, perhaps?)
/// 7. test all of the above
/// 8. benchmark all of the above

/// Run each session with a shutdown signal
async fn handle_session<S>(session: Result<Session, session::Error>, shutdown: S)
where
    S: Future<Output = ()>,
{
    match session {
        Ok(session) => {
            if let Err(error) = session.serve(shutdown).await {
                tracing::error!(error = ?error, "Closing Session with error");
            }
        }
        Err(error) => {
            tracing::warn!(error = ?error, "Failed to start session");
        }
    }
}

/// Run the proxy in a Result-contained function
async fn run_service() -> Result<(), Error> {
    // configure logging
    tracing_subscriber::fmt::init();

    // handle SIGTERM-based termination gracefully
    let mut termination = signal(SignalKind::terminate())?;

    let shutdown = async move {
        termination.recv().await;

        tracing::info!("SIGTERM heard in Postrust proxy. Shutting down gracefully...");
    }
    .shared();

    // generate a configuration from the environment
    let configuration = Configuration::from_env()?;
    let address = SocketAddr::new(configuration.host, configuration.port);

    tracing::info!("Listening on {address}", address = &address);

    // proxy new connections to user sessions
    Connections::new(address)
        .await?
        .map_err(session::Error::Tcp)
        .and_then(Session::new)
        .take_until(shutdown.clone())
        .for_each_concurrent(None, |session| handle_session(session, shutdown.clone()))
        .await;

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run_service().await {
        tracing::error!(error = ?&error, "Postrust proxy error, stopping process");
        std::process::exit(1);
    }
}
