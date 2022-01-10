#![deny(unused_crate_dependencies)]
use configuration::Configuration;
use futures_util::{StreamExt, TryStreamExt};
use session::Session;
use std::net::SocketAddr;
use tcp::Connections;
use thiserror::Error;
use tokio::signal::unix::{signal, SignalKind};

mod authentication;
mod cluster;
mod configuration;
mod connection;
mod credentials;
mod endpoint;
mod pool;
mod protocol;
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

/// Arch Component Overview
/// 1. Configuration: handles configuration values from different sources
/// 2. Connection: a TCP connection and its codecs for frontend + backend database messages
/// 3. Connections: the stream of new TCP connections to be handled by the Proxy
/// 4. Session: message routing for a single user-facing connection and its backing cluster
/// 5. Cluster: the set of leader + follower Endpoints that a message might be routed to
/// 6. Clusters: the shared set of Clusters that Sessions can reference
/// 7. Endpoint: an upstream database configuration within a Cluster
/// 8. RoundRobinEndpoints: round-robin load-balanced iterator over sets of proxied connections for
///    an Endpoint
/// 9. ProxiedConnection: a single upstream database connection for an Endpoint
/// 10. ProxiedConnections: a ProxiedConnection pool for an Endpoint load-balanced by idleness
///
/// Arch Component Lifecycle
/// Configuration -configures-> Address -used to build-> Connections -yields-> Connection
/// -maps to-> Session -routes messages to-> ProxiedConnections
///
/// TODO:
/// 1. AST-aware plugins (powered by libpg_query): map, filter, route -> perhaps in helix-inspired
///    WASI setup?
/// 2. implement basic cache layer (in conjunction with AST plugin system, perhaps?)
/// 3. handle graceful shutdown of the proxy service/sessions for in-flight queries
///
/// FIXME:
/// 1. add some criterion-based benchmarks/flamegraphs
/// 2. tackle bottlenecks in the proxy layer (probably in tokio channels or RwLocks)

/// Run the proxy in a Result-contained function
async fn run_service() -> Result<(), Error> {
    // configure logging
    tracing_subscriber::fmt::init();

    // handle SIGTERM-based termination gracefully
    let mut termination = signal(SignalKind::terminate())?;

    let shutdown = async move {
        termination.recv().await;

        tracing::info!("SIGTERM heard in Postrust proxy");

        // TODO: wait for active queries to complete

        tracing::info!("Postrust proxy shutting down");
    };

    // generate a configuration from the environment
    let configuration = Configuration::from_env()?;
    let address = SocketAddr::new(configuration.host, configuration.port);

    tracing::info!("Listening on {address}", address = &address);

    // proxy new connections to user sessions
    Connections::new(address)
        .await?
        .map_err(session::Error::Tcp)
        .and_then(Session::new)
        .take_until(shutdown)
        .for_each_concurrent(None, |session| async move {
            match session {
                Ok(session) => {
                    if let Err(error) = session.serve().await {
                        tracing::error!(error = %error, "Closing Session with error");
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "Failed to start session");
                }
            }
        })
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
