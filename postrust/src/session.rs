use crate::{
    cluster::{self, Cluster, CLUSTERS},
    connection,
    credentials::{self, Credentials, CredentialsBuilder},
    endpoint::Endpoint,
    protocol::{
        backend,
        errors::{
            CONNECTION_EXCEPTION, FEATURE_NOT_SUPPORTED, INVALID_PASSWORD, PROTOCOL_VIOLATION,
        },
        frontend, startup,
    },
    router::{self, Router},
    tcp,
};
use futures_core::Future;
use futures_util::{future, stream::SplitStream, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use postguard::Guard;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Cluster(#[from] cluster::Error),
    #[error(transparent)]
    Connection(#[from] connection::Error),
    #[error(transparent)]
    Credentials(#[from] credentials::Error),
    #[error(transparent)]
    Router(#[from] router::Error),
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Message {0:?} violates Postgres message protocol")]
    Protocol(frontend::Message),
    #[error(r#"password authentication failed for user "{0}""#)]
    Unauthorized(String),
    #[error("Error flushing startup messages on session init")]
    Flush,
    #[error("Message type not yet supported")]
    Unimplemented,
    #[error("Connection closed unexpectedly")]
    UnexpectedEof,
}

impl From<&Error> for backend::Message {
    fn from(error: &Error) -> Self {
        match error {
            Error::Cluster(error) => Self::from(error),
            Error::Connection(error) => Self::from(error),
            Error::Credentials(error) => Self::from(error),
            Error::Router(error) => Self::from(error),
            Error::Tcp(error) => Self::from(error),
            Error::Protocol(..) => Self::ErrorResponse {
                code: PROTOCOL_VIOLATION.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Fatal,
            },
            Error::Unauthorized(..) => Self::ErrorResponse {
                code: INVALID_PASSWORD.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Fatal,
            },
            Error::Unimplemented => Self::ErrorResponse {
                code: FEATURE_NOT_SUPPORTED.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Fatal,
            },
            Error::Flush | Error::UnexpectedEof => Self::ErrorResponse {
                code: CONNECTION_EXCEPTION.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Fatal,
            },
        }
    }
}

/// User session wrapper that brokers messages between user connections and clusters
pub struct Session {
    frontend_messages: SplitStream<tcp::Connection<frontend::Codec>>,
    router: Router,
}

impl Session {
    /// Create a new session
    #[tracing::instrument]
    pub async fn new(mut connection: tcp::Connection<startup::Codec>) -> Result<Self, Error> {
        tracing::debug!("Retrieving Session for Connection");

        // get the remote address for logging
        let remote_peer = connection.peer();

        // handle the startup phase of the TCP connection
        let mut credentials = Credentials::build();

        if let Err(error) = handle_startup(&mut credentials, &mut connection).await {
            let mut connection = tcp::Connection::<frontend::Codec>::from(connection);
            return Err(handle_error(&mut connection, error).await?);
        };

        // handle the password verification and client-side auth phase
        let mut connection = tcp::Connection::<frontend::Codec>::from(connection);

        let cluster::Configuration {
            leaders,
            followers,
            statement_guard,
        } = match handle_cluster_configuration(credentials, &mut connection).await {
            Ok(configuration) => configuration,
            Err(error) => return Err(handle_error(&mut connection, error).await?),
        };

        // get or init the cluster for the session
        let cluster = match handle_cluster_generation(leaders, followers).await {
            Ok(cluster) => cluster,
            Err(error) => return Err(handle_error(&mut connection, error).await?),
        };

        // generate a router from the cluster
        let (router, mut receiver) = match handle_router_generation(cluster, statement_guard).await
        {
            Ok(router_pair) => router_pair,
            Err(error) => return Err(handle_error(&mut connection, error).await?),
        };

        // send messages from the session back upstream to the connection
        let (mut user_backend_messages, frontend_messages) = connection.split();

        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                if let Err(error) = user_backend_messages.send(message).await {
                    return tracing::error!(%error, "User message Sink error");
                }
            }

            tracing::debug!(connection = ?&remote_peer, "Closing Session Connection");
        });

        tracing::info!("Session initiated");

        Ok(Self {
            router,
            frontend_messages,
        })
    }

    /// Broker messages between a Connection and Cluster until a shutdown signal or Session end
    #[tracing::instrument(skip(self, shutdown))]
    pub async fn serve<F>(self, shutdown: F) -> Result<(), Error>
    where
        F: Future<Output = ()>,
    {
        self.frontend_messages
            .map_err(Error::Tcp)
            .take_until(shutdown)
            .try_take_while(|message| match message {
                frontend::Message::Terminate => {
                    tracing::info!("Session terminated by user");
                    future::ok(false)
                }
                frontend::Message::PasswordMessage(..)
                | frontend::Message::SASLInitialResponse(..)
                | frontend::Message::SASLResponse(..) => {
                    tracing::warn!("Authentication message received outside of a handshake");
                    future::err(Error::Protocol(message.clone()))
                }
                frontend::Message::Forward(..) => {
                    tracing::error!("Unsupported message type found");
                    future::err(Error::Unimplemented)
                }
                _ => future::ok(true),
            })
            .try_for_each_concurrent(None, |message| {
                self.router.route(message).map_err(Error::Router)
            })
            .await
    }
}

/// Convert a startup-flavored TCP Connection into a frontend-flavored TCP Connection
async fn handle_startup(
    credentials: &mut CredentialsBuilder,
    connection: &mut tcp::Connection<startup::Codec>,
) -> Result<(), Error> {
    while let Some(message) = connection.try_next().await? {
        match message {
            startup::Message::SslRequest { .. } => {
                tracing::debug!(%connection, "SSLRequest heard during startup");

                connection.send(backend::Message::SslResponse).await?
            }
            startup::Message::Startup {
                version,
                user,
                mut options,
            } => {
                tracing::debug!(%version, ?user, ?options, "Startup initiated");

                // send the AuthenticationCleartextPassword message
                // FIXME: use SASL by default instead
                connection
                    .send(backend::Message::AuthenenticationCleartextPassword)
                    .await?;

                // store the previous credentials
                credentials.user(user);

                if let Some(database) = options.remove("database".as_bytes()) {
                    credentials.database(database);
                }

                break;
            }
        }
    }

    Ok(())
}

/// Generate a cluster configuration from a frontend-flavored TCP Connection
async fn handle_cluster_configuration(
    mut credentials: CredentialsBuilder,
    connection: &mut tcp::Connection<frontend::Codec>,
) -> Result<cluster::Configuration, Error> {
    match connection.try_next().await?.ok_or(Error::UnexpectedEof)? {
        frontend::Message::PasswordMessage(message) => {
            let password = message.cleartext_password();

            // validate the credentials with the given password
            credentials.password(password);
            let credentials = credentials.finish()?;

            // get the cluster configuration from the auth query
            // FIXME: run externally/validate
            match &credentials {
                Credentials {
                    user,
                    database,
                    password,
                } if user == "test" && database == "testdb" && password == "hunter2" => {}
                credentials => {
                    return Err(Error::Unauthorized(
                        String::from_utf8_lossy(&credentials.user).into(),
                    ));
                }
            }

            // return the pre-query startup messages
            connection.send(backend::Message::AuthenticationOk).await?;

            Ok(cluster::Configuration::default()) // FIXME: get from auth step instead
        }
        message => Err(Error::Protocol(message)),
    }
}

/// Handle generation of a Cluster from a set of leaders and followers
async fn handle_cluster_generation(
    leaders: Vec<Endpoint>,
    followers: Vec<Endpoint>,
) -> Result<Arc<Cluster>, Error> {
    let key = (leaders, followers);
    let clusters = CLUSTERS.read().await;
    let cluster = match clusters.get(&key) {
        Some(cluster) => Arc::clone(cluster),
        None => {
            drop(clusters);

            let cluster = Cluster::connect(key.0.clone(), key.1.clone())
                .await
                .map(Arc::new)?;

            let session_cluster = Arc::clone(&cluster);

            CLUSTERS.write().await.insert(key, cluster);

            session_cluster
        }
    };

    Ok(cluster)
}

/// Handle generation of a Router from a cluster
async fn handle_router_generation(
    cluster: Arc<Cluster>,
    statement_guard: Guard,
) -> Result<(Router, UnboundedReceiver<backend::Message>), Error> {
    // flush the cluster's startup messages on session init
    let (backend_messages, receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut leader = cluster.leader().await?;
    let startup_messages = leader.startup_messages().await?;

    for message in startup_messages {
        backend_messages.send(message).map_err(|_| Error::Flush)?;
    }

    // build a Router from the cluster
    let router = Router::new(cluster, backend_messages, Arc::new(statement_guard));

    Ok((router, receiver))
}

/// Forward errors to a TCP Connection as proper Postgres-style backend Messages
async fn handle_error(
    connection: &mut tcp::Connection<frontend::Codec>,
    error: Error,
) -> Result<Error, Error> {
    let message = backend::Message::from(&error);
    connection.send(message).await?;
    Ok(error)
}
