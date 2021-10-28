#![allow(clippy::mutable_key_type)]
use crate::{
    authentication::ClusterConfiguration,
    configuration::Configuration,
    connections::{self, Connection, Connections},
    credentials::{self, Credentials, CredentialsBuilder},
    pool::{self, session, Pool},
    protocol::{backend, frontend, startup},
};
use futures_util::{FutureExt, SinkExt, StreamExt, TryStreamExt};
use std::{net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::net::TcpListener;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error(transparent)]
    Credentials(#[from] credentials::Error),
    #[error(transparent)]
    Pool(#[from] pool::Error),
    #[error(transparent)]
    Session(#[from] session::Error),
    #[error("Error syncing proxied connection")]
    Sync,
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Connection attempt failed authorization step")]
    Unauthorized,
    #[error("Connection closed unexpectedly")]
    UnexpectedEof,
    #[error("Exiting connection")]
    Exit, // more of a placeholder than an error
}

/// The proxy service that handles TCP requests and connections
pub struct Proxy {
    address: SocketAddr,
}

impl Proxy {
    #[tracing::instrument(skip(self))]
    pub async fn serve(self) -> Result<(), Error> {
        tracing::info!("Listening on {address}", address = &self.address);
        let pool = Arc::new(Pool::default());

        TcpListener::bind(&self.address)
            .await
            .map(Connections::from)
            .map_err(|source| Error::Listen {
                address: self.address,
                source,
            })?
            .for_each_concurrent(None, |connection| {
                let pool = Arc::clone(&pool);

                async move {
                    let mut connection = connection?;

                    // handle the startup phase
                    let mut credential_builder = Credentials::build();
                    handle_startup(&mut connection, &mut credential_builder).await?;

                    // handle the password verification and client-side auth phase
                    let mut connection = Connection::<frontend::Codec>::from(connection);

                    let cluster_configuration =
                        handle_password(&mut connection, credential_builder).await?;

                    // handle subsequent messages through a user's connection pool after startup
                    handle_proxied_messages(connection, cluster_configuration, pool).await
                }
                .then(|result| async move {
                    if let Err(error) = result {
                        match error {
                            Error::Unauthorized => {
                                tracing::warn!(error = %error, "Connection rejected")
                            }
                            Error::UnexpectedEof => {
                                tracing::warn!(error = %error, "Connection closed by client")
                            }
                            Error::Exit => {
                                tracing::info!("Connection terminated by user")
                            }
                            error => {
                                tracing::error!(error = %error, "Closing Connection with error")
                            }
                        }
                    }
                })
            })
            .await;

        Ok(())
    }
}

impl From<&Configuration> for Proxy {
    fn from(configuration: &Configuration) -> Self {
        let address = SocketAddr::new(configuration.host, configuration.port);

        Self { address }
    }
}

/// Handle the startup message lifecycle
#[tracing::instrument(skip(credentials))]
async fn handle_startup(
    connection: &mut Connection<startup::Codec>,
    credentials: &mut CredentialsBuilder,
) -> Result<(), Error> {
    while let Some(message) = connection.try_next().await? {
        match message {
            startup::Message::SslRequest { .. } => {
                tracing::debug!(connection = %connection, "SSLRequest heard during startup");

                connection.send(backend::Message::SslResponse).await?;
            }
            startup::Message::Startup {
                version,
                user,
                mut options,
            } => {
                tracing::debug!(
                    version = %version,
                    user = ?user,
                    options = ?options,
                    "Startup initiated"
                );

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

/// Handle SASL auth messages
#[tracing::instrument(skip(credentials))]
async fn handle_password(
    connection: &mut Connection<frontend::Codec>,
    mut credentials: CredentialsBuilder,
) -> Result<ClusterConfiguration, Error> {
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
                    // reject the connection
                    // FIXME: figure out how to do this without getting a Connection refused error
                    // in psql (see: pgbouncer and postgres itself). Is there another frame that we
                    // should be sending?
                    connection
                        .send(backend::Message::ErrorResponse {
                            severity: backend::Severity::Fatal,
                            code: "28P01".into(),
                            message: format!(
                                r#"password authentication failed for user "{}""#,
                                String::from_utf8_lossy(&credentials.user),
                            )
                            .into(),
                        })
                        .await?;

                    return Err(Error::Unauthorized);
                }
            }

            // return the pre-query startup messages
            connection.send(backend::Message::AuthenticationOk).await?;

            Ok(ClusterConfiguration::default()) // FIXME: get from auth step instead
        }
        _ => Err(Error::Unauthorized),
    }
}

/// Handle all future messages that need to be forwarded to upstream connections
#[tracing::instrument(skip(leaders, followers, statement_guard, pool))]
async fn handle_proxied_messages(
    connection: Connection<frontend::Codec>,
    ClusterConfiguration {
        leaders,
        followers,
        statement_guard,
    }: ClusterConfiguration,
    pool: Arc<Pool>,
) -> Result<(), Error> {
    // get the remote address for logging
    let remote_peer = connection.peer();

    // get a session and message stream for this cluster configuration
    let (session, mut session_messages) = pool.get_session(leaders, followers).await?;

    // send messages from the downstream session back upstream to users
    let (transmitter, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn({
        let transmitter = transmitter.clone();

        async move {
            while let Some(message) = session_messages.recv().await {
                transmitter.send(message).map_err(|_| Error::Sync)?;
            }

            Ok::<_, Error>(())
        }
    });

    // aggregate messages from the pool and this proxy back to the user
    let (mut user_connection, user_messages) = connection.split();

    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            user_connection.send(message).await?;
        }

        tracing::debug!(connection = ?&remote_peer, "Closing Connection");

        Ok::<_, Error>(())
    });

    // share the statement guard
    let statement_guard = Arc::new(statement_guard);

    // handle messages from the user connection stream
    user_messages
        .map_err(Error::Connection)
        .try_for_each_concurrent(None, |message| {
            let session = session.clone();
            let transmitter = transmitter.clone();
            let statement_guard = Arc::clone(&statement_guard);

            async move {
                match message {
                    // FIXME: supported extended query protocol
                    frontend::Message::Query(body) => {
                        tracing::debug!(body = ?body, "Query message received");

                        let query = body.query();
                        let statement = String::from_utf8_lossy(&query);

                        match statement_guard.guard(&statement) {
                            Err(error) => {
                                tracing::warn!(reason = %error, "Statement rejected");

                                // send back an error response
                                transmitter
                                    .send(backend::Message::ErrorResponse {
                                        code: "42501".into(),
                                        message: error.to_string().into(),
                                        severity: backend::Severity::Error,
                                    })
                                    .map_err(|_| Error::Sync)?;

                                // return the ReadyForQuery message
                                transmitter
                                    .send(backend::Message::ReadyForQuery {
                                        transaction_status: backend::TransactionStatus::Idle,
                                    })
                                    .map_err(|_| Error::Sync)?;
                            }
                            Ok(..) => {
                                // FIXME: use leader or follower based on query flavor
                                // reconstruct the original query message and forward it
                                session
                                    .leader()
                                    .send(frontend::Message::Query(body))
                                    .await?;
                            }
                        }
                    }
                    frontend::Message::Forward(frame) => {
                        // pipe this frame directly to a proxied connection
                        session
                            .leader()
                            .send(frontend::Message::Forward(frame))
                            .await?;
                    }
                    frontend::Message::Terminate => return Err(Error::Exit),
                    frontend::Message::PasswordMessage(..)
                    | frontend::Message::SASLInitialResponse(..)
                    | frontend::Message::SASLResponse(..) => {
                        tracing::warn!("Authentication message received outside of a handshake");
                        return Err(Error::Exit);
                    }
                }

                Ok(())
            }
        })
        .await?;

    Ok(())
}
