use crate::{
    connections::{self, Connection, Connections},
    credentials::{self, Credentials, CredentialsBuilder},
    protocol::{backend, frontend, startup},
};
use futures_util::{FutureExt, SinkExt, StreamExt, TryStreamExt};
use postguard::{AllowedFunctions, AllowedStatements, Command, Guard};
use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use thiserror::Error;
use tokio::net::TcpListener;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error(transparent)]
    Credentials(#[from] credentials::Error),
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
}

/// Proxy configuration
#[derive(Debug, Deserialize)]
pub struct Configuration {
    /// proxy host IP address
    #[serde(default = "get_v4_localhost")]
    pub host: IpAddr,
    /// proxy port
    #[serde(default = "get_proxy_port")]
    pub port: u16,
}

/// Generate a default Ipv4 pointing to localhost for configuration
fn get_v4_localhost() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
}

/// Generate a default proxy port for configuration
fn get_proxy_port() -> u16 {
    6432
}

/// The proxy service that handles TCP requests and connections
// TODO:
// initialize connection meta-pools (sets of read/write connection pools for every resolved auth response)
// set up a cleanup process for expired pools
// send auth requests (if configured) to gRPC or HTTP service
// create pools for each auth response (and create proto for that auth request/response)
// if there are leaders and followers, test AST with postguard for read versus writes
// use a connection from the proper pool to send the request along, returning once finished
//
// TODO low-priority:
// add transaction-level pooling
// add session-level pooling
pub struct Proxy {
    configuration: Configuration,
}
impl Proxy {
    pub fn new(configuration: Configuration) -> Self {
        Self { configuration }
    }

    #[tracing::instrument(skip(self))]
    pub async fn serve(self) -> Result<(), Error> {
        let address = SocketAddr::new(self.configuration.host, self.configuration.port);

        tracing::info!("Listening on {address}", address = &address);

        TcpListener::bind(&address)
            .await
            .map(Connections::from)
            .map_err(|source| Error::Listen { address, source })?
            .for_each_concurrent(None, |connection| {
                async move {
                    let mut connection = connection?;

                    // handle the startup phase
                    let password_salt = [b'a', b'b', 3, b'z']; // FIXME: generate anew for each conn
                    let mut credential_builder = Credentials::build();
                    handle_startup(&mut connection, &mut credential_builder, password_salt).await?;

                    // handle the password verification and auth phase
                    let mut connection = Connection::<frontend::Codec>::from(connection);
                    handle_password(&mut connection, &mut credential_builder, password_salt)
                        .await?;

                    // handle subsequent messages with a statement guard after startup has been completed
                    handle_proxied_messages(
                        &mut connection,
                        credential_builder.finish()?,
                        Guard::new(
                            AllowedStatements::List(vec![Command::Select]),
                            AllowedFunctions::List(vec![]),
                        ),
                    )
                    .await
                }
                .then(|result| async move {
                    if let Err(error) = result {
                        tracing::error!(error = %error, "Closing Connection with error");
                    }
                })
            })
            .await;

        Ok(())
    }
}

/// Handle the startup message lifecycle
#[tracing::instrument(skip(credentials))]
async fn handle_startup(
    connection: &mut Connection<startup::Codec>,
    credentials: &mut CredentialsBuilder,
    password_salt: [u8; 4],
) -> Result<(), Error> {
    while let Some(message) = connection.try_next().await? {
        match message {
            startup::Message::SslRequest => {
                tracing::debug!(connection = %connection, "SSLRequest heard during handshake");

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

                // send the AuthenticationMd5Password message
                connection
                    .send(backend::Message::AuthenticationMd5Password {
                        salt: password_salt,
                    })
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

/// Handle password auth messages
#[tracing::instrument(skip(credentials))]
async fn handle_password(
    connection: &mut Connection<frontend::Codec>,
    credentials: &mut CredentialsBuilder,
    password_salt: [u8; 4],
) -> Result<(), Error> {
    if let Some(frontend::Message::PasswordMessage(message)) = connection.try_next().await? {
        let password = message.password(password_salt);

        // TODO:
        // run the auth query/request/what-have-you

        // return the AuthenticationOk message
        connection.send(backend::Message::AuthenticationOk).await?;

        // TODO:
        // send some backend parameters back to the client

        // return the ReadyForQuery message
        connection
            .send(backend::Message::ReadyForQuery {
                transaction_status: backend::TransactionStatus::Idle,
            })
            .await?;

        // add the password to the credentials
        credentials.password(password);
    }

    Ok(())
}

/// Handle all future messages that need to be forwarded to upstream connections
#[tracing::instrument(skip(_credentials))]
async fn handle_proxied_messages(
    connection: &mut Connection<frontend::Codec>,
    _credentials: Credentials,
    statement_guard: postguard::Guard,
) -> Result<(), Error> {
    while let Some(message) = connection.try_next().await? {
        match message {
            frontend::Message::Query(body) => {
                tracing::info!(body = ?body, "Query message received");

                // FIXME: let guard take bytes, too!
                let statement = unsafe { String::from_utf8_unchecked(body.query().to_vec()) };

                if let Err(error) = statement_guard.guard(&statement) {
                    tracing::warn!(reason = %error, "Statement rejected");

                    // send back an error response
                    connection
                        .send(backend::Message::ErrorResponse {
                            code: 42501,
                            message: error.to_string().into(),
                            severity: backend::Severity::Error,
                        })
                        .await?;

                    // return the ReadyForQuery message
                    connection
                        .send(backend::Message::ReadyForQuery {
                            transaction_status: backend::TransactionStatus::Idle,
                        })
                        .await?;
                }

                // TODO: otherwise, reconstruct the original message and
                // forward it upstream
            }
            frontend::Message::Forward(_frame) => {
                // TODO: pipe this frame directly to an upstream
                // postgres connection
            }
            frontend::Message::PasswordMessage(..) => {
                tracing::warn!("PasswordMessage received outside of a handshake")
            }
        }
    }

    Ok(())
}
