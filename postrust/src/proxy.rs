use crate::{
    connections::{self, Connection, Connections},
    protocol::{backend, frontend, startup},
};
use bytes::Bytes;
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
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Invalid credentials")]
    InvalidCredentials,
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
// set up a TCP listener
// parse messages from that TCP listener with postgres-protocol
// parse connection URIs from messages
// send auth requests (if configured) to gRPC or HTTP service
// create pools for each auth response (and create proto for that auth request/response)
// parse statements from messages
// guard statements with postguard lib
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
            .for_each_concurrent(None, |connection| async move {
                match connection {
                    Ok(mut connection) => {
                        // generate shared password salt 
                         let password_salt = [b'a', b'b', 3, b'z']; // FIXME: generate anew for each conn

                        // set up shared credentials for future requests
                        let mut credentials = Credentials::build();

                        // handle the handshake lifecycle
                        while let Some(message) = connection.try_next().await? {
                            match message {
                                startup::Message::SslRequest => {
                                    tracing::debug!(connection = %connection, "SSLRequest heard during handshake");

                                    connection.send(backend::Message::SslResponse).await?;
                                },
                                startup::Message::Startup { version, user, mut options } => {
                                    tracing::debug!(
                                        version = %version,
                                        user = ?user,
                                        options = ?options,
                                        "Startup initiated"
                                    );

                                    // send the AuthenticationMd5Password message
                                    connection
                                        .send(backend::Message::AuthenticationMd5Password { salt: password_salt })
                                        .await?;

                                    // store the previous credentials
                                    credentials.user(user);

                                    if let Some(database) = options.remove("database".as_bytes()) {
                                        credentials.database(database);
                                    }

                                    // break out of this part of the stream
                                    break;
                                }
                            }
                        }

                        // convert the connection to a message stream
                        let mut connection = Connection::<frontend::Codec>::from(connection);

                        // validate the provided credentials with the next message
                        if let Some(frontend::Message::PasswordMessage(message)) = connection.try_next().await? {
                            let password = message.password(password_salt);

                            // TODO:
                            // run the auth query/request/what-have-you

                            // return the AuthenticationOk message
                            connection.send(backend::Message::AuthenticationOk).await?;

                            // TODO:
                            // send some backend parameters back to the client

                            // return the ReadyForQuery message
                            connection.send(backend::Message::ReadyForQuery {
                                transaction_status: backend::TransactionStatus::Idle
                            }).await?;

                            // add the password to the credentials
                            credentials.password(password);
                        }

                        let credentials = credentials.build()?;

                        tracing::debug!(credentials = ?&credentials, "Connection authenticated");

                        // set up guards from credentials based on auth response
                        let statement_guard = Guard::new(
                            AllowedStatements::List(vec![Command::Select]),
                            AllowedFunctions::List(vec![]),
                        );

                        // handle subsequent messages after the handshake has been completed
                        while let Some(message) = connection.try_next().await? {
                            match message {
                                frontend::Message::Query(body) => {
                                    tracing::info!(body = ?body, "Query message received");

                                    // FIXME: let guard take bytes, too!
                                    let statement =
                                        unsafe { String::from_utf8_unchecked(body.query().to_vec()) };

                                    if let Err(error) = statement_guard.guard(&statement) {
                                        tracing::warn!(reason = %error, "Statement rejected");

                                        // send back an error response
                                        connection.send(backend::Message::ErrorResponse {
                                            code: 42501,
                                            message: error.to_string().into(),
                                            severity: backend::Severity::Error
                                        }).await?;

                                        // return the ReadyForQuery message
                                        connection.send(backend::Message::ReadyForQuery {
                                            transaction_status: backend::TransactionStatus::Idle
                                        }).await?;
                                    }

                                    // TODO: otherwise, reconstruct the original message and
                                    // forward it upstream
                                }
                                frontend::Message::Forward(_frame) => {
                                    // TODO: pipe this frame directly to an upstream
                                    // postgres connection
                                }
                                frontend::Message::PasswordMessage(..) => tracing::warn!("PasswordMessage received outside of a handshake")
                            }
                        }

                        // TODO:
                        // use the auth response to key into the right connection
                        // (from the pool, of course)
                        // guard statements in parse and query messages
                    }
                    Err(error) => tracing::error!(error = %error, "Error establishing Connection"),
                }

                Ok::<_, Error>(())
            }.then(|result| async move {
                if let Err(error) = result {
                    tracing::error!(error = %error, "Closing Connection with error");
                }
            }))
            .await;

        Ok(())
    }
}

/// Database connection credentials from the connection string
#[derive(Debug)]
struct Credentials {
    user: Bytes,
    database: Bytes,
    password: Bytes,
}

impl Credentials {
    fn build() -> CredentialsBuilder {
        CredentialsBuilder::default()
    }
}

#[derive(Default)]
struct CredentialsBuilder {
    user: Option<Bytes>,
    database: Option<Bytes>,
    password: Option<Bytes>,
}

impl CredentialsBuilder {
    fn user(&mut self, user: Bytes) {
        self.user = Some(user);
    }

    fn database(&mut self, database: Bytes) {
        self.database = Some(database);
    }

    fn password(&mut self, password: Bytes) {
        self.password = Some(password);
    }

    fn build(self) -> Result<Credentials, Error> {
        let user = self.user.ok_or(Error::InvalidCredentials)?;
        let password = self.password.ok_or(Error::InvalidCredentials)?;
        let database = self.database.unwrap_or_else(|| user.to_owned());

        Ok(Credentials {
            user,
            password,
            database,
        })
    }
}
