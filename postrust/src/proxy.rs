#![allow(clippy::mutable_key_type)]
use crate::{
    configuration::Configuration,
    connections::{self, Connection, Connections},
    credentials::{self, Credentials, CredentialsBuilder},
    protocol::{
        backend,
        frontend::{self, SASLInitialResponseBody, SASLResponseBody},
        startup,
    },
};
use futures_util::{FutureExt, SinkExt, StreamExt, TryStreamExt};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postguard::{AllowedFunctions, AllowedStatements, Command, Guard};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error(transparent)]
    Credentials(#[from] credentials::Error),
    #[error("Error syncing proxied connection")]
    Sync,
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Cluster onfiguration for the current user is missing a leader")]
    MissingLeader,
    #[error("Error connecting to upstream database at {address}: {source}")]
    TcpConnect {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Connection attempt failed authorization step")]
    Unauthorized,
    #[error("Connection closed unexpectedly")]
    UnexpectedEof,
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
    address: SocketAddr,
}

impl Proxy {
    #[tracing::instrument(skip(self))]
    pub async fn serve(self) -> Result<(), Error> {
        tracing::info!("Listening on {address}", address = &self.address);

        TcpListener::bind(&self.address)
            .await
            .map(Connections::from)
            .map_err(|source| Error::Listen {
                address: self.address,
                source,
            })?
            .for_each_concurrent(None, |connection| {
                async move {
                    let mut connection = connection?;

                    // handle the startup phase
                    let mut credential_builder = Credentials::build();
                    handle_startup(&mut connection, &mut credential_builder).await?;

                    // handle the password verification and auth phase
                    let mut connection = Connection::<frontend::Codec>::from(connection);
                    let cluster = handle_password(&mut connection, credential_builder).await?;

                    // handle subsequent messages with a statement guard after startup has been completed
                    handle_proxied_messages(connection, cluster).await
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
) -> Result<Cluster, Error> {
    match connection.try_next().await?.ok_or(Error::UnexpectedEof)? {
        frontend::Message::PasswordMessage(message) => {
            let password = message.cleartext_password();

            // validate the credentials with the given password
            credentials.password(password);
            let credentials = credentials.finish()?;

            // get the cluster configuration from the auth query
            // FIXME: run externally/validate
            match credentials {
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

            // return the AuthenticationOk message
            connection.send(backend::Message::AuthenticationOk).await?;

            Ok(Cluster::default()) // FIXME: get from auth step instead
        }
        _ => Err(Error::Unauthorized),
    }
}

/// Handle all future messages that need to be forwarded to upstream connections
#[tracing::instrument(skip(cluster))]
async fn handle_proxied_messages(
    connection: Connection<frontend::Codec>,
    cluster: Cluster,
) -> Result<(), Error> {
    // FIXME:
    // use a lazily-initialized pool instead (a la deadpool) that implements Pool from postgres-pool
    // send back postrust-specific params (or maybe use the leader to get parameters, enforcing
    // that those parameters are true for the entire cluster)
    // load balance connections
    // split read and writes by query type
    let leader = cluster.leaders.first().ok_or(Error::MissingLeader)?;

    let upstream_address = SocketAddr::new(leader.host, leader.port);
    let mut upstream = TcpStream::connect(upstream_address)
        .await
        .map(|socket| Connection::new(socket, startup::Codec, upstream_address))
        .map_err(|source| Error::TcpConnect {
            address: upstream_address,
            source,
        })?;

    // handle the upstream auth handshake
    let mut options = BTreeMap::new();

    options.insert("database".into(), leader.database.to_string().into());
    options.insert("application_name".into(), "postrust".into());
    options.insert("client_encoding".into(), "UTF8".into());

    upstream
        .send(startup::Message::Startup {
            user: leader.user.to_string().into(),
            options,
            version: startup::VERSION,
        })
        .await?;

    let mut upstream = Connection::<backend::Codec>::from(upstream);

    match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
        backend::Message::AuthenticationSASL { mechanisms } => {
            let mechanism = mechanisms.first().cloned().ok_or(Error::Unauthorized)?; // FIXME: better error here

            // construct a ScramSha256
            let channel_binding = ChannelBinding::unrequested(); // is this right?
            let mut scram_client = ScramSha256::new(leader.password.as_bytes(), channel_binding);

            // send out a SASLInitialResponse message
            upstream
                .send(frontend::Message::SASLInitialResponse(
                    SASLInitialResponseBody {
                        mechanism,
                        initial_response: scram_client.message().to_vec().into(),
                    },
                ))
                .await?;

            // wait for the SASL continuation message from upstream
            match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
                backend::Message::AuthenticationSASLContinue { data } => {
                    scram_client
                        .update(&data)
                        .map_err(|_| Error::Unauthorized)?;
                }
                _ => return Err(Error::Unauthorized),
            }

            // send out a SASLResponse message
            upstream
                .send(frontend::Message::SASLResponse(SASLResponseBody {
                    data: scram_client.message().to_vec().into(),
                }))
                .await?;

            // wait for the final SASL message from upstream
            match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
                backend::Message::AuthenticationSASLFinal { data } => {
                    scram_client
                        .finish(&data)
                        .map_err(|_| Error::Unauthorized)?;
                }
                _ => return Err(Error::Unauthorized),
            }

            // wait for AuthenticationOk then carry on
            match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
                backend::Message::AuthenticationOk => (),
                _ => return Err(Error::Unauthorized),
            }

            tracing::debug!("SASL handshake completed");
        }
        backend::Message::AuthenticationOk => {
            // all good, carry on
        }
        _ => {
            // TODO: support other auth schemes
            // reject all other responses
            return Err(Error::Unauthorized);
        }
    }

    // pipe messages from upstream to a transmitter
    let (mut upstream, mut upstream_messages) = upstream.split();
    let (transmitter, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    let upstream_messages_complete = tokio::spawn({
        let transmitter = transmitter.clone();

        async move {
            while let Some(message) = upstream_messages.try_next().await? {
                transmitter.send(message).map_err(|_| Error::Sync)?; // FIXME: make this error better
            }

            Ok::<_, Error>(())
        }
    });

    // send messages from a channel to the user's end of the connection
    let remote_peer = connection.peer();
    let (mut user_connection, user_messages) = connection.split();

    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            user_connection.send(message).await?;
        }

        tracing::debug!(connection = ?&remote_peer, "Closing Connection");

        Ok::<_, Error>(())
    });

    // stop parsing user messages once the user stops sending messages
    let mut user_messages = user_messages.take_until(upstream_messages_complete);

    // handle messages from the user connection stream
    // FIXME: use takeUntil or select to short-circuit the connection when an error happens
    while let Some(message) = user_messages.try_next().await? {
        match message {
            frontend::Message::Query(body) => {
                tracing::info!(body = ?body, "Query message received");

                let query = body.query();
                let statement = String::from_utf8_lossy(&query);

                match cluster.statement_guard.guard(&statement) {
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
                        // reconstruct the original query message and forward it
                        upstream.send(frontend::Message::Query(body)).await?;
                    }
                }
            }
            frontend::Message::Forward(frame) => {
                // pipe this frame directly upstream
                upstream.send(frontend::Message::Forward(frame)).await?;
            }
            frontend::Message::PasswordMessage(..)
            | frontend::Message::SASLInitialResponse(..)
            | frontend::Message::SASLResponse(..) => {
                tracing::warn!("Authentication message received outside of a handshake");
                break;
            }
        }
    }

    Ok(())
}

/// Connection configurations for an individual database
#[derive(Clone)]
struct Database {
    user: String,
    password: String,
    database: String,
    host: IpAddr,
    port: u16,
    protocol_version: i32,
}

// TODO: turn this into a proto for gRPC handlers, too
/// Database connection configurations organized into leaders and followers
#[allow(unused)]
struct Cluster {
    leaders: Vec<Database>,
    followers: Vec<Database>,
    statement_guard: Guard,
}

impl Default for Cluster {
    // FIXME: remove this impl
    fn default() -> Self {
        let leader = Database {
            user: "postgres".into(),
            password: "supersecretpassword".into(),
            database: "postgres".into(),
            host: [127, 0, 0, 1].into(),
            port: 5432,
            protocol_version: 196608, // FIXME: make const
        };

        let statement_guard = Guard::new(
            AllowedStatements::List(vec![Command::Select]),
            AllowedFunctions::List(vec![]),
        );

        Self {
            leaders: vec![leader],
            followers: vec![],
            statement_guard,
        }
    }
}
