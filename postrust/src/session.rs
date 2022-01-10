use crate::{
    authentication::ClusterConfiguration,
    cluster::{self, Cluster, CLUSTERS},
    connection,
    credentials::{self, Credentials},
    protocol::{backend, frontend, startup},
    tcp,
};
use futures_util::{future, stream::SplitStream, SinkExt, StreamExt, TryStreamExt};
use postguard::Guard;
use std::{fmt, sync::Arc};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error reading messages from backend connection: {0}")]
    Broadcast(#[from] BroadcastStreamRecvError),
    #[error(transparent)]
    Cluster(#[from] cluster::Error),
    #[error(transparent)]
    Connection(#[from] connection::Error),
    #[error(transparent)]
    Credentials(#[from] credentials::Error),
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Error flushing startup messages on session init")]
    Flush,
    #[error("Connection attempt failed authorization step")]
    Unauthorized,
    #[error("Message type not yet supported")]
    Unimplemented,
    #[error("Connection closed unexpectedly")]
    UnexpectedEof,
    #[error("Error syncing messages between connections")]
    Sync,
}

/// User session wrapper that brokers messages between user connections and clusters
pub struct Session {
    inner: InnerSession,
    frontend_messages: SplitStream<tcp::Connection<frontend::Codec>>,
}

impl Session {
    /// Create a new session
    #[tracing::instrument]
    pub async fn new(mut connection: tcp::Connection<startup::Codec>) -> Result<Self, Error> {
        tracing::debug!("Retrieving Session for Connection");

        // get the remote address for logging
        let remote_peer = connection.peer();

        // handle the startup messages
        let mut credentials = Credentials::build();

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

        // handle the password verification and client-side auth phase
        let mut connection = tcp::Connection::<frontend::Codec>::from(connection);

        let ClusterConfiguration {
            leaders,
            followers,
            statement_guard,
        } = match connection.try_next().await?.ok_or(Error::UnexpectedEof)? {
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
        }?;

        // get or init the cluster for the session
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

        // flush the cluster's startup messages on session init
        let (backend_messages, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        for message in cluster.leader().await?.startup_messages().await? {
            backend_messages.send(message).map_err(|_| Error::Flush)?;
        }

        // send messages from the session back upstream to the connection
        let (mut user_backend_messages, frontend_messages) = connection.split();

        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                user_backend_messages.send(message).await?;
            }

            tracing::debug!(connection = ?&remote_peer, "Closing Connection");

            Ok::<_, Error>(())
        });

        tracing::info!("Session initiated");

        Ok(Self {
            inner: InnerSession {
                cluster,
                backend_messages,
                statement_guard: Arc::new(statement_guard),
            },
            frontend_messages,
        })
    }

    /// Broker messages between a Connection and Cluster until the Session ends
    #[tracing::instrument(skip(self))]
    pub async fn serve(self) -> Result<(), Error> {
        let session = self.inner;

        self.frontend_messages
            .map_err(Error::Tcp)
            .try_take_while(|message| match message {
                frontend::Message::Terminate => {
                    tracing::info!("Session terminated by user");
                    future::ok(false)
                }
                frontend::Message::PasswordMessage(..)
                | frontend::Message::SASLInitialResponse(..)
                | frontend::Message::SASLResponse(..) => {
                    tracing::warn!("Authentication message received outside of a handshake");
                    future::err(Error::Unauthorized)
                }
                frontend::Message::Forward(..) => {
                    tracing::error!("Unsupported message type found");
                    future::err(Error::Unimplemented)
                }
                _ => future::ok(true),
            })
            .try_for_each_concurrent(None, |message| async {
                match message {
                    frontend::Message::Bind(body) => {
                        tracing::debug!(body = ?body, "Bind message received");
                        session.route(body).await?;
                    }
                    frontend::Message::Close(body) => {
                        tracing::debug!(body = ?body, "Close message received");
                        session.route(body).await?;
                    }
                    frontend::Message::Execute(body) => {
                        tracing::debug!(body = ?body, "Execute message received");
                        session.route(body).await?;
                    }
                    frontend::Message::Describe(body) => {
                        tracing::debug!(body = ?body, "Describe message received");
                        session.route(body).await?;
                    }
                    frontend::Message::Parse(body) => {
                        tracing::debug!(body = ?body, "Parse message received");

                        let query = body.query();
                        let statement = String::from_utf8_lossy(&query);
                        if session.guard(&statement)? {
                            session.route(body).await?;
                        }
                    }
                    frontend::Message::Query(body) => {
                        tracing::debug!(body = ?body, "Query message received");

                        let query = body.query();
                        let statement = String::from_utf8_lossy(&query);

                        if session.guard(&statement)? {
                            session.route(body).await?;
                        }
                    }
                    _ => {
                        // TODO: handle an explicit subset of messages by this point
                        unreachable!("Message filtered improperly")
                    }
                }

                Ok(())
            })
            .await
    }
}

/// Internal session components for better logic encapsulation and easier splitting/sharing
struct InnerSession {
    cluster: Arc<Cluster>,
    backend_messages: UnboundedSender<backend::Message>,
    statement_guard: Arc<Guard>,
}

impl InnerSession {
    /// Guard against invalid statements using the provided statement guard
    // TODO: extract lower-level parse step from postrust for AST-based plugin system
    fn guard(&self, statement: &str) -> Result<bool, Error> {
        match self.statement_guard.guard(statement) {
            Ok(..) => Ok(true),
            Err(error) => {
                tracing::warn!(reason = %error, "Statement rejected");

                // send back an error response
                self.backend_messages
                    .send(backend::Message::ErrorResponse {
                        code: "42501".into(),
                        message: error.to_string().into(),
                        severity: backend::Severity::Error,
                    })
                    .map_err(|_| Error::Sync)?;

                // return a ReadyForQuery message
                self.backend_messages
                    .send(backend::Message::ReadyForQuery {
                        transaction_status: backend::TransactionStatus::Idle,
                    })
                    .map_err(|_| Error::Sync)?;

                Ok(false)
            }
        }
    }

    /// Route a single frontend message body to the correct proxied connection
    // FIXME: handle routing logic in a dedicated Router struct
    #[tracing::instrument(skip(self))]
    async fn route<P>(&self, payload: P) -> Result<(), Error>
    where
        P: Routable + fmt::Debug + Into<frontend::Message>,
    {
        // route to the proper connection based on the payload
        let mut connection = if payload.is_read_only() {
            self.cluster.follower().await?
        } else {
            self.cluster.leader().await?
        };

        // send the payload to the connection
        connection.send(payload.into()).await?;

        // proxy backend messages back to the frontend for the duration of the transaction
        // FIXME: keep track of in-progress transactions where appropriate
        let mut backend_messages = connection.transaction().await?;

        while let Some(message) = backend_messages.try_next().await? {
            self.backend_messages
                .send(message)
                .map_err(|_| Error::Sync)?;
        }

        // TODO: handle prepared statements in extended query protocol
        // - track Parse messages (and their underlying connections) by their name, fingerprint,
        // and reference count (dropping those Parse entries when there are no more clients
        // actively referencing them) in Cluster
        // - route Bind messages based on Cluster statement table (should a new Parse message
        // be sent along in the case of backend reconnects, etc? to start, no... send the
        // equivalent Not Found SQL message)
        // - map between the Session's client-provided statement names and their hashed equivalents
        // in the given proxied connection in Session
        // - track bound portals in each proxied connection
        // - route Execute messages to proxied connections by portal name

        Ok(())
    }
}

/// frontend message payloads that can be routed by a Session
trait Routable {
    fn is_read_only(&self) -> bool {
        false
    }

    // TODO: handle routing to SPECIFIC leaders and followers based on payload contents too
    // (where appropriate)
}

impl Routable for frontend::BindBody {}
impl Routable for frontend::CloseBody {}
impl Routable for frontend::DescribeBody {}
impl Routable for frontend::ExecuteBody {}
impl Routable for frontend::QueryBody {}
impl Routable for frontend::ParseBody {}
