use crate::{
    authentication::ClusterConfiguration,
    cluster::{self, Cluster, CLUSTERS},
    connection,
    credentials::{self, Credentials},
    protocol::{backend, frontend, startup},
    router::{self, Router},
    tcp,
};
use futures_util::{future, stream::SplitStream, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use std::sync::Arc;
use thiserror::Error;

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
    #[error("Error flushing startup messages on session init")]
    Flush,
    #[error("Connection attempt failed authorization step")]
    Unauthorized,
    #[error("Message type not yet supported")]
    Unimplemented,
    #[error("Connection closed unexpectedly")]
    UnexpectedEof,
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

        let router = Router::new(cluster, backend_messages, Arc::new(statement_guard));

        Ok(Self {
            router,
            frontend_messages,
        })
    }

    /// Broker messages between a Connection and Cluster until the Session ends
    #[tracing::instrument(skip(self))]
    pub async fn serve(self) -> Result<(), Error> {
        let router = self.router;

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
            .try_for_each_concurrent(None, |message| router.route(message).map_err(Error::Router))
            .await
    }
}
