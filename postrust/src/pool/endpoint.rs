use crate::{
    connections::{self, Connection},
    protocol::{
        backend::{self, TransactionStatus},
        frontend,
    },
};
use futures_core::Stream;
use futures_util::{future, SinkExt, StreamExt, TryStreamExt};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc::UnboundedSender, RwLock};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error reading messages from backend connection: {0}")]
    BroadcastRead(#[from] BroadcastStreamRecvError),
    #[error("Error sending message from backend connection: {0}")]
    BroadcastWrite(#[from] broadcast::error::SendError<backend::Message>),
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error("Error syncing proxied connection")]
    Sync,
    #[error("Connection is inactive")]
    Inactivity(#[from] tokio::time::error::Elapsed),
}

/// Postrust's only supported protocol version
// TODO: see if we can support a range of these
const SUPPORTED_PROTOCOL_VERSION: i32 = 196608;

/// Database connection endpoint configuration
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Endpoint {
    pub user: String,
    pub password: String,
    pub database: String,
    host: IpAddr,
    port: u16,
    protocol_version: i32,
}

impl Endpoint {
    pub fn new(user: String, password: String, database: String, host: IpAddr, port: u16) -> Self {
        Self {
            user,
            password,
            database,
            host,
            port,
            protocol_version: SUPPORTED_PROTOCOL_VERSION,
        }
    }

    pub fn address(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Endpoint")
            .field("user", &self.user)
            .field("password", &"******")
            .field("database", &self.database)
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

/// Database connection represented by a sink/stream pair
/// orderable by the number of active receivers
pub struct ProxiedConnection {
    frontend_sink: UnboundedSender<frontend::Message>,
    backend_broadcast: tokio::sync::broadcast::Sender<backend::Message>,
    is_terminated: tokio::sync::watch::Receiver<bool>,
}

impl ProxiedConnection {
    /// Subscribe to the next ReadyForQuery-terminated chunk of messages from the backend
    fn subscribe(&self) -> impl Stream<Item = Result<backend::Message, Error>> {
        BroadcastStream::new(self.backend_broadcast.subscribe())
            .try_take_while(|message| {
                future::ok(!matches!(
                    message,
                    backend::Message::ReadyForQuery {
                        transaction_status: TransactionStatus::Idle,
                    }
                ))
            })
            .map_err(Error::BroadcastRead)
    }

    /// Send a frontend message to the connection
    fn send(&self, message: frontend::Message) -> Result<(), Error> {
        self.frontend_sink.send(message).map_err(|_| Error::Sync)
    }
}

impl From<Connection<backend::Codec>> for ProxiedConnection {
    fn from(connection: Connection<backend::Codec>) -> Self {
        let remote_address = connection.peer();
        let (mut backend_sink, mut backend_stream) = connection.split();
        let (frontend_sink, mut frontend_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (backend_broadcast, _) = tokio::sync::broadcast::channel(128);
        let backend_broadcast_transmitter = backend_broadcast.clone();

        // FIXME: make this configurable
        // consider alternatives to handle long-running queries
        // (right now, this exactly the same as statement timeouts)
        let inactivity_limit = Duration::from_secs(30);

        // send messages from the frontend to the backend through the bounded sender
        #[allow(unreachable_code)]
        let proxy = tokio::spawn({
            async move {
                loop {
                    match tokio::time::timeout(inactivity_limit, frontend_receiver.recv()).await {
                        Ok(Some(message)) => {
                            backend_sink.send(message).await.map_err(|_| Error::Sync)?
                        }
                        Ok(None) => backend_sink
                            .send(frontend::Message::Terminate)
                            .await
                            .map_err(|_| Error::Sync)?,
                        Err(error) => {
                            backend_sink
                                .send(frontend::Message::Terminate)
                                .await
                                .map_err(|_| Error::Sync)?;

                            return Err(Error::Inactivity(error));
                        }
                    }
                }

                Ok::<_, Error>(())
            }
        });

        // broadcast messages from the backend to listeners
        let broadcast = tokio::spawn(async move {
            while let Some(message) = backend_stream.try_next().await? {
                if let Err(error) = backend_broadcast_transmitter
                    .send(message)
                    .map_err(Error::BroadcastWrite)
                {
                    // FIXME: figure out how/if to communicate this to end users
                    tracing::warn!(error = %error, "Error sending message from backend to listeners");
                }
            }

            Ok::<_, Error>(())
        });

        // listen to previous spawned tasks, yielding when one is complete
        let (termination_trigger, is_terminated) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            let result = tokio::select! {
                result = proxy => result,
                result = broadcast => result
            };

            match result {
                Ok(Ok(())) => tracing::warn!(
                    remote_address = %remote_address,
                    "Closing proxied connection due to disconnection"
                ),
                Ok(Err(error)) => match error {
                    Error::Inactivity(..) => {
                        tracing::info!(
                            remote_address = %remote_address,
                            "Closing proxied connection due to inactivity"
                        )
                    }
                    error => {
                        tracing::warn!(
                            remote_address = %remote_address,
                            error = %&error,
                            "Closing proxied connection due to unrecoverable error"
                        )
                    }
                },
                Err(error) => {
                    tracing::warn!(
                        remote_address = %remote_address,
                        error = %&error,
                        "Closing proxied connection with task error"
                    )
                }
            }

            if let Err(error) = termination_trigger.send(true) {
                tracing::error!(
                    remote_address = %remote_address,
                    error = %&error,
                    "Error notifying connection of its termination"
                );
            }
        });

        Self {
            frontend_sink,
            backend_broadcast,
            is_terminated,
        }
    }
}

/// Set of load-balance-ready connections proxied over a channel
pub struct ProxiedConnections {
    /// Sink-like and Stream-like pairs for each connection
    pub connections: Arc<RwLock<Vec<ProxiedConnection>>>,
    /// Endpoint configuration for spawning new connections
    endpoint: Endpoint,
}

impl ProxiedConnections {
    /// Create a new set of ProxiedConnections
    pub fn new<C>(endpoint: Endpoint, connections: C) -> Self
    where
        C: IntoIterator<Item = Connection<backend::Codec>>,
    {
        let connections = connections
            .into_iter()
            .map(ProxiedConnection::from)
            .collect();

        let connections = Arc::new(RwLock::new(connections));

        // Clean up old connections periodically
        tokio::spawn({
            let connections = Arc::clone(&connections);

            async move {
                loop {
                    let connections_read = connections.read().await;
                    let proxied_connections: &Vec<_> = &*connections_read;
                    let total_connections = proxied_connections.len();
                    let terminated_connections: Vec<_> = proxied_connections
                        .iter()
                        .enumerate()
                        .filter(|(_, connection)| *connection.is_terminated.borrow())
                        .map(|(index, _)| index)
                        .collect();

                    // TODO: measure how long this takes
                    if !terminated_connections.is_empty() {
                        tracing::debug!(
                            "Cleaning up {count} terminated connections",
                            count = terminated_connections.len(),
                        );

                        drop(connections_read);

                        let pruned_connections: Vec<ProxiedConnection> =
                            Vec::with_capacity(total_connections - terminated_connections.len());

                        let mut connections = connections.write().await;

                        let previous_connections =
                            std::mem::replace(&mut *connections, pruned_connections);

                        for (index, connection) in previous_connections.into_iter().enumerate() {
                            if !terminated_connections.contains(&index) {
                                connections.push(connection);
                            }
                        }
                    }
                }
            }
        });

        Self {
            connections,
            endpoint,
        }
    }

    /// Subscribe to an idle or newly-created connection
    #[tracing::instrument]
    pub async fn subscribe_next_idle(
        &self,
        init_message: Option<frontend::Message>,
    ) -> Result<impl Stream<Item = Result<backend::Message, Error>>, Error> {
        let connections = self.connections.read().await;

        tracing::debug!(connections = %connections.len(), "Active connections for this endpoint");

        // subscribe to the next idle or new connection
        match connections.iter().find(|connection| {
            connection.backend_broadcast.receiver_count() == 0
                && !*connection.is_terminated.borrow()
        }) {
            Some(connection) => {
                tracing::debug!("Reusing existing connection for subscription");

                let subscription = connection.subscribe();

                if let Some(init_message) = init_message {
                    connection.send(init_message)?;
                }

                Ok(subscription)
            }
            None => {
                drop(connections);

                tracing::info!("Adding connection for endpoint");

                let address = self.endpoint.address();

                let proxied_connection = Connection::<backend::Codec>::connect(
                    address,
                    self.endpoint.user.to_string(),
                    self.endpoint.password.to_string(),
                    self.endpoint.database.to_string(),
                )
                .await
                .map(ProxiedConnection::from)?;

                tracing::debug!("Proxied Connection created for endpoint");

                // drain the proxied_connection stream of startup messages
                proxied_connection
                    .subscribe()
                    .try_for_each(|_| future::ok(()))
                    .await?;

                tracing::debug!("Proxied Connection drained of startup messages");

                // set up the backend message stream from the drained connection
                let backend_messages = proxied_connection.subscribe();

                // send the initial message to the new connection
                if let Some(init_message) = init_message {
                    proxied_connection.send(init_message)?;
                }

                {
                    // store the connection again for later
                    self.connections.write().await.push(proxied_connection);
                }

                Ok(backend_messages)
            }
        }
    }
}

impl fmt::Debug for ProxiedConnections {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxiedConnections")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Collection of endpoints that cycles endlessly
pub struct RoundRobinEndpoints {
    index: AtomicUsize,
    endpoints: Vec<ProxiedConnections>,
}

impl RoundRobinEndpoints {
    /// Create a new round-robin endpoint balancer over proxied connections
    pub fn new(endpoints: Vec<ProxiedConnections>) -> Self {
        Self {
            index: AtomicUsize::new(0),
            endpoints,
        }
    }

    /// Iter-like next for selecting the next endpoint in the Round Robin queue
    pub fn next(&self) -> Option<&ProxiedConnections> {
        let index = self.index.fetch_add(1, Ordering::Relaxed);

        self.endpoints.get(index % self.endpoints.len())
    }
}
