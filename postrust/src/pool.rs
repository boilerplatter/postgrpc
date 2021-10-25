#![allow(clippy::mutable_key_type)]
use crate::{
    authentication::{self, authenticate},
    connections::{self, Connection},
    protocol::{backend, frontend, startup},
};
use futures_core::Stream;
use futures_util::{stream::SplitSink, SinkExt, StreamExt, TryStreamExt};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc::UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Authentication(#[from] authentication::Error),
    #[error(transparent)]
    Connection(#[from] connections::Error),
    #[error("Error syncing proxied connection")]
    Sync,
    #[error("Cluster onfiguration for the current user is missing a leader")]
    MissingLeader,
    #[error("Error connecting to upstream database at {address}: {source}")]
    TcpConnect {
        address: SocketAddr,
        source: std::io::Error,
    },
}

/// Postrust's only supported protocol version
const SUPPORTED_PROTOCOL_VERSION: i32 = 196608;

// TODO:
// use a meta-pool (pool of pools) for each set of resolved user credentials
// set up a cleanup process for expired pools
// send auth requests (if configured) to gRPC or HTTP service
// create pools for each auth response (and create proto for that auth request/response)
// if there are leaders and followers, test AST with postguard for read versus writes
// use a connection from the proper pool to send the request along, returning once finished
//
// TODO low-priority:
// add transaction-level pooling
// add session-level pooling

/// Database connection endpoint configuration
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
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

/// Set of load-balance-ready connections proxied over a channel
#[derive(Default)]
struct ProxiedConnections {
    connections: Vec<UnboundedSender<frontend::Message>>,
    last_used: AtomicUsize,
}

impl ProxiedConnections {
    fn new<C>(connections: C) -> Self
    where
        C: IntoIterator<Item = SplitSink<Connection<backend::Codec>, frontend::Message>>,
    {
        // turn each connection into a sender/receiver pair
        let connections = connections
            .into_iter()
            .map(|mut connection| {
                let (transmitter, mut receiver) = tokio::sync::mpsc::unbounded_channel();

                tokio::spawn(async move {
                    while let Some(message) = receiver.recv().await {
                        connection.send(message).await.map_err(|_| Error::Sync)?;
                    }

                    Ok::<_, Error>(())
                });

                transmitter
            })
            .collect();

        // track which sender was last used
        let last_used = AtomicUsize::new(0);

        Self {
            connections,
            last_used,
        }
    }
}

pin_project_lite::pin_project! {
    /// Wrapper around the connections for a single auth response
    // i.e. keyed/deduped by a hash of the cluster configuration
    pub struct Pool {
        leaders: ProxiedConnections,
        followers: ProxiedConnections,
        transmitter: UnboundedSender<backend::Message>,
        #[pin]
        receiver: UnboundedReceiverStream<backend::Message>,
    }
}

impl Pool {
    /// Create a new connection pool from leader and follower configurations
    pub async fn connect(
        mut leaders: Vec<Endpoint>,
        _followers: Vec<Endpoint>, // FIXME: store configurations separately from connections
    ) -> Result<Self, Error> {
        // guard against empty leader configurations
        if leaders.is_empty() {
            return Err(Error::MissingLeader);
        }

        // connect to the first leader
        let leader = leaders.swap_remove(0);
        let upstream_address = leader.address();

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

        let mut proxied_connection = Connection::<backend::Codec>::from(upstream);

        authenticate(&mut proxied_connection, leader.password.as_bytes()).await?;

        // set up message-passing for this connection
        let (transmitter, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (proxied_connection_sink, mut proxied_connection_stream) = proxied_connection.split();

        // FIXME: make sure that we can clean up after dead connections on either end (sink or
        // stream) without dropping messages
        tokio::spawn({
            let transmitter = transmitter.clone();

            async move {
                // send all messages from the proxied connection
                // to the consumer of the Pool's message stream
                while let Some(message) = proxied_connection_stream.try_next().await? {
                    transmitter.send(message).map_err(|_| Error::Sync)?; // FIXME: make this error better
                }

                Ok::<_, Error>(())
            }
        });

        // store the connection for later use
        let leaders = ProxiedConnections::new([proxied_connection_sink]);
        let followers = ProxiedConnections::new([]);

        Ok(Self {
            leaders,
            followers,
            transmitter,
            receiver: UnboundedReceiverStream::new(receiver),
        })
    }

    /// Create a new Leader sink for this pool for sending messages to leaders
    pub fn leader(&self) -> Leader {
        // FIXME: create more connections in response to higher loads
        let index =
            self.leaders.last_used.fetch_add(1, Ordering::Relaxed) % self.leaders.connections.len();

        let sender = self.leaders.connections[index].clone();

        Leader { sender }
    }

    /// Create a new Follower sink for this pool for sending messages to followers
    #[allow(unused)]
    pub async fn follower(&self) -> Option<Follower> {
        if self.followers.connections.is_empty() {
            None
        } else {
            // FIXME: create more connections in response to higher loads
            let index = self.followers.last_used.fetch_add(1, Ordering::Relaxed)
                % self.followers.connections.len();

            let sender = self.followers.connections[index].clone();

            Some(Follower { sender })
        }
    }
}

impl Stream for Pool {
    type Item = backend::Message;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pool = self.project();

        match pool.receiver.poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(message) => Poll::Ready(message),
        }
    }
}

/// Sink for sending messages to leaders (required for e.g. DDL changes)
pub struct Leader {
    sender: UnboundedSender<frontend::Message>,
}

impl Leader {
    pub fn send(&self, message: frontend::Message) -> Result<(), Error> {
        self.sender.send(message).map_err(|_| Error::Sync)
    }
}

/// Sink for sending messages to followers (usually read-only)
#[allow(unused)]
pub struct Follower {
    sender: UnboundedSender<frontend::Message>,
}

impl Follower {
    #[allow(unused)]
    pub fn send(&self, message: frontend::Message) -> Result<(), Error> {
        self.sender.send(message).map_err(|_| Error::Sync)
    }
}

// TODO:
// Pool should be a Stream of messages from ANY connection
// Pool should expose two Sinks (maybe with a split() method?): Leader + Follower
// (maybe there's work to be done using this instead of a postgres-pool style API?)
