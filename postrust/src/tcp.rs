use crate::protocol::{
    backend,
    frontend::{self, SASLInitialResponseBody, SASLResponseBody},
    startup,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{Sink, SinkExt, StreamExt, TryStreamExt};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use std::{
    collections::BTreeMap,
    fmt::{self, Display},
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Encoder, Framed};

// FIXME: Unify error codes in protocol module
static CONNECTION_FAILURE: Bytes = Bytes::from_static(b"08006");
static CONNECTION_EXCEPTION: Bytes = Bytes::from_static(b"08000");
static INVALID_PASSWORD: Bytes = Bytes::from_static(b"28P01");

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error accepting connection: {0}")]
    Accept(std::io::Error),
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Error reading next message: {0}")]
    Read(std::io::Error),
    #[error("Error writing next message: {0}")]
    Write(std::io::Error),
    #[error("Connection attempt failed authorization step")]
    Unauthorized,
    #[error("Error connecting to upstream database at {address}: {source}")]
    TcpConnect {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Error completing SASL authentication handshake: {0}")]
    Sasl(std::io::Error),
    #[error("Error response from upstream")]
    Upstream {
        code: Bytes,
        severity: backend::Severity,
        message: Bytes,
    },
}

impl From<&Error> for backend::Message {
    fn from(error: &Error) -> Self {
        match error {
            Error::Accept(..)
            | Error::Listen { .. }
            | Error::Read(..)
            | Error::Write(..)
            | Error::TcpConnect { .. } => Self::ErrorResponse {
                code: CONNECTION_FAILURE.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Error,
            },
            Error::Sasl(..) => Self::ErrorResponse {
                code: CONNECTION_EXCEPTION.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Error,
            },
            Error::Unauthorized => Self::ErrorResponse {
                code: INVALID_PASSWORD.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Error,
            },
            Error::Upstream {
                code,
                severity,
                message,
            } => Self::ErrorResponse {
                code: code.clone(),
                severity: *severity,
                message: message.clone(),
            },
        }
    }
}

/// Stream of new TCP connections from a listener
pub struct Connections {
    listener: TcpListener,
}

impl Connections {
    #[tracing::instrument]
    pub async fn new(address: SocketAddr) -> Result<Self, Error> {
        let listener = TcpListener::bind(&address)
            .await
            .map_err(|source| Error::Listen { address, source })?;

        tracing::debug!("Connections stream initialized");

        Ok(Self { listener })
    }
}

impl Stream for Connections {
    type Item = Result<Connection<startup::Codec>, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.listener.poll_accept(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(Error::Accept(error)))),
            Poll::Ready(Ok((socket, remote_peer))) => {
                let connection = Connection::new(socket, startup::Codec, remote_peer);

                tracing::debug!(%remote_peer, "Connection accepted");

                Poll::Ready(Some(Ok(connection)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Connection stream that returns frames from a TcpStream, parameterized by Codec
    pub struct Connection<C> {
        remote_peer: SocketAddr,
        #[pin]
        frames: Framed<TcpStream, C>,
    }
}

impl<C> Connection<C> {
    /// Create a new framed connection from a codec
    pub fn new(socket: TcpStream, codec: C, remote_peer: SocketAddr) -> Self {
        Self {
            frames: Framed::new(socket, codec),
            remote_peer,
        }
    }

    /// Get the peer address of this connection
    pub fn peer(&self) -> SocketAddr {
        self.remote_peer
    }
}

impl<C> Display for Connection<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.remote_peer.fmt(formatter)
    }
}

impl<C> fmt::Debug for Connection<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, formatter)
    }
}

impl Connection<backend::Codec> {
    /// Initiate a brand new backend connection
    #[tracing::instrument(skip(password))]
    pub async fn connect(
        address: SocketAddr,
        user: String,
        password: String,
        database: String,
    ) -> Result<Self, Error> {
        let mut upstream = TcpStream::connect(address)
            .await
            .map(|socket| Connection::new(socket, startup::Codec, address))
            .map_err(|source| Error::TcpConnect { address, source })?;

        // handle the upstream auth handshake
        #[allow(clippy::mutable_key_type)]
        let mut options = BTreeMap::new();

        options.insert("database".into(), database.into());
        options.insert("application_name".into(), "postrust".into());
        options.insert("client_encoding".into(), "UTF8".into());

        upstream
            .send(startup::Message::Startup {
                user: user.into(),
                options,
                version: startup::VERSION,
            })
            .await?;

        Connection::<backend::Codec>::from(upstream)
            .authenticate(password.as_bytes())
            .await
    }

    /// Authenticate a Connection with SASL
    #[tracing::instrument(skip(password))]
    async fn authenticate(mut self, password: &[u8]) -> Result<Self, Error> {
        match self.try_next().await?.ok_or(Error::Unauthorized)? {
            backend::Message::AuthenticationSASL { mechanisms } => {
                let mechanism = mechanisms.first().cloned().ok_or(Error::Unauthorized)?;

                // construct a ScramSha256
                let channel_binding = ChannelBinding::unrequested(); // is this right?
                let mut scram_client = ScramSha256::new(password, channel_binding);

                // send out a SASLInitialResponse message
                self.send(frontend::Message::SASLInitialResponse(
                    SASLInitialResponseBody {
                        mechanism,
                        initial_response: scram_client.message().to_vec().into(),
                    },
                ))
                .await?;

                // wait for the SASL continuation message from upstream
                match self.try_next().await?.ok_or(Error::Unauthorized)? {
                    backend::Message::AuthenticationSASLContinue { data } => {
                        scram_client.update(&data).map_err(Error::Sasl)?;
                    }
                    _ => return Err(Error::Unauthorized),
                }

                // send out a SASLResponse message
                self.send(frontend::Message::SASLResponse(SASLResponseBody {
                    data: scram_client.message().to_vec().into(),
                }))
                .await?;

                // wait for the final SASL message from upstream
                match self.try_next().await?.ok_or(Error::Unauthorized)? {
                    backend::Message::AuthenticationSASLFinal { data } => {
                        scram_client.finish(&data).map_err(Error::Sasl)?;
                    }
                    _ => return Err(Error::Unauthorized),
                }

                // wait for AuthenticationOk then carry on
                match self.try_next().await?.ok_or(Error::Unauthorized)? {
                    backend::Message::AuthenticationOk => (),
                    _ => return Err(Error::Unauthorized),
                }

                tracing::debug!("SASL handshake completed");
            }
            // forward error responses
            backend::Message::ErrorResponse {
                code,
                severity,
                message,
            } => {
                return Err(Error::Upstream {
                    code,
                    severity,
                    message,
                })
            }
            backend::Message::AuthenticationOk => {
                // all good, carry on
            }
            message => {
                tracing::warn!(?message, "Unsupported message in authentication");
                // TODO: support other auth schemes
                // reject all other responses
                return Err(Error::Unauthorized);
            }
        }

        Ok(self)
    }
}

impl From<Connection<startup::Codec>> for Connection<frontend::Codec> {
    fn from(previous: Connection<startup::Codec>) -> Self {
        let socket = previous.frames.into_inner();

        Self {
            remote_peer: previous.remote_peer,
            frames: Framed::new(socket, frontend::Codec),
        }
    }
}

impl From<Connection<startup::Codec>> for Connection<backend::Codec> {
    fn from(previous: Connection<startup::Codec>) -> Self {
        let socket = previous.frames.into_inner();

        Self {
            remote_peer: previous.remote_peer,
            frames: Framed::new(socket, backend::Codec),
        }
    }
}

impl Stream for Connection<startup::Codec> {
    type Item = Result<startup::Message, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projection = self.project();

        match projection.frames.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Error::Read(error)))),
            Poll::Ready(Some(Ok(mut frame))) => {
                let handshake = startup::Message::parse(&mut frame)
                    .map_err(Error::Read)
                    .transpose();

                Poll::Ready(handshake)
            }
        }
    }
}

impl Stream for Connection<frontend::Codec> {
    type Item = Result<frontend::Message, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projection = self.project();

        match projection.frames.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Error::Read(error)))),
            Poll::Ready(Some(Ok(mut frame))) => {
                let handshake = frontend::Message::parse(&mut frame)
                    .map_err(Error::Read)
                    .transpose();

                Poll::Ready(handshake)
            }
        }
    }
}

impl Stream for Connection<backend::Codec> {
    type Item = Result<backend::Message, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projection = self.project();

        match projection.frames.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Error::Read(error)))),
            Poll::Ready(Some(Ok(mut frame))) => {
                let handshake = backend::Message::parse(&mut frame)
                    .map_err(Error::Read)
                    .transpose();

                Poll::Ready(handshake)
            }
        }
    }
}

impl<C, I> Sink<I> for Connection<C>
where
    C: Encoder<I, Error = io::Error>,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project()
            .frames
            .poll_ready(context)
            .map_err(Error::Write)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().frames.start_send(item).map_err(Error::Write)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project()
            .frames
            .poll_flush(context)
            .map_err(Error::Write)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project()
            .frames
            .poll_close(context)
            .map_err(Error::Write)
    }
}
