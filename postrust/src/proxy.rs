use crate::frontend::{Handshake, HandshakeCodec, Message, MessageCodec};
use bytes::{BufMut, Bytes, BytesMut};
use futures_core::Stream;
use futures_util::{FutureExt, Sink, SinkExt, StreamExt, TryStreamExt};
use postguard::{AllowedFunctions, AllowedStatements, Command, Guard};
use serde::Deserialize;
use std::{
    fmt::{self, Display},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Encoder, Framed};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error accepting connection: {0}")]
    Accept(std::io::Error),
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Error reading next message: {0}")]
    Read(std::io::Error),
    #[error("Error writing next message: {0}")]
    Write(std::io::Error),
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
                                Handshake::SslRequest => {
                                    tracing::debug!(connection = %connection, "SSLRequest heard during handshake");

                                    connection.send(BytesMut::from("N")).await.map_err(Error::Write)?;
                                },
                                Handshake::Startup { version, user, mut options } => {
                                    tracing::debug!(
                                        version = %version,
                                        user = ?user,
                                        options = ?options,
                                        "Startup initiated"
                                    );

                                    // write the AuthenticationMd5Password message
                                    let mut message = BytesMut::new();
                                    message.put_u8(b'R');
                                    message.put_i32(12);
                                    message.put_i32(5);
                                    message.put_slice(&password_salt);

                                    connection
                                        .send(message)
                                        .await
                                        .map_err(Error::Write)?;

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
                        let mut connection = Connection::<MessageCodec>::from(connection);

                        // validate the provided credentials with the next message
                        if let Some(Message::PasswordMessage(message)) = connection.try_next().await? {
                            let password = message.password(password_salt);

                            // TODO:
                            // run the auth query/request/what-have-you

                            // return the AuthenticationOk message
                            let mut authentication_ok_message = BytesMut::new();
                            authentication_ok_message.put_u8(b'R');
                            authentication_ok_message.put_i32(8);
                            authentication_ok_message.put_i32(0);
                            connection.send(authentication_ok_message).await.map_err(Error::Write)?;

                            // TODO:
                            // send some backend parameters back to the client

                            // return the ReadyForQuery message
                            let mut ready_for_query_message = BytesMut::new();
                            ready_for_query_message.put_u8(b'Z');
                            ready_for_query_message.put_i32(5);
                            ready_for_query_message.put_u8(b'I');
                            connection.send(ready_for_query_message).await.map_err(Error::Write)?;

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
                                Message::Query(body) => {
                                    tracing::info!(body = ?body, "Query message received");

                                    // FIXME: let guard take bytes, too!
                                    let statement =
                                        unsafe { String::from_utf8_unchecked(body.query().to_vec()) };

                                    if let Err(error) = statement_guard.guard(&statement) {
                                        tracing::warn!(reason = %error, "Statement rejected");

                                        // send back an error response
                                        let mut fields = BytesMut::new();
                                        fields.put_u8(b'S');
                                        fields.put_slice(b"ERROR");
                                        fields.put_u8(0);
                                        fields.put_u8(b'C');
                                        fields.put_slice(b"42501");
                                        fields.put_u8(0);
                                        fields.put_u8(b'M');
                                        fields.put_slice(error.to_string().as_bytes());
                                        fields.put_u8(0);
                                        fields.put_u8(0);
                                        let fields_length = fields.len() as i32;

                                        let mut error_response_message = BytesMut::new();
                                        error_response_message.put_u8(b'E');
                                        error_response_message.put_i32(fields_length + 4);
                                        error_response_message.put_slice(&fields);

                                        connection.send(error_response_message).await.map_err(Error::Write)?;

                                        // return the ReadyForQuery message
                                        let mut ready_for_query_message = BytesMut::new();
                                        ready_for_query_message.put_u8(b'Z');
                                        ready_for_query_message.put_i32(5);
                                        ready_for_query_message.put_u8(b'I');
                                        connection.send(ready_for_query_message).await.map_err(Error::Write)?;
                                    }

                                    // TODO: otherwise, reconstruct the original message and
                                    // forward it upstream
                                }
                                Message::Forward(_frame) => {
                                    // TODO: pipe this frame directly to an upstream
                                    // postgres connection
                                }
                                Message::PasswordMessage(..) => tracing::warn!("PasswordMessage received outside of a handshake")
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

// TODO (refactoring):
// TcpListener should be represented as a stream of Connections
// Connections should yield a Connection, which is a stream of framed messages
// parameterized over different Codecs in a typestate-style lifecycle

/// Stream of new TCP connections from a listener
struct Connections {
    listener: TcpListener,
}

impl From<TcpListener> for Connections {
    fn from(listener: TcpListener) -> Self {
        Self { listener }
    }
}

impl Stream for Connections {
    type Item = Result<Connection<HandshakeCodec>, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.listener.poll_accept(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(Error::Accept(error)))),
            Poll::Ready(Ok((socket, remote_peer))) => {
                let connection = Connection::new(socket, HandshakeCodec, remote_peer);

                tracing::debug!(remote_peer = %remote_peer, "Connection accepted");

                Poll::Ready(Some(Ok(connection)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Connection stream that returns frames from a TcpStream, parameterized by Codec
    struct Connection<C> {
        remote_peer: SocketAddr,
        #[pin]
        frames: Framed<TcpStream, C>,
    }
}

impl<C> Connection<C> {
    fn new(socket: TcpStream, codec: C, remote_peer: SocketAddr) -> Self {
        Self {
            frames: Framed::new(socket, codec),
            remote_peer,
        }
    }
}

impl<C> Display for Connection<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.remote_peer.fmt(formatter)
    }
}

impl From<Connection<HandshakeCodec>> for Connection<MessageCodec> {
    fn from(previous: Connection<HandshakeCodec>) -> Self {
        let socket = previous.frames.into_inner();

        Self {
            remote_peer: previous.remote_peer,
            frames: Framed::new(socket, MessageCodec),
        }
    }
}

impl Stream for Connection<HandshakeCodec> {
    type Item = Result<Handshake, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projection = self.project();

        match projection.frames.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Error::Read(error)))),
            Poll::Ready(Some(Ok(mut frame))) => {
                let handshake = Handshake::parse(&mut frame)
                    .map_err(Error::Read)
                    .transpose();

                Poll::Ready(handshake)
            }
        }
    }
}

impl Stream for Connection<MessageCodec> {
    type Item = Result<Message, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projection = self.project();

        match projection.frames.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Error::Read(error)))),
            Poll::Ready(Some(Ok(mut frame))) => {
                let handshake = Message::parse(&mut frame).map_err(Error::Read).transpose();

                Poll::Ready(handshake)
            }
        }
    }
}

impl<C, I> Sink<I> for Connection<C>
where
    C: Encoder<I>,
{
    type Error = C::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project().frames.poll_ready(context)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().frames.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project().frames.poll_flush(context)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project().frames.poll_close(context)
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
