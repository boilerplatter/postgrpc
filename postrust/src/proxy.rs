use crate::frontend::{Handshake, Message};
use bytes::BytesMut;
use serde::Deserialize;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinError,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error accepting connection: {0}")]
    Accept(std::io::Error),
    #[error("Error listening at {address}: {source}")]
    Listen {
        address: SocketAddr,
        source: std::io::Error,
    },
    #[error("Error parsing message: {0}")]
    Parse(std::io::Error),
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

        let listener = TcpListener::bind(&address)
            .await
            .map_err(|source| Error::Listen { address, source })?;

        tracing::info!("Listening on {address}", address = &address);

        let connection = Connection::new(&listener);

        loop {
            connection.listen().await?;
        }
    }
}

struct Connection<'a> {
    listener: &'a TcpListener,
}

impl<'a> Connection<'a> {
    fn new(listener: &'a TcpListener) -> Self {
        Self { listener }
    }

    // FIXME: turn entire connection into a stream that wraps TcpStream
    async fn listen(&self) -> Result<(), Error> {
        let (mut socket, remote_peer) = self.listener.accept().await.map_err(Error::Accept)?;

        tracing::debug!(remote_peer = %remote_peer, "Connection accepted");

        // spawn the real work off on the executor
        let connection = tokio::spawn(async move {
            let mut frontend_handshake = Handshake::Pending;

            loop {
                socket.readable().await.map_err(Error::Read)?;

                let mut socket_buffer = [0; 1028];
                let mut message_buffer = BytesMut::new();

                match socket.read(&mut socket_buffer).await {
                    Ok(0) => {
                        tracing::debug!(remote_peer = %remote_peer, "Connection closing");

                        break;
                    }
                    Ok(..) => {
                        // dump the socket buffer into the message buffer
                        message_buffer.extend_from_slice(&socket_buffer);

                        // progress through the handshake state machine
                        match frontend_handshake {
                            Handshake::Complete => {
                                // TODO:
                                // use the auth response to key into the right connection
                                // (from the pool, of course)
                                // guard statements in parse and query messages
                            }
                            Handshake::Pending | Handshake::SslRequest => {
                                match Handshake::parse(&mut message_buffer).map_err(Error::Parse)? {
                                    Some(Handshake::SslRequest) => {
                                        tracing::debug!("SSLRequest heard during handshake");
                                        socket.write_u8(b'N').await.map_err(Error::Write)?;
                                        frontend_handshake = Handshake::SslRequest;
                                    }
                                    Some(Handshake::Startup {
                                        password_salt,
                                        user,
                                        options,
                                        version,
                                    }) => {
                                        tracing::debug!(
                                            version = %version,
                                            user = ?user,
                                            options = ?options,
                                            "Startup initiated"
                                        );

                                        // write the AuthenticationMd5Password message
                                        socket.write_u8(b'R').await.map_err(Error::Write)?;
                                        socket.write_i32(12).await.map_err(Error::Write)?;
                                        socket.write_i32(5).await.map_err(Error::Write)?;

                                        socket
                                            .write_all(&password_salt)
                                            .await
                                            .map_err(Error::Write)?;

                                        frontend_handshake = Handshake::Startup {
                                            password_salt,
                                            user,
                                            options,
                                            version,
                                        };
                                    }
                                    _ => (),
                                }
                            }
                            Handshake::Startup { password_salt, .. } => {
                                if let Some(Message::PasswordMessage(message)) =
                                    Message::parse(&mut message_buffer).map_err(Error::Parse)?
                                {
                                    let _password = message.password(password_salt);

                                    // TODO:
                                    // run the auth query/request/what-have-you

                                    tracing::debug!("Connection authenticated");

                                    // return the AuthenticationOk message
                                    socket.write_u8(b'R').await.map_err(Error::Write)?;
                                    socket.write_i32(8).await.map_err(Error::Write)?;
                                    socket.write_i32(0).await.map_err(Error::Write)?;

                                    // TODO:
                                    // frame all additional messages

                                    frontend_handshake = Handshake::Complete;
                                }
                            }
                        }
                    }
                    Err(ref error) if error.kind() == io::ErrorKind::WouldBlock => (),
                    Err(error) => return Err(Error::Read(error)),
                }
            }

            Ok(())
        });

        // spawn logging of errors on the executor
        tokio::spawn(async move {
            if let Err(error) = connection.await? {
                tracing::error!(error = ?error, remote_peer = %remote_peer, "Connection dropped")
            }

            Ok::<_, JoinError>(())
        });

        Ok(())
    }
}
