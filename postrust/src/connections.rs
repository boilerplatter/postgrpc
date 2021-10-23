use crate::protocol::{frontend, startup};
use futures_core::Stream;
use futures_util::{Sink, StreamExt};
use std::{
    fmt::{self, Display},
    io,
    net::SocketAddr,
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
    #[error("Error reading next message: {0}")]
    Read(std::io::Error),
    #[error("Error writing next message: {0}")]
    Write(std::io::Error),
}

/// Stream of new TCP connections from a listener
pub struct Connections {
    listener: TcpListener,
}

impl From<TcpListener> for Connections {
    fn from(listener: TcpListener) -> Self {
        Self { listener }
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

                tracing::debug!(remote_peer = %remote_peer, "Connection accepted");

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

impl<C> fmt::Debug for Connection<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, formatter)
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
