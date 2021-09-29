use crate::{
    pools::{Connection, Pool, RawConnect, RawConnection},
    proto::channel::{channel_server::Channel as GrpcService, ListenRequest, NotifyRequest},
    protocol::json,
};
use futures::{pin_mut, StreamExt, TryStream, TryStreamExt};
use std::{collections::BTreeMap, fmt, sync::Arc};
use thiserror::Error;
use tokio::sync::{
    broadcast::{self, Sender},
    mpsc, RwLock,
};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, ReceiverStream};
use tonic::{Request, Response, Status};

#[derive(Debug, Error)]
pub enum Error<P, C>
where
    P: Into<Status> + std::error::Error + 'static,
    C: Into<Status> + std::error::Error + 'static,
{
    #[error("Error broadcasting message on the channel: {0}")]
    Broadcast(#[from] broadcast::error::SendError<prost_types::Value>),
    #[error("Error receiving message from the channel: {0}")]
    Channel(#[from] BroadcastStreamRecvError),
    #[error(transparent)]
    Connection(C),
    #[error(transparent)]
    Pool(P),
    #[error("Error sending message through response stream: {0}")]
    Stream(#[from] mpsc::error::SendError<Result<prost_types::Value, Status>>),
    #[error("SQL Query Error: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("Channel not found")]
    NotFound,
}

impl<P, C> From<Error<P, C>> for Status
where
    P: Into<Status> + std::error::Error + 'static,
    C: Into<Status> + std::error::Error + 'static,
{
    fn from(error: Error<P, C>) -> Self {
        let message = format!("{}", &error);

        match error {
            Error::Connection(error) => error.into(),
            Error::Pool(error) => error.into(),
            Error::NotFound => Self::not_found(message),
            Error::Query(..) => Self::invalid_argument(message),
            Error::Broadcast(..) | Error::Channel(..) | Error::Stream(..) => {
                Self::internal(message)
            }
        }
    }
}

#[derive(Clone)]
struct OpenChannel<C>
where
    C: Connection,
{
    connection: C,
    transmitter: Sender<prost_types::Value>,
}

/// Protocol-agnostic Channel handlers for any connection pool
#[derive(Clone)]
pub struct Channel<P, K>
where
    K: Ord,
    P: RawConnect<K>,
{
    // FIXME: convert to a meta-pool like the transaction pool
    open_channels: Arc<RwLock<BTreeMap<String, OpenChannel<P::Client>>>>,
    pool: Arc<P>,
}

impl<P, K> Channel<P, K>
where
    K: Ord + fmt::Debug + Clone,
    P: Pool<K> + RawConnect<K>,
    P::RawConnection: 'static,
    P::Error: std::error::Error + Send + Sync + 'static,
    <P::Client as Connection>::Error: std::error::Error + 'static,
    <P::Connection as Connection>::Error: std::error::Error + 'static,
    <P::RawConnection as RawConnection>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(pool: Arc<P>) -> Self {
        Self {
            open_channels: Arc::new(RwLock::new(BTreeMap::new())),
            pool,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn listen(
        &self,
        name: String,
    ) -> Result<
        impl TryStream<
            Ok = prost_types::Value,
            Error = Error<P::Error, <P::RawConnection as RawConnection>::Error>,
        >,
        Error<P::Error, <P::Client as Connection>::Error>,
    > {
        tracing::info!("Listening to channel");

        // fetch or establish the open connection for the channel
        let open_channels = self.open_channels.read().await;

        let messages = match open_channels.get(&name) {
            Some(channel) => BroadcastStream::new(channel.transmitter.subscribe()),
            None => {
                drop(open_channels);

                // set up the broadcast channel
                let (transmitter, receiver) = tokio::sync::broadcast::channel(8);

                // set up the connection
                let (connection, mut raw_connection) =
                    self.pool.connect().await.map_err(Error::Pool)?;

                tokio::spawn({
                    let transmitter = transmitter.clone();

                    async move {
                        let mut messages = futures::stream::poll_fn(move |context| {
                            raw_connection.poll_messages(context)
                        });

                        while let Some(message) =
                            messages.try_next().await.map_err(Error::Connection)?
                        {
                            transmitter.send(message)?;
                        }

                        Ok::<_, Error<P::Error, _>>(())
                    }
                });

                connection
                    .batch(&format!("LISTEN {};", name))
                    .await
                    .map_err(Error::Connection)?;

                // store the broadcast channel for later use
                let channel = OpenChannel {
                    connection,
                    transmitter,
                };

                self.open_channels.write().await.insert(name, channel);

                BroadcastStream::new(receiver)
            }
        }
        .map_err(Error::Channel);

        Ok(messages)
    }

    #[tracing::instrument(skip(self))]
    async fn notify(
        &self,
        name: String,
        message: prost_types::Value,
    ) -> Result<(), Error<P::Error, <P::Client as Connection>::Error>> {
        tracing::info!("Notifying through channel");

        // format the notification query
        let notification = format!(
            r#"NOTIFY {}, '{}';"#,
            &name,
            json::from_proto_value(message)
        );

        // get the channel from the cache
        let open_channels = self.open_channels.read().await;

        let channel = open_channels.get(&name).ok_or(Error::NotFound)?;

        channel
            .connection
            .batch(&notification)
            .await
            .map_err(Error::Connection)?;

        Ok(())
    }
}

/// gRPC service implementation for Channel service
#[tonic::async_trait]
impl<P> GrpcService for Channel<P, Option<String>>
where
    P: Pool<Option<String>> + RawConnect<Option<String>> + Send + Sync + 'static,
    P::Client: Send + Sync,
    P::Connection: Send + Sync,
    P::Error: std::error::Error + Send + Sync + 'static,
    <P::Client as Connection>::Error: std::error::Error + 'static,
    <P::Connection as Connection>::Error: std::error::Error + Send + Sync + 'static,
    <P::RawConnection as RawConnection>::Error: std::error::Error + Send + Sync + 'static,
{
    type ListenStream = ReceiverStream<Result<prost_types::Value, Status>>;

    async fn listen(
        &self,
        request: Request<ListenRequest>,
    ) -> Result<Response<Self::ListenStream>, Status> {
        // create the row stream transmitter and receiver
        let (transmitter, receiver) = tokio::sync::mpsc::channel(100);

        // get the row stream
        let ListenRequest { name } = request.into_inner();

        let messages = Channel::listen(self, name).await?.map_err(Status::from);

        // emit the rows as a Send stream
        tokio::spawn(async move {
            pin_mut!(messages);

            while let Some(message) = messages.next().await {
                transmitter.send(message).await?;
            }

            Ok::<_, mpsc::error::SendError<_>>(())
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn notify(&self, request: Request<NotifyRequest>) -> Result<Response<()>, Status> {
        // send the message to the channel
        let NotifyRequest { name, message } = request.into_inner();

        if let Some(message) = message {
            Channel::notify(self, name, message).await?;
        }

        Ok(Response::new(()))
    }
}
