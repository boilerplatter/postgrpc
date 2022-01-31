use crate::{
    cluster::{self, Cluster},
    connection::{self, Connection},
    pool::{Pooled, PooledObject},
    protocol::{
        backend,
        errors::{
            CONNECTION_EXCEPTION, DUPLICATE_PREPARED_STATEMENT, FEATURE_NOT_SUPPORTED,
            INSUFFICIENT_PRIVILEGE, INVALID_SQL_STATEMENT_NAME, INVALID_TRANSACTION_STATE,
        },
        frontend::{self, ParseBody},
    },
    tcp,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{stream::SplitStream, Sink, SinkExt, TryStreamExt};
use postguard::Guard;
use std::{collections::BTreeMap, fmt, sync::Arc};
use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, Mutex};

/// Unnamed statement bytes for invalidating prepared statement cache
static UNNAMED_PREPARED_STATEMENT: Bytes = Bytes::from_static(b"");

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Cluster(#[from] cluster::Error),
    #[error(transparent)]
    Connection(#[from] connection::Error),
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Unroutable message type: {0:?}")]
    Unroutable(frontend::Message),
    #[error("Error syncing messages from the backend to the client")]
    Sync,
}

impl From<&Error> for backend::Message {
    fn from(error: &Error) -> Self {
        match error {
            Error::Cluster(error) => Self::from(error),
            Error::Connection(error) => Self::from(error),
            Error::Tcp(error) => Self::from(error),
            Error::Unroutable(..) => Self::ErrorResponse {
                code: FEATURE_NOT_SUPPORTED.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Error,
            },
            Error::Sync => Self::ErrorResponse {
                code: CONNECTION_EXCEPTION.clone(),
                message: error.to_string().into(),
                severity: backend::Severity::Fatal,
            },
        }
    }
}

/// State of the current transaction, along with its error responses
enum TransactionMode<S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
    Connection<S>: Pooled,
    <Connection<S> as Pooled>::Configuration: fmt::Debug,
{
    InProgress {
        connection: PooledObject<Connection<S>>,
    },
    IgnoreTillSync {
        code: Bytes,
        message: Bytes,
        severity: backend::Severity,
        connection: PooledObject<Connection<S>>,
    },
}

impl<S> TransactionMode<S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
    Connection<S>: Pooled,
    <Connection<S> as Pooled>::Configuration: fmt::Debug,
{
    fn into_error(self, code: Bytes, message: Bytes, severity: backend::Severity) -> Self {
        let connection = match self {
            Self::InProgress { connection } => connection,
            Self::IgnoreTillSync { connection, .. } => connection,
        };

        Self::IgnoreTillSync {
            code,
            message,
            severity,
            connection,
        }
    }
}

impl<S> fmt::Debug for TransactionMode<S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
    Connection<S>: Pooled,
    <Connection<S> as Pooled>::Configuration: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InProgress { .. } => formatter.debug_struct("InProgress").finish(),
            Self::IgnoreTillSync {
                code,
                message,
                severity,
                ..
            } => formatter
                .debug_struct("IgnoreTillSync")
                .field("code", &code)
                .field("message", &message)
                .field("severity", &severity)
                .finish(),
        }
    }
}

/// Frontend message router to a single Cluster
pub struct Router<S = SplitStream<tcp::Connection<backend::Codec>>>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin,
    Connection<S>: Pooled + Sink<frontend::Message, Error = connection::Error>,
    <Connection<S> as Pooled>::Configuration: fmt::Debug,
{
    /// Reference to the Cluster where messages should be routed
    cluster: Arc<Cluster<Connection<S>>>,

    /// Message broker for aggregating backend messages from the Cluster
    backend_messages: UnboundedSender<backend::Message>,

    /// Postrust-based Guard for rejecting disallowed statements
    statement_guard: Arc<Guard>,

    /// Map of prepared statements names to content hashes for extended query protocol support
    // FIXME: provide DEALLOCATE mechanism (either here or in the cluster through refcounts)
    named_statements: Mutex<BTreeMap<Bytes, ParseBody>>,

    /// An active transaction and its dedicated connection for when the session is in a transaction
    transaction: Mutex<Option<TransactionMode<S>>>,
}

impl<S> Router<S>
where
    S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin + 'static,
    Connection<S>: Pooled + Sink<frontend::Message, Error = connection::Error>,
    <Connection<S> as Pooled>::Configuration: fmt::Debug,
    <Connection<S> as Pooled>::Error: Into<cluster::Error> + fmt::Display,
{
    /// Initialize a new Router
    pub fn new(
        cluster: Arc<Cluster<Connection<S>>>,
        backend_messages: UnboundedSender<backend::Message>,
        statement_guard: Arc<Guard>,
    ) -> Self {
        Self {
            cluster,
            backend_messages,
            statement_guard,
            named_statements: Mutex::new(BTreeMap::new()),
            transaction: Mutex::new(None),
        }
    }

    /// Route a single frontend message body to the correct proxied connection
    #[tracing::instrument(skip(self))]
    pub async fn route(&self, message: frontend::Message) -> Result<(), Error> {
        let transaction = self.transaction.lock().await;

        tracing::trace!(?transaction, "Routing message");

        // ignore messages other than SYNC when the transaction is in ignore_till_sync mode
        if matches!(*transaction, Some(TransactionMode::IgnoreTillSync { .. }))
            && !matches!(&message, frontend::Message::Sync)
        {
            tracing::trace!("Skipping message in ignore_till_sync transaction");

            return Ok(());
        }

        drop(transaction);

        // guard against invalid message types
        // FIXME: handle an explicit subset of messages by this point
        if !matches!(
            &message,
            frontend::Message::Parse(..)
                | frontend::Message::Query(..)
                | frontend::Message::Bind(..)
                | frontend::Message::Close(..)
                | frontend::Message::Execute(..)
                | frontend::Message::Describe(..)
                | frontend::Message::Sync
                | frontend::Message::Flush
        ) {
            return Err(Error::Unroutable(message));
        }

        // guard message types and their payloads
        if let Err(error) = message.guard(&self.statement_guard) {
            tracing::warn!(reason = %error, "Statement rejected");

            let code = INSUFFICIENT_PRIVILEGE.clone();
            let message = error.to_string().into();
            let severity = backend::Severity::Error;
            let mut transaction = self.transaction.lock().await;

            match transaction.take() {
                Some(transaction_mode) => {
                    // put the transaction into an error state
                    *transaction = Some(transaction_mode.into_error(code, message, severity));
                }
                None => {
                    // send back an error response directly
                    self.backend_messages
                        .send(backend::Message::ErrorResponse {
                            code,
                            message,
                            severity,
                        })
                        .map_err(|_| Error::Sync)?;

                    // return a ReadyForQuery message
                    self.backend_messages
                        .send(backend::Message::ReadyForQuery {
                            transaction_status: backend::TransactionStatus::Idle,
                        })
                        .map_err(|_| Error::Sync)?;
                }
            }

            return Ok(());
        };

        // FIXME: implement this method just for ParseBody and QueryBody (right?)
        let is_read_only = message.is_read_only();

        // handle each message type as needed
        match message {
            frontend::Message::Parse(mut body) => {
                // check that the statement doesn't already exist (skipping unnamed statement)
                let client_name = body.name();

                if !client_name.is_empty() {
                    let named_statements = self.named_statements.lock().await;

                    if named_statements.get(&client_name).is_some() {
                        let code = DUPLICATE_PREPARED_STATEMENT.clone();
                        let name = String::from_utf8_lossy(&client_name);
                        let message =
                            format!(r#"Prepared statement "{name}" already exists"#).into();

                        // put the session into an ignore_till_sync transaction
                        let mut transaction = self.transaction.lock().await;

                        match transaction.take() {
                            Some(transaction_mode) => {
                                *transaction = Some(transaction_mode.into_error(
                                    code,
                                    message,
                                    backend::Severity::Error,
                                ));
                            }
                            None => {
                                let connection = self.cluster.follower().await?;

                                *transaction = Some(TransactionMode::IgnoreTillSync {
                                    code,
                                    message,
                                    connection,
                                    severity: backend::Severity::Error,
                                });
                            }
                        }

                        return Ok(());
                    };
                }

                // hash the parse body components to get a new statement name
                let hash = body.hash();
                body.name = hash;

                // store the parse message body for later
                let mut named_statements = self.named_statements.lock().await;

                named_statements.insert(client_name, body);
            }
            frontend::Message::Bind(mut body) => {
                // get the statement name to BIND to
                let client_statement = body.statement();
                let named_statements = self.named_statements.lock().await;
                let parse_body = match named_statements.get(&client_statement) {
                    Some(parse_body) => {
                        let parse_body = parse_body.clone();
                        drop(named_statements);
                        parse_body
                    }
                    None => {
                        let code = INVALID_SQL_STATEMENT_NAME.clone();
                        let statement = String::from_utf8_lossy(&client_statement);
                        let message =
                            format!(r#"Prepared statement "{statement}" not found"#).into();

                        drop(named_statements);

                        // put the session into an ignore_till_sync transaction
                        let mut transaction = self.transaction.lock().await;

                        match transaction.take() {
                            Some(transaction_mode) => {
                                *transaction = Some(transaction_mode.into_error(
                                    code,
                                    message,
                                    backend::Severity::Error,
                                ));
                            }
                            None => {
                                let connection = self.cluster.follower().await?;
                                *transaction = Some(TransactionMode::IgnoreTillSync {
                                    code,
                                    message,
                                    connection,
                                    severity: backend::Severity::Error,
                                });
                            }
                        }

                        return Ok(());
                    }
                };

                // prepare the cached Parse message
                let statement = parse_body.name();
                let parse_message = frontend::Message::Parse(parse_body);

                // extract a connection from the transaction state
                let mut transaction = self.transaction.lock().await;

                let mut connection = match transaction.take() {
                    Some(transaction_mode) => {
                        if let TransactionMode::InProgress { connection } = transaction_mode {
                            // forward the connection from transactions in progress
                            connection
                        } else {
                            // ignore this message if a transaction is ignore_till_sync
                            *transaction = Some(transaction_mode);

                            return Ok(());
                        }
                    }
                    None => {
                        // route to the proper connection based on the parse message
                        if parse_message.is_read_only() {
                            self.cluster.follower().await?
                        } else {
                            self.cluster.leader().await?
                        }
                    }
                };

                // forward a copy of the original PARSE body before BIND
                connection.send(parse_message).await?;

                // modify the BIND message statement
                // FIXME: handle overwrite of unnamed portal
                // (overwritten on Query or end of transaction)
                body.set_statement(statement);

                // forward the modified BIND message
                connection.send(frontend::Message::Bind(body)).await?;
                *transaction = Some(TransactionMode::InProgress { connection });
            }
            frontend::Message::Execute(body) => {
                // verify that EXECUTE is already in a transaction
                let mut transaction = self.transaction.lock().await;

                match transaction.as_mut() {
                    Some(transaction_mode) => {
                        // forward the EXECUTE message
                        if let TransactionMode::InProgress { connection } = transaction_mode {
                            connection.send(frontend::Message::Execute(body)).await?;
                        }
                    }
                    None => {
                        // let the user know about the invalid transaction state
                        self.backend_messages
                            .send(backend::Message::ErrorResponse {
                                code: INVALID_TRANSACTION_STATE.clone(),
                                message: "Invalid transaction state".into(),
                                severity: backend::Severity::Error,
                            })
                            .map_err(|_| Error::Sync)?;
                    }
                }
            }
            frontend::Message::Describe(frontend::DescribeBody::Portal { name }) => {
                // verify that DESCRIBE is already in a transaction
                let mut transaction = self.transaction.lock().await;

                match transaction.as_mut() {
                    Some(transaction_mode) => {
                        // forward the DESCRIBE message
                        if let TransactionMode::InProgress { connection } = transaction_mode {
                            connection
                                .send(frontend::Message::Describe(
                                    frontend::DescribeBody::Portal { name },
                                ))
                                .await?;
                        }
                    }
                    None => {
                        // let the user know about the invalid transaction state
                        self.backend_messages
                            .send(backend::Message::ErrorResponse {
                                code: INVALID_TRANSACTION_STATE.clone(),
                                message: "Invalid transaction state".into(),
                                severity: backend::Severity::Error,
                            })
                            .map_err(|_| Error::Sync)?;
                    }
                }
            }
            frontend::Message::Describe(frontend::DescribeBody::Statement {
                name: client_statement,
            }) => {
                // get the statement name to DESCRIBE
                let named_statements = self.named_statements.lock().await;
                let parse_body = match named_statements.get(&client_statement) {
                    Some(parse_body) => {
                        let parse_body = parse_body.clone();
                        drop(named_statements);
                        parse_body
                    }
                    None => {
                        let code = INVALID_SQL_STATEMENT_NAME.clone();
                        let statement = String::from_utf8_lossy(&client_statement);
                        let message =
                            format!(r#"Prepared statement "{statement}" not found"#).into();

                        drop(named_statements);

                        // put the session into an ignore_till_sync transaction
                        let mut transaction = self.transaction.lock().await;

                        match transaction.take() {
                            Some(transaction_mode) => {
                                *transaction = Some(transaction_mode.into_error(
                                    code,
                                    message,
                                    backend::Severity::Error,
                                ));
                            }
                            None => {
                                let connection = self.cluster.follower().await?;
                                *transaction = Some(TransactionMode::IgnoreTillSync {
                                    code,
                                    message,
                                    connection,
                                    severity: backend::Severity::Error,
                                });
                            }
                        }

                        return Ok(());
                    }
                };

                // prepare the cached Parse message
                let statement = parse_body.name();
                let parse_message = frontend::Message::Parse(parse_body);

                // extract a connection from the transaction state
                let mut transaction = self.transaction.lock().await;

                let mut connection = match transaction.take() {
                    Some(transaction_mode) => {
                        if let TransactionMode::InProgress { connection } = transaction_mode {
                            // forward the connection from transactions in progress
                            connection
                        } else {
                            // ignore this message if a transaction is ignore_till_sync
                            *transaction = Some(transaction_mode);

                            return Ok(());
                        }
                    }
                    None => {
                        // route to the proper connection based on the parse message
                        if parse_message.is_read_only() {
                            self.cluster.follower().await?
                        } else {
                            self.cluster.leader().await?
                        }
                    }
                };

                // forward a copy of the original PARSE body before DESCRIBE
                connection.send(parse_message).await?;

                // forward the modified DESCRIBE message
                // FIXME: handle overwrite of unnamed portal
                // (overwritten on Query or end of transaction)
                connection
                    .send(frontend::Message::Describe(
                        frontend::DescribeBody::new_statement(statement),
                    ))
                    .await?;
            }
            frontend::Message::Sync => {
                // verify that SYNC is already in a transaction
                let mut transaction = self.transaction.lock().await;

                if let Some(transaction_mode) = transaction.take() {
                    match transaction_mode {
                        TransactionMode::InProgress { mut connection } => {
                            // flush the transaction output on SYNC
                            connection.send(frontend::Message::Sync).await?;

                            let mut sync = connection.sync().await?;

                            while let Some(message) = sync.try_next().await? {
                                self.backend_messages
                                    .send(message)
                                    .map_err(|_| Error::Sync)?;
                            }

                            // keep the transaction state if needed
                            if sync.transaction_status()
                                == Some(backend::TransactionStatus::Transaction)
                            {
                                tracing::debug!("Maintaining transaction lock after SYNC");

                                *transaction = Some(TransactionMode::InProgress { connection });
                            }
                        }
                        TransactionMode::IgnoreTillSync {
                            code,
                            message,
                            mut connection,
                            severity,
                        } => {
                            // flush the transaction output on SYNC
                            connection.send(frontend::Message::Sync).await?;

                            let mut transaction = connection.transaction().await?;

                            while let Some(message) = transaction.try_next().await? {
                                self.backend_messages
                                    .send(message)
                                    .map_err(|_| Error::Sync)?;
                            }

                            // send back error responses from session errors
                            self.backend_messages
                                .send(backend::Message::ErrorResponse {
                                    code,
                                    message,
                                    severity,
                                })
                                .map_err(|_| Error::Sync)?;
                        }
                    }
                }

                // let the frontend know that another transaction can be started
                self.backend_messages
                    .send(backend::Message::ReadyForQuery {
                        transaction_status: backend::TransactionStatus::Idle,
                    })
                    .map_err(|_| Error::Sync)?;
            }
            frontend::Message::Flush => {
                let mut transaction = self.transaction.lock().await;

                if let Some(TransactionMode::InProgress { connection }) = transaction.as_mut() {
                    connection.send(frontend::Message::Flush).await?;

                    // flush the output up to the next pending point
                    let mut flush = connection.flush().await?;

                    while let Some(message) = flush.try_next().await? {
                        self.backend_messages
                            .send(message)
                            .map_err(|_| Error::Sync)?;
                    }
                }
            }
            frontend::Message::Query(body) => {
                // reset the unnamed prepared statement if one exists
                let mut named_statements = self.named_statements.lock().await;

                named_statements.remove(&UNNAMED_PREPARED_STATEMENT);

                drop(named_statements);

                // route to the proper connection based on the message type
                let mut connection = if is_read_only {
                    self.cluster.follower().await?
                } else {
                    self.cluster.leader().await?
                };

                // proxy the entire simple query transaction directly
                connection.send(frontend::Message::Query(body)).await?;

                // FIXME: concatenate these streams across subsequent Query messages
                // when not inside a transaction (since response order must be maintained!)
                // i.e. turn these transaction streams into a queue
                // FIXME: process the queue concurrently until the first non-read-only query
                let mut backend_messages = connection.transaction().await?;

                while let Some(message) = backend_messages.try_next().await? {
                    self.backend_messages
                        .send(message)
                        .map_err(|_| Error::Sync)?;
                }
            }
            _ => todo!("Implement handlers for this message type"),
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::Router;
    use crate::{
        cluster::{self, Cluster},
        connection::Connection,
        pool::Pooled,
        protocol::{
            backend::{self, TransactionStatus},
            errors::{DUPLICATE_PREPARED_STATEMENT, INVALID_TRANSACTION_STATE},
            frontend::{self, BindBody, DescribeBody, ExecuteBody, ParseBody, QueryBody},
        },
        tcp,
    };
    use bytes::Bytes;
    use futures_core::Stream;
    use futures_util::{future, stream, StreamExt};
    use postguard::Guard;
    use std::sync::Arc;
    use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver};
    use tokio_stream::wrappers::UnboundedReceiverStream;

    /// Create a mocked Router from a single Connection and its message channels
    fn mock_router<S>(
        messages: S,
    ) -> (
        Router<S>,
        UnboundedReceiver<frontend::Message>,
        UnboundedReceiver<backend::Message>,
    )
    where
        S: Stream<Item = Result<backend::Message, tcp::Error>> + Unpin + 'static,
        Connection<S>: Pooled,
        <Connection<S> as Pooled>::Configuration: Default + std::fmt::Debug,
        <Connection<S> as Pooled>::Error:
            Into<cluster::Error> + std::fmt::Display + std::fmt::Debug,
    {
        // create a mock connection
        let (frontend_sink, frontend_stream) = tokio::sync::mpsc::unbounded_channel();
        let connection = Connection::test(messages, frontend_sink);

        // create a mock router
        let cluster = Cluster::test(connection)
            .map(Arc::new)
            .expect("Error creating test Cluster");
        let (transmitter, receiver) = tokio::sync::mpsc::unbounded_channel();
        let guard = Arc::new(Guard::default());
        let router = Router::new(cluster, transmitter, guard);

        (router, frontend_stream, receiver)
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn rejects_duplicate_named_prepared_statements() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, _frontend_messages, mut backend_messages) = mock_router(messages);

        // route an identical PARSE messages with a name
        let body = ParseBody::new(
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"select $1"),
            vec![],
        );

        let message = frontend::Message::Parse(body);

        router
            .route(message.clone())
            .await
            .expect("Error routing valid PARSE message");

        // route an identical PARSE message
        router
            .route(message)
            .await
            .expect("Error routing valid PARSE message");

        router
            .route(frontend::Message::Sync)
            .await
            .expect("Error routing valid SYNC message");

        // expect a DUPLICATE_PREPARED_STATEMENT error
        match backend_messages
            .recv()
            .await
            .expect("Failed to receive message after duplicate PARSE")
        {
            backend::Message::ErrorResponse { code, .. } => assert_eq!(
                code, DUPLICATE_PREPARED_STATEMENT,
                "Incorrect error code in ErrorResponse"
            ),
            message => panic!("Expected ErrorResponse, found {message:?}"),
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handles_parse_bind_mappings() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, mut frontend_messages, _backend_messages) = mock_router(messages);

        // route a PARSE messages with a name
        let name = Bytes::from_static(b"foo");
        let statement = Bytes::from_static(b"select $1");
        let parse_body = ParseBody::new(name.clone(), statement, vec![]);
        let parse_message = frontend::Message::Parse(parse_body.clone());

        router
            .route(parse_message)
            .await
            .expect("Error routing valid PARSE message");

        // expect that no PARSE message was sent upstream
        let next_message_error = frontend_messages
            .try_recv()
            .expect_err("Expected a TryRecvError, but there was a message");

        assert_eq!(
            next_message_error,
            TryRecvError::Empty,
            "Expected TryRecvError::Empty, but found {next_message_error:?}"
        );

        // route a BIND message bound to the prepared statement
        let portal = Bytes::from_static(b"bar");
        let bind_body = BindBody::new(portal, name.clone());
        let bind_message = frontend::Message::Bind(bind_body.clone());

        router
            .route(bind_message.clone())
            .await
            .expect("Error routing valid BIND message");

        // expect PARSE and BIND message sent upstream together
        let next_message = frontend_messages
            .recv()
            .await
            .expect("Parse message not proxied");

        let hashed_statement_name = parse_body.hash();
        let mut expected = parse_body;
        expected.name = hashed_statement_name.clone();
        let parse_message = frontend::Message::Parse(expected);

        assert_eq!(
            next_message, parse_message,
            "Expected a proxied Parse message, but found {next_message:?}"
        );

        let mut expected = bind_body;
        expected.set_statement(hashed_statement_name);
        let bind_message = frontend::Message::Bind(expected);

        let next_message = frontend_messages
            .recv()
            .await
            .expect("Bind message not proxied");

        assert_eq!(
            next_message, bind_message,
            "Expected a proxied Bind message, but found {next_message:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handles_parse_describe_mappings() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, mut frontend_messages, mut backend_messages) = mock_router(messages);

        // route a PARSE messages with a name
        let name = Bytes::from_static(b"foo");
        let statement = Bytes::from_static(b"select $1");
        let parse_body = ParseBody::new(name.clone(), statement, vec![]);
        let parse_message = frontend::Message::Parse(parse_body.clone());

        router
            .route(parse_message)
            .await
            .expect("Error routing valid PARSE message");

        // expect that no PARSE message was sent upstream
        let next_message_error = frontend_messages
            .try_recv()
            .expect_err("Expected a TryRecvError, but there was a message");

        assert_eq!(
            next_message_error,
            TryRecvError::Empty,
            "Expected TryRecvError::Empty, but found {next_message_error:?}"
        );

        // route a DESCRIBE message bound to the prepared statement
        let describe_body = DescribeBody::new_statement(name);
        let describe_message = frontend::Message::Describe(describe_body);

        router
            .route(describe_message.clone())
            .await
            .expect("Error routing valid DESCRIBE message");

        // expect PARSE and DESCRIBE messages to be sent upstream together
        let next_message = frontend_messages
            .recv()
            .await
            .expect("Parse message not proxied");

        let hashed_statement_name = parse_body.hash();
        let mut expected = parse_body;
        expected.name = hashed_statement_name.clone();
        let parse_message = frontend::Message::Parse(expected);

        assert_eq!(
            next_message, parse_message,
            "Expected a proxied Parse message, but found {next_message:?}"
        );

        let describe_message =
            frontend::Message::Describe(DescribeBody::new_statement(hashed_statement_name));

        let next_message = frontend_messages
            .recv()
            .await
            .expect("Describe message not proxied");

        assert_eq!(
            next_message, describe_message,
            "Expected a proxied Describe message, but found {next_message:?}"
        );

        // verify that there were no errors after SYNC
        router
            .route(frontend::Message::Sync)
            .await
            .expect("Error routing valid SYNC message");

        let last_message = backend_messages
            .recv()
            .await
            .expect("Error fetching the last message after SYNC");

        assert_eq!(
            last_message,
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle
            },
            "Expected a ReadyForQuery message, but found {last_message:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handles_bound_execute_mappings() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, frontend_messages, _backend_messages) = mock_router(messages);
        let frontend_stream = UnboundedReceiverStream::from(frontend_messages);

        // route a PARSE messages with a name
        let name = Bytes::from_static(b"foo");
        let statement = Bytes::from_static(b"select $1");
        let parse_body = ParseBody::new(name.clone(), statement, vec![]);
        let parse_message = frontend::Message::Parse(parse_body);

        router
            .route(parse_message)
            .await
            .expect("Error routing valid PARSE message");

        // route a BIND message bound to the prepared statement
        let portal = Bytes::from_static(b"bar");
        let bind_body = BindBody::new(portal.clone(), name);
        let bind_message = frontend::Message::Bind(bind_body);

        router
            .route(bind_message)
            .await
            .expect("Error routing valid BIND message");

        // route an EXECUTE message to the bound portal
        let execute_body = ExecuteBody::new(portal);
        let execute_message = frontend::Message::Execute(execute_body);

        router
            .route(execute_message.clone())
            .await
            .expect("Error routing valid EXECUTE message");

        // expect the EXECUTE message to be sent to the Connection successfully
        let third_message = frontend_stream
            .skip(2)
            .next()
            .await
            .expect("Parse message not proxied");

        assert_eq!(
            third_message, execute_message,
            "Expected proxied EXECUTE message, but found {third_message:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handles_bound_describe_mappings() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, frontend_messages, _backend_messages) = mock_router(messages);
        let frontend_stream = UnboundedReceiverStream::from(frontend_messages);

        // route a PARSE messages with a name
        let name = Bytes::from_static(b"foo");
        let statement = Bytes::from_static(b"select $1");
        let parse_body = ParseBody::new(name.clone(), statement, vec![]);
        let parse_message = frontend::Message::Parse(parse_body);

        router
            .route(parse_message)
            .await
            .expect("Error routing valid PARSE message");

        // route a BIND message bound to the prepared statement
        let portal = Bytes::from_static(b"bar");
        let bind_body = BindBody::new(portal.clone(), name);
        let bind_message = frontend::Message::Bind(bind_body);

        router
            .route(bind_message)
            .await
            .expect("Error routing valid BIND message");

        // route a DESCRIBE message to the bound portal
        let describe_body = DescribeBody::new_portal(portal);
        let describe_message = frontend::Message::Describe(describe_body);

        router
            .route(describe_message.clone())
            .await
            .expect("Error routing valid DESCRIBE message");

        // expect the DESCRIBE message to be sent to the Connection successfully
        let third_message = frontend_stream
            .skip(2)
            .next()
            .await
            .expect("Parse message not proxied");

        assert_eq!(
            third_message, describe_message,
            "Expected proxied DESCRIBE message, but found {third_message:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handles_duplicate_syncs() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, _frontend_messages, mut backend_messages) = mock_router(messages);

        // route duplicate SYNC messages
        router
            .route(frontend::Message::Sync)
            .await
            .expect("Error routing valid SYNC message");

        router
            .route(frontend::Message::Sync)
            .await
            .expect("Error routing valid SYNC message");

        // expect two ReadyForQuery responses (even without active transactions)
        let first_transaction_message = backend_messages
            .recv()
            .await
            .expect("Error fetching first message from first SYNC");

        assert_eq!(
            first_transaction_message,
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle
            },
            "Expected ReadyForQuery after Sync, was {first_transaction_message:?}"
        );

        let second_transaction_message = backend_messages
            .recv()
            .await
            .expect("Error fetching second message from second SYNC");

        assert_eq!(
            second_transaction_message,
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle
            },
            "Expected ReadyForQuery after Sync, was {second_transaction_message:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn frames_simple_queries_as_transactions() {
        // create a mock router
        let transaction_message = backend::Message::Forward(Bytes::from_static(b"foo"));

        let messages = stream::iter([
            Ok::<_, tcp::Error>(backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            }),
            Ok::<_, tcp::Error>(transaction_message.clone()),
            Ok::<_, tcp::Error>(backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            }),
            Ok::<_, tcp::Error>(backend::Message::Forward(Bytes::from_static(b"bar"))),
        ])
        .boxed();

        let (router, _frontend_messages, mut backend_messages) = mock_router(messages);

        // route a QUERY message
        let message = frontend::Message::Query(QueryBody::new(Bytes::from_static(b"select 'foo'")));

        router
            .route(message)
            .await
            .expect("Error routing valid QUERY message");

        // expect the entire transaction (up to ReadyForQuery) to be proxied
        let first_message = backend_messages
            .recv()
            .await
            .expect("Error fetching the first message of a QUERY transaction");

        assert_eq!(
            first_message, transaction_message,
            "Expected transaction message, was {transaction_message:?}"
        );

        let second_message = backend_messages
            .recv()
            .await
            .expect("Error fetching last message of a QUERY transaction");

        assert_eq!(
            second_message,
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle
            },
            "Expected ReadyForQuery after Query, was {second_message:?}"
        );

        // expect that additional messages are witheld
        let next_message_error = backend_messages
            .try_recv()
            .expect_err("Expected a TryRecvError, but there was a message");

        assert_eq!(
            next_message_error,
            TryRecvError::Empty,
            "Expected TryRecvError::Empty, but found {next_message_error:?}"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn rejects_transactional_messages_without_transaction() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, _frontend_messages, mut backend_messages) = mock_router(messages);

        // route an EXECUTE message
        let execute_message =
            frontend::Message::Execute(ExecuteBody::new(Bytes::from_static(b"foo")));

        router
            .route(execute_message)
            .await
            .expect("Error routing valid EXECUTE message");

        // expect a transaction state error
        match backend_messages
            .recv()
            .await
            .expect("Error fetching invalid transaction message after EXECUTE")
        {
            backend::Message::ErrorResponse { code, .. } => {
                assert_eq!(
                    code, INVALID_TRANSACTION_STATE,
                    "Incorrect error code in ErrorResponse"
                )
            }
            message => panic!("Expected ErrorResponse, found {message:?}"),
        }

        // route a portal-flavored DESCRIBE message
        let describe_message =
            frontend::Message::Describe(DescribeBody::new_portal(Bytes::from_static(b"bar")));

        router
            .route(describe_message)
            .await
            .expect("Error routing valid DESCRIBE message");

        // expect a transaction state error
        match backend_messages
            .recv()
            .await
            .expect("Error fetching invalid transaction message after DESCRIBE")
        {
            backend::Message::ErrorResponse { code, .. } => {
                assert_eq!(
                    code, INVALID_TRANSACTION_STATE,
                    "Incorrect error code in ErrorResponse"
                )
            }
            message => panic!("Expected ErrorResponse, found {message:?}"),
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn ignores_messages_until_sync_after_transaction_error() {
        // create a mock router
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        let (router, mut frontend_messages, mut backend_messages) = mock_router(messages);

        // cause an error by calling EXECUTE on nonexistant portal
        router
            .route(frontend::Message::Execute(ExecuteBody::new(
                Bytes::from_static(b"does not exist"),
            )))
            .await
            .expect("Error routing valid EXECUTE message");

        // route otherwise valid PARSE and BIND messages
        let parse_body = ParseBody::new(
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"select $1"),
            vec![],
        );

        router
            .route(frontend::Message::Parse(parse_body.clone()))
            .await
            .expect("Error routing valid PARSE message");

        router
            .route(frontend::Message::Bind(BindBody::new(
                Bytes::from_static(b"portal"),
                parse_body.hash(),
            )))
            .await
            .expect("Error routing valid BIND message");

        // verify that no messages are actually proxied
        let next_message_error = frontend_messages
            .try_recv()
            .expect_err("Expected a TryRecvError, but there was a message");

        assert_eq!(
            next_message_error,
            TryRecvError::Empty,
            "Expected TryRecvError::Empty, but found {next_message_error:?}"
        );

        // returns original error after SYNC
        router
            .route(frontend::Message::Sync)
            .await
            .expect("Error routing valid SYNC message");

        match backend_messages
            .recv()
            .await
            .expect("Error fetching transaction error after SYNC")
        {
            backend::Message::ErrorResponse { code, .. } => {
                assert_eq!(
                    code, INVALID_TRANSACTION_STATE,
                    "Incorrect error code in ErrorResponse"
                )
            }
            message => panic!("Expected ErrorResponse, found {message:?}"),
        }
    }

    // TOTEST:
    // 1. concurrent Queries are processed in-order (and values returned in-order)
    // 2. BEGIN and COMMIT are handled in simple Query
    // 3. BEGIN and COMMIT are handled in extended query protocol
    // 4. non-transactional messages are rejected inside of a query
    // 5. reads are concurrently processed within a transaction until the first write
    // 6. post-write messages are processed on a single connection
    // 7. all inter-transaction backend messages are returned in-order
}
