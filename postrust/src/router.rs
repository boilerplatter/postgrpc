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
    Connection<S>: Pooled<Error = tcp::Error>,
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
    Connection<S>: Pooled<Error = tcp::Error>,
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
    Connection<S>: Pooled<Error = tcp::Error>,
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
    Connection<S>: Pooled<Error = tcp::Error> + Sink<frontend::Message, Error = connection::Error>,
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
    Connection<S>: Pooled<Error = tcp::Error> + Sink<frontend::Message, Error = connection::Error>,
    <Connection<S> as Pooled>::Configuration: fmt::Debug,
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

                // prepare a Parse message with new statement name
                body.name = hash;
                let parse_message = frontend::Message::Parse(body.clone());

                // initialize a transaction if one doesn't exist
                let mut transaction = self.transaction.lock().await;

                if transaction.is_none() {
                    // route to the proper connection based on the parse message
                    let connection = if parse_message.is_read_only() {
                        self.cluster.follower().await?
                    } else {
                        self.cluster.leader().await?
                    };

                    *transaction = Some(TransactionMode::InProgress { connection });
                }

                drop(transaction);

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
            frontend::Message::Describe(frontend::DescribeBody::Statement { name }) => {
                // verify that DESCRIBE is already in a transaction
                let mut transaction = self.transaction.lock().await;

                match transaction.take() {
                    Some(mut transaction_mode) => {
                        if let TransactionMode::InProgress { connection } = &mut transaction_mode {
                            // check that the statement has been prepared
                            let named_statements = self.named_statements.lock().await;

                            match named_statements.get(&name) {
                                Some(parse_body) => {
                                    let statement = parse_body.name();
                                    let parse_message =
                                        frontend::Message::Parse(parse_body.clone());

                                    drop(named_statements);

                                    // forward a copy of the original PARSE body before DESCRIBE
                                    connection.send(parse_message).await?;

                                    // modify the DESCRIBE message statement
                                    // FIXME: handle the "unnamed portal" case
                                    // (which is overwritten on every Query or at the end of a transaction)
                                    let body =
                                        frontend::DescribeBody::Statement { name: statement };

                                    // forward the modified DESCRIBE message
                                    connection.send(frontend::Message::Describe(body)).await?;
                                    *transaction = Some(transaction_mode);
                                }
                                None => {
                                    let code = INVALID_SQL_STATEMENT_NAME.clone();
                                    let statement = String::from_utf8_lossy(&name);
                                    let message =
                                        format!(r#"Prepared statement "{statement}" not found"#)
                                            .into();

                                    drop(named_statements);

                                    // set the transaction to an error state
                                    *transaction = Some(transaction_mode.into_error(
                                        code,
                                        message,
                                        backend::Severity::Error,
                                    ));
                                }
                            };
                        } else {
                            *transaction = Some(transaction_mode);
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
            frontend::Message::Sync => {
                // verify that SYNC is already in a transaction
                let mut transaction = self.transaction.lock().await;

                if let Some(transaction_mode) = transaction.take() {
                    match transaction_mode {
                        TransactionMode::InProgress { mut connection } => {
                            // flush the transaction output on SYNC
                            connection.send(frontend::Message::Sync).await?;

                            let mut transaction = connection.transaction().await?;

                            while let Some(message) = transaction.try_next().await? {
                                self.backend_messages
                                    .send(message)
                                    .map_err(|_| Error::Sync)?;
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
                // verify that FLUSH is already in a transaction
                let mut transaction = self.transaction.lock().await;

                match transaction.as_mut() {
                    Some(mut transaction_mode) => {
                        if let TransactionMode::InProgress { connection } = &mut transaction_mode {
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
            frontend::Message::Query(body) => {
                // FIXME:
                // 1. support EXPLICIT and NESTED transactions
                // 2. support PREPARE and EXECUTE SQL statements in Query bodies
                // 3. check that multiple QUERY messages don't result in interleaved results
                // (i.e. order of responses is maintained, even between transactions!)
                // if this is a queue, consider removing the concurrency from message parsing, too

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
        cluster::Cluster,
        connection::Connection,
        protocol::{
            backend::{self, TransactionStatus},
            errors::DUPLICATE_PREPARED_STATEMENT,
            frontend::{self, BindBody, ParseBody},
        },
        tcp,
    };
    use bytes::Bytes;
    use futures_util::{future, stream, StreamExt};
    use postguard::Guard;
    use std::sync::Arc;
    use tokio::sync::mpsc::error::TryRecvError;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn rejects_duplicate_named_prepared_statements() {
        // create a mock connection
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        // create a mock Connection
        let (frontend_sink, _frontend_stream) = tokio::sync::mpsc::unbounded_channel();
        let connection = Connection::test(messages, frontend_sink);

        // create a mock router
        let cluster = Cluster::test(connection)
            .map(Arc::new)
            .expect("Error creating test Cluster");
        let (transmitter, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let guard = Arc::new(Guard::default());
        let router = Router::new(cluster, transmitter, guard);

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
        match receiver
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
        // handle the startup message stream
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        // create a mock Connection
        let (frontend_sink, mut frontend_stream) = tokio::sync::mpsc::unbounded_channel();
        let connection = Connection::test(messages, frontend_sink);

        // create a mock router
        let cluster = Cluster::test(connection)
            .map(Arc::new)
            .expect("Error creating test Cluster");
        let (transmitter, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let guard = Arc::new(Guard::default());
        let router = Router::new(cluster, transmitter, guard);

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
        let next_message_error = frontend_stream
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
        let next_message = frontend_stream
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

        let next_message = frontend_stream
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
    async fn handles_duplicate_syncs() {
        // handle the startup message stream
        let messages = stream::once(future::ok::<_, tcp::Error>(
            backend::Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle,
            },
        ))
        .boxed();

        // create a mock connection
        let (frontend_sink, _frontend_stream) = tokio::sync::mpsc::unbounded_channel();
        let connection = Connection::test(messages, frontend_sink);

        // create a mock router
        let cluster = Cluster::test(connection)
            .map(Arc::new)
            .expect("Error creating test Cluster");
        let (transmitter, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let guard = Arc::new(Guard::default());
        let router = Router::new(cluster, transmitter, guard);

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
        let first_transaction_message = receiver
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

        let second_transaction_message = receiver
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
}
