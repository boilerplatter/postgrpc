use crate::{
    cluster::{self, Cluster},
    connection,
    pool::PooledConnection,
    protocol::{
        backend,
        frontend::{self, ParseBody},
    },
    tcp,
};
use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use postguard::Guard;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{mpsc::UnboundedSender, Mutex};

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

/// State of the current transaction, along with its error responses
#[derive(Debug)]
enum TransactionMode {
    InProgress {
        connection: PooledConnection,
    },
    IgnoreTillSync {
        code: Bytes,
        message: Bytes,
        severity: backend::Severity,
        connection: PooledConnection,
    },
}

impl TransactionMode {
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

/// Frontend message router to a single Cluster
pub struct Router {
    /// Reference to the Cluster where messages should be routed
    cluster: Arc<Cluster>,
    /// Message broker for aggregating backend messages from the Cluster
    backend_messages: UnboundedSender<backend::Message>,
    /// Postrust-based Guard for rejecting disallowed statements
    statement_guard: Arc<Guard>,
    /// Map of prepared statements names to content hashes for extended query protocol support
    // FIXME: provide DEALLOCATE mechanism (either here or in the cluster through refcounts)
    named_statements: Mutex<BTreeMap<Bytes, ParseBody>>,
    /// An active transaction and its dedicated connection for when the session is in a transaction
    transaction: Mutex<Option<TransactionMode>>,
}

impl Router {
    /// Initialize a new Router
    pub fn new(
        cluster: Arc<Cluster>,
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
        let statement = match message.guard(&self.statement_guard) {
            Err(error) => {
                tracing::warn!(reason = %error, "Statement rejected");

                let code = "42501".into();
                let message = error.to_string().into();
                let severity = backend::Severity::Error;
                let mut transaction = self.transaction.lock().await;

                match transaction.take() {
                    Some(transaction_mode) => {
                        // put the transaction into an error state
                        *transaction = Some(transaction_mode.into_error(code, message, severity));
                    }
                    None => {
                        // FIXME: encapsulate this as a "frontend reset" function or somesuch
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
            }
            Ok(statement) => statement,
        };

        // FIXME: implement this method just for ParseBody and QueryBody (right?)
        let is_read_only = message.is_read_only();

        // handle each message type as needed
        match message {
            frontend::Message::Parse(mut body) => {
                // check that the statement doesn't already exist
                let client_name = body.name();
                let named_statements = self.named_statements.lock().await;
                if named_statements.get(&client_name).is_some() {
                    // FIXME: handle the "unnamed statement" case
                    // (which can be overwritten without error)
                    let code = "42P05".into();
                    let name = String::from_utf8_lossy(&client_name);
                    let message = format!(r#"Prepared statement "{name}" already exists"#).into();

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

                drop(named_statements);

                // hash the parse body components to get a new statement name
                let parameter_types = body.parameter_types();
                let mut state = DefaultHasher::new();
                statement.hash(&mut state);
                parameter_types.hash(&mut state);
                let hash = state.finish();

                // prepare a Parse message with new statement name
                body.name = hash.to_string().into();
                let parse_message = frontend::Message::Parse(body.clone()); // FIXME: avoid clone

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
                // FIXME: store eagerly in the first check
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
                        let code = "26000".into();
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
                                code: "25000".into(),
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
                                code: "25000".into(),
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
                                    let code = "26000".into();
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
                                code: "25000".into(),
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

                match transaction.take() {
                    Some(transaction_mode) => {
                        match transaction_mode {
                            TransactionMode::InProgress { mut connection } => {
                                // flush the transaction output on SYNC
                                connection.send(frontend::Message::Sync).await?;

                                let mut transaction = connection.transaction().await?;

                                while let Some(message) = transaction.try_next().await? {
                                    self.backend_messages.send(message).map_err(|error| {
                                        tracing::error!(
                                            ?error,
                                            "Failed to send backend message in SYNC"
                                        );

                                        Error::Sync
                                    })?;
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

                        // let the frontend know that another transaction can be started
                        self.backend_messages
                            .send(backend::Message::ReadyForQuery {
                                transaction_status: backend::TransactionStatus::Idle,
                            })
                            .map_err(|_| Error::Sync)?;

                        // reset the internal transaction state
                        *transaction = None;
                    }
                    None => {
                        // let the user know about the invalid transaction state
                        self.backend_messages
                            .send(backend::Message::ErrorResponse {
                                code: "25000".into(),
                                message: "Invalid transaction state".into(),
                                severity: backend::Severity::Error,
                            })
                            .map_err(|_| Error::Sync)?;
                    }
                }
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
                                code: "25000".into(),
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
