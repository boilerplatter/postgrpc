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
use parking_lot::Mutex;
use postguard::Guard;
use std::{
    collections::{btree_map::Entry, hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

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
    #[error("Error syncing messages between connections")]
    Sync,
}

// FIXME: replace magic error codes with something in the protocol module

/// State of the current transaction, along with its error responses
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
    // FIXME: see if this should be a RwLock instead of a Mutex
    named_statements: Mutex<BTreeMap<Bytes, (u64, ParseBody)>>,
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
        // ignore messages other than SYNC when the transaction is in ignore_till_sync mode
        let mut transaction = self.transaction.lock();

        if let Some(TransactionMode::IgnoreTillSync {
            code,
            message: error_message,
            severity,
            mut connection,
        }) = transaction.take()
        {
            if matches!(&message, frontend::Message::Sync) {
                // flush the transaction output on SYNC
                while let Some(message) = connection.try_next().await? {
                    self.backend_messages
                        .send(message)
                        .map_err(|_| Error::Sync)?;
                }

                // send back the error response that caused the transaction failure
                self.backend_messages
                    .send(backend::Message::ErrorResponse {
                        code,
                        message: error_message,
                        severity,
                    })
                    .map_err(|_| Error::Sync)?;

                // let the frontend know that another transaction can be started
                self.backend_messages
                    .send(backend::Message::ReadyForQuery {
                        transaction_status: backend::TransactionStatus::Idle,
                    })
                    .map_err(|_| Error::Sync)?;

                // reset the internal transaction state
                *transaction = None;
            } else {
                // continue ignoring until the next Sync
                *transaction = Some(TransactionMode::IgnoreTillSync {
                    code,
                    message: error_message,
                    severity,
                    connection,
                });
            }

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
                let mut transaction = self.transaction.lock();

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

        // route to the proper connection based on the message type
        let mut connection = if message.is_read_only() {
            self.cluster.follower().await?
        } else {
            self.cluster.leader().await?
        };

        // handle each message type as needed
        match message {
            frontend::Message::Parse(body) => {
                // create a transaction if one doesn't already exist
                let mut transaction = self.transaction.lock();

                if transaction.is_none() {
                    *transaction = Some(TransactionMode::InProgress { connection });
                }

                drop(transaction);

                // hash the parse body components to get a new statement name
                let client_name = body.name();
                let parameter_types = body.parameter_types();
                let mut state = DefaultHasher::new();
                statement.hash(&mut state);
                parameter_types.hash(&mut state);
                let hash = state.finish();

                // store the name and hash of the statement if it doesn't already exist
                let mut named_statements = self.named_statements.lock();

                match named_statements.entry(client_name) {
                    Entry::Vacant(entry) => {
                        entry.insert((hash, body));
                    }
                    Entry::Occupied(entry) => {
                        // FIXME: handle the "unnamed statement" case
                        // (which can be overwritten without error)
                        let name = String::from_utf8_lossy(entry.key());
                        let message =
                            format!(r#"Prepared statement "{name}" already exists"#).into();

                        // move the transaction to an error state
                        let mut transaction = self.transaction.lock();

                        if let Some(transaction_mode) = transaction.take() {
                            if let TransactionMode::InProgress { connection } = transaction_mode {
                                let code = "42P05".into();

                                *transaction = Some(TransactionMode::IgnoreTillSync {
                                    code,
                                    message,
                                    severity: backend::Severity::Error,
                                    connection,
                                });
                            } else {
                                *transaction = Some(transaction_mode);
                            }
                        }
                    }
                }
            }
            frontend::Message::Bind(mut body) => {
                // verify that BIND is already in a transaction
                let mut transaction = self.transaction.lock();

                match transaction.take() {
                    Some(mut transaction_mode) => {
                        if let TransactionMode::InProgress { connection } = &mut transaction_mode {
                            // map BIND message statement names to statement hashes
                            let client_statement = body.statement();
                            let named_statements = self.named_statements.lock();
                            match named_statements.get(&client_statement) {
                                Some((statement, parse_body)) => {
                                    let statement =
                                        Bytes::copy_from_slice(&statement.to_be_bytes());
                                    let parse_message =
                                        frontend::Message::Parse(parse_body.clone());

                                    drop(named_statements);

                                    // forward a copy of the original PARSE body before BIND
                                    connection.send(parse_message).await?;

                                    // modify the BIND message statement
                                    // FIXME: handle the "unnamed portal" case
                                    // (which is overwritten on every Query or at the end of a transaction)
                                    body.set_statement(statement);

                                    // forward the modified BIND message
                                    connection.send(frontend::Message::Bind(body)).await?;
                                    *transaction = Some(transaction_mode);
                                }
                                None => {
                                    let code = "26000".into();
                                    let statement = String::from_utf8_lossy(&client_statement);
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
            frontend::Message::Execute(body) => {
                // verify that BIND is already in a transaction
                let mut transaction = self.transaction.lock();

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
            frontend::Message::Query(body) => {
                // FIXME:
                // 1. support EXPLICIT and NESTED transactions
                // 2. support PREPARE and EXECUTE SQL statements in Query bodies
                // 3. check that multiple QUERY messages don't result in interleaved results
                // (i.e. order of responses is maintained, even between transactions!)

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
