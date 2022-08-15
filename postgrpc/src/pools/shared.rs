/*!
A Postgres connection pool built on `postgres-pool` and `deadpool_postgres` that is meant for
JSON-based querying from remote sources. This pool initiates connections from a single user,
then uses a Key that maps to a Postgres `ROLE` to `SET LOCAL ROLE` before each connection is used.
In addition, this pool limits inputs to a scalar `Parameter` subset of valid JSON values,
returning rows as a stream of JSON Objects.
!*/
use arc_swap::ArcSwap;
use super::{Connection, Parameter};
use futures_util::{ready, Stream};
use pin_project_lite::pin_project;
use serde::{de::Error as _, Deserialize, Deserializer};
use std::{
    sync::Arc,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio_postgres::{config::SslMode, RowStream};
use tonic::{async_trait, Status};
#[cfg(feature = "ssl-native-tls")]
use {native_tls::TlsConnector, postgres_native_tls::MakeTlsConnector};

/// Errors related to pooling or running queries against the Postgres database
#[derive(Debug, Error)]
pub enum Error {
    /// Parameters did not match the number of parameters inferred by statement preparation
    #[error("Expected {expected} parameters but found {actual} instead")]
    Params {
        /// number of expected params
        expected: usize,
        /// number of actual params in the request after validation
        actual: usize,
    },
    /// Bubbled-up `tokio_postgres` SQL-level errors within a connection
    #[error("SQL Query error: {0}")]
    Query(#[from] tokio_postgres::Error),
    /// JSON-formatted rows could not be properly converted between Postgres' built-in `to_json()` output and
    /// `serde_json::Value`. If this error occurs, it is because of an AS-induced name collision!
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
    /// Could not establish a secure, SSL connection
    #[cfg(feature = "ssl-native-tls")]
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] native_tls::Error),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        Status::invalid_argument(error.to_string())
    }
}

/// Shared connection "pool" implementation pointing to a single database over a single connection.
// this pool only supports binary encoding, so all non-JSON types must be hinted at in the query
pub struct Pool {
    client: ArcSwap<tokio_postgres::Client>,
    configuration: Configuration,
}

#[async_trait]
impl super::Pool for Pool {
    type Key = ();
    type Connection = Arc<tokio_postgres::Client>;
    type Error = <Self::Connection as Connection>::Error;

    async fn get_connection(&self, _key: ()) -> Result<Self::Connection, Self::Error> {
        // clean up connection state before handing it off
        if let Err(error) = self.client.load().batch_execute("DISCARD ALL").await {
            if error.is_closed() {
                // recover from closed connections
                let client = self.configuration.create_client().await?;
                client.batch_execute("DISCARD ALL").await?;
                self.client.store(Arc::new(client));
            }
        }

        // set the statement_timeout for the session
        if let Some(statement_timeout) = self.configuration.statement_timeout {
            self.client.load()
                .batch_execute(&format!(
                    "SET statement_timeout={}",
                    statement_timeout.as_millis()
                ))
                .await?;
        }

        Ok(self.client.load_full())
    }
}

pin_project! {
    /// The stream of gRPC-formatted rows returned by this pool's associated connection
    pub struct StructStream {
        #[pin]
        rows: RowStream,
    }
}

impl Stream for StructStream {
    type Item = Result<pbjson_types::Struct, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.rows.poll_next(context)?) {
            Some(row) => {
                if let serde_json::Value::Object(map) = row.try_get("json")? {
                    Poll::Ready(Some(Ok(to_proto_struct(map))))
                } else {
                    Poll::Ready(Some(Err(Error::InvalidJson)))
                }
            }
            None => Poll::Ready(None),
        }
    }
}

impl From<RowStream> for StructStream {
    fn from(rows: RowStream) -> Self {
        Self { rows }
    }
}

#[async_trait]
impl Connection for Arc<tokio_postgres::Client> {
    type Error = Error;
    type RowStream = StructStream;

    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<Self::RowStream, Self::Error> {
        // prepare the statement
        let prepared_statement = self.prepare(statement).await?;

        // check parameter count to avoid panics
        let inferred_types = prepared_statement.params();

        if inferred_types.len() != parameters.len() {
            return Err(Error::Params {
                expected: inferred_types.len(),
                actual: parameters.len(),
            });
        }

    let rows = if prepared_statement.columns().is_empty() {
        // execute statements that return no data without modification
        self
            .query_raw(&prepared_statement, parameters)
            .await?
    } else {
        // wrap queries that return data in to_json()
        let json_statement = format!(
            "WITH cte AS ({})
            SELECT TO_JSON(__result) AS json
            FROM (SELECT * FROM cte) AS __result",
            &statement
        );

        self
            .query_raw(&json_statement, parameters)
            .await?
    };

        Ok(StructStream::from(rows))
    }

    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        self.batch_execute(query).await?;

        Ok(())
    }
}

/// Pool-specific configuration variables
#[derive(Deserialize, Debug)]
pub struct Configuration {
    /// maximum amount of time to wait for a statement to complete (in milliseconds)
    #[serde(default, deserialize_with = "from_milliseconds_string")]
    statement_timeout: Option<Duration>,
    /// Postgres database to connect to
    pgdbname: String,
    /// host to use for database connections
    #[serde(default = "get_localhost")]
    pghost: String,
    /// Password to use for database connections
    pgpassword: String,
    /// Port to use for database connections
    #[serde(default = "get_postgres_port")]
    pgport: u16,
    /// User to use for database connections
    pguser: String,
    /// Application name for Postgres session tracking
    #[serde(default = "get_application_name")]
    pgappname: String,
    /// SSL mode for upstream connections
    #[serde(default = "get_ssl_mode", deserialize_with = "from_sslmode_string")]
    pgsslmode: SslMode,
}

impl Configuration {
    /// Create a Pool from this Configuration
    pub async fn create_pool(self)  -> Result<Pool, Error> {
        let client = self.create_client().await.map(ArcSwap::from_pointee)?;

        Ok(Pool {
            client,
            configuration: self,
        })
    }

    /// Create a Client from this configuration
    async fn create_client(&self)  -> Result<tokio_postgres::Client, Error> {
        // set up TLS connectors
        #[cfg(feature = "ssl-native-tls")]
        let connector = TlsConnector::builder().build()?;
        #[cfg(feature = "ssl-native-tls")]
        let tls_connector = MakeTlsConnector::new(connector);
        #[cfg(not(feature = "ssl-native-tls"))]
        let tls_connector = tokio_postgres::NoTls;

        // configure the underlying connection
        let (client, connection) = tokio_postgres::Config::new()
            .dbname(&self.pgdbname)
            .host(&self.pghost)
            .password(&self.pgpassword)
            .port(self.pgport)
            .user(&self.pguser)
            .application_name(&self.pgappname)
            .ssl_mode(self.pgsslmode)
            .connect(tls_connector)
            .await?;

        // spawn the connection for later
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error);
            }
        });

        Ok(client)
    }
}

/// Generate a default "localhost" host value
fn get_localhost() -> String {
    "localhost".to_string()
}

/// Generate a default port for connecting to the postgres database
fn get_postgres_port() -> u16 {
    5432
}

/// Generate a default application name
fn get_application_name() -> String {
    "postgrpc".to_string()
}

/// Generate a default SSLMODE
fn get_ssl_mode() -> SslMode {
    SslMode::Prefer
}

/// Deserializer for milliseconds, passed through the environment as a string
fn from_milliseconds_string<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let base_string = String::deserialize(deserializer)?;
    if base_string.is_empty() {
        Ok(None)
    } else {
        let parsed_millis: u64 = base_string.parse().map_err(serde::de::Error::custom)?;
        let duration = Duration::from_millis(parsed_millis);

        Ok(Some(duration))
    }
}

/// Deserializer for SSL_MODE, passed through the environment as a string
fn from_sslmode_string<'de, D>(deserializer: D) -> Result<SslMode, D::Error>
where
    D: Deserializer<'de>,
{
    let base_string = String::deserialize(deserializer)?.to_lowercase();

    let ssl_mode = match base_string.as_str() {
        "disable" => SslMode::Disable,
        "prefer" => SslMode::Prefer,
        "require" => SslMode::Require,
        input => return Err(D::Error::custom(format!("{input} is not a valid SSL_MODE"))),
    };

    Ok(ssl_mode)
}

// FIXME: share these helpers where it makes sense
/// Convert a serde_json::Value into a pbjson_types::Value
fn to_proto_value(json: serde_json::Value) -> pbjson_types::Value {
    let kind = match json {
        serde_json::Value::Null => pbjson_types::value::Kind::NullValue(0),
        serde_json::Value::Bool(boolean) => pbjson_types::value::Kind::BoolValue(boolean),
        serde_json::Value::Number(number) => match number.as_f64() {
            Some(number) => pbjson_types::value::Kind::NumberValue(number),
            None => pbjson_types::value::Kind::StringValue(number.to_string()),
        },
        serde_json::Value::String(string) => pbjson_types::value::Kind::StringValue(string),
        serde_json::Value::Array(array) => {
            pbjson_types::value::Kind::ListValue(pbjson_types::ListValue {
                values: array.into_iter().map(to_proto_value).collect(),
            })
        }
        serde_json::Value::Object(map) => {
            pbjson_types::value::Kind::StructValue(to_proto_struct(map))
        }
    };

    pbjson_types::Value { kind: Some(kind) }
}

/// Convert a serde_json::Map into a pbjson_types::Struct
fn to_proto_struct(map: serde_json::Map<String, serde_json::Value>) -> pbjson_types::Struct {
    pbjson_types::Struct {
        fields: map
            .into_iter()
            .map(|(key, value)| (key, to_proto_value(value)))
            .collect(),
    }
}
