//! A [`deadpool_postgres`]-based connection pool implementation.
//!
//! When used in conjunction with the `role-header` feature and interceptor,
//! this pool runs `SET ROLE` on each connection for the `ROLE` specified in the
//! `X-Postgres-Role` header (assuming it exists).
use super::Connection;
use deadpool_postgres::{
    tokio_postgres::{error::SqlState, RowStream, Statement},
    ManagerConfig, PoolConfig,
};
use futures_util::{ready, Stream};
use pbjson_types::Struct;
use pin_project_lite::pin_project;
use serde::{Deserialize, Deserializer};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio_postgres::{
    types::ToSql,
    Row,
};
use tonic::{async_trait, Status};
#[cfg(feature = "ssl-native-tls")]
use {native_tls::TlsConnector, postgres_native_tls::MakeTlsConnector};

/// Errors related to pooling or running queries against the Postgres database
#[derive(Debug, Error)]
pub enum Error {
    /// Parameters did not match the number of parameters inferred by statement preparation.
    #[error("Expected {expected} parameters but found {actual} instead")]
    Params {
        /// number of expected params
        expected: usize,
        /// number of actual params in the request after validation
        actual: usize,
    },
    /// Bubbled-up [`deadpool_postgres`] connection pool errors.
    #[error("Error fetching connection from the pool: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    /// Bubbled-up [`tokio_postgres`] SQL-level errors within a connection.
    #[error("SQL Query error: {0}")]
    Query(#[from] deadpool_postgres::tokio_postgres::Error),
    /// ROLE-setting errors before connections are returned to users.
    #[error("Unable to set the ROLE of the connection before use: {0}")]
    Role(deadpool_postgres::tokio_postgres::Error),
    /// JSON-formatted rows could not be properly converted between Postgres' built-in `to_json()` output and
    /// [`serde_json::Value`]. If this error occurs, it is because of an AS-induced name collision!
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
    /// Bubbled-up configuration errors from the underlying [`deadpool_postgres`] configuration.
    #[error("Error creating the connection pool: {0}")]
    Create(#[from] deadpool_postgres::CreatePoolError),
    /// TLS errors during setup of [`native_tls`] SSL connectors.
    #[cfg(feature = "ssl-native-tls")]
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] native_tls::Error),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = error.to_string();

        match error {
            Error::Params { .. } | Error::Role(..) | Error::Query(..) | Error::InvalidJson => {
                Status::invalid_argument(message)
            }
            Error::Create(..) | Error::Pool(..) => Status::resource_exhausted(message),
            #[cfg(feature = "ssl-native-tls")]
            Error::Tls(..) => Status::internal(message),
        }
    }
}

/// [`deadpool_postgres`]-based `Pool` implementation `Key`ed by Postgres `ROLE`.
pub struct Pool {
    pool: deadpool_postgres::Pool,
    statement_timeout: Option<Duration>,
}

#[allow(unused_variables)]
#[async_trait]
impl super::Pool<Struct> for Pool {
    #[cfg(feature = "role-header")]
    type Key = crate::extensions::role_header::Role;
    #[cfg(not(feature = "role-header"))]
    type Key = ();
    type Connection = Client;
    type Error = <Self::Connection as Connection<Struct>>::Error;

    #[tracing::instrument(skip(self))]
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error> {
        tracing::trace!("Fetching connection from the pool");

        let client = self.pool.get().await?;

        #[cfg(feature = "role-header")]
        {
            // configure the connection's ROLE
            let local_role_statement = match key {
                Some(role) => format!(r#"SET ROLE "{}""#, role),
                None => "RESET ROLE".to_string(),
            };

            client
                .batch_execute(&local_role_statement)
                .await
                .map_err(Error::Role)?;
        }

        // set the statement_timeout for the session
        if let Some(statement_timeout) = self.statement_timeout {
            client
                .batch_execute(&format!(
                    "SET statement_timeout={}",
                    statement_timeout.as_millis()
                ))
                .await?;
        }

        Ok(Client { client })
    }
}

#[allow(unused_variables)]
#[async_trait]
impl super::Pool<Row> for Pool {
    #[cfg(feature = "role-header")]
    type Key = crate::extensions::role_header::Role;
    #[cfg(not(feature = "role-header"))]
    type Key = ();
    type Connection = Client;
    type Error = <Self::Connection as Connection<Row>>::Error;

    #[tracing::instrument(skip(self))]
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error> {
        // FIXME: share this logic across pool implementations
        tracing::trace!("Fetching connection from the pool");

        let client = self.pool.get().await?;

        #[cfg(feature = "role-header")]
        {
            // configure the connection's ROLE
            let local_role_statement = match key {
                Some(role) => format!(r#"SET ROLE "{}""#, role),
                None => "RESET ROLE".to_string(),
            };

            client
                .batch_execute(&local_role_statement)
                .await
                .map_err(Error::Role)?;
        }

        // set the statement_timeout for the session
        if let Some(statement_timeout) = self.statement_timeout {
            client
                .batch_execute(&format!(
                    "SET statement_timeout={}",
                    statement_timeout.as_millis()
                ))
                .await?;
        }

        Ok(Client { client })
    }
}

pin_project! {
    /// The stream of gRPC-compatible JSON rows returned by this pool's [`Client`].
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

/// `Connection`-implementing wrapper around a [`deadpool_postgres::Client`]
pub struct Client {
    client: deadpool_postgres::Client,
}

#[async_trait]
impl Connection<Struct> for Client {
    type RowStream = StructStream;
    type Error = Error;

    #[tracing::instrument(skip(self, parameters))]
    async fn query(
        &self,
        statement: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<Self::RowStream, Self::Error>
    {
        tracing::trace!("Querying Connection");

        // prepare the statement using the statement cache
        let prepared_statement = self.client.prepare_cached(statement).await?;

        // check parameter count to avoid panics
        let inferred_types = prepared_statement.params();
        if inferred_types.len() != parameters.len() {
            return Err(Error::Params {
                expected: inferred_types.len(),
                actual: parameters.len(),
            });
        }

        let rows = match query_raw_json(self, statement, &prepared_statement, parameters)
            .await
        {
            // retry the query if the schema changed underneath the prepared statement cache
            Err(Error::Query(error)) if error.code() == Some(&SqlState::FEATURE_NOT_SUPPORTED) => {
                tracing::warn!("Schema poisoned underneath statement cache. Retrying query");

                self.client
                    .statement_cache
                    .remove(statement, inferred_types);

                query_raw_json(self, statement, &prepared_statement, &parameters).await
            }
            result => result,
        }?;

        Ok(StructStream::from(rows))
    }

    #[tracing::instrument(skip(self))]
    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        tracing::trace!("Executing batch query on Connection");

        self.client.batch_execute(query).await?;

        Ok(())
    }
}

pin_project! {
    /// The stream of raw rows returned by this pool's [`Client`].
    pub struct RawStream {
        #[pin]
        rows: RowStream,
    }
}

impl Stream for RawStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.rows.poll_next(context)?) {
            Some(row) => Poll::Ready(Some(Ok(row))),
            None => Poll::Ready(None),
        }
    }
}

impl From<RowStream> for RawStream {
    fn from(rows: RowStream) -> Self {
        Self { rows }
    }
}

#[async_trait]
impl Connection<Row> for Client {
    type RowStream = RawStream;
    type Error = Error;

    #[tracing::instrument(skip(self, parameters))]
    async fn query(
        &self,
        statement: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<Self::RowStream, Self::Error> {
        tracing::trace!("Querying TypedConnection");

        // prepare the statement using the statement cache
        let prepared_statement = self.client.prepare_cached(statement).await?;

        // check parameter count to avoid panics
        let inferred_types = prepared_statement.params();

        if inferred_types.len() != parameters.len() {
            return Err(Error::Params {
                expected: inferred_types.len(),
                actual: parameters.len(),
            });
        }

        let rows = match self
            .client
            .query_raw(&prepared_statement, parameters.to_vec())
            .await
        {
            // retry the query if the schema changed underneath the prepared statement cache
            Err(error) if error.code() == Some(&SqlState::FEATURE_NOT_SUPPORTED) => {
                tracing::warn!("Schema poisoned underneath statement cache. Retrying query");

                self.client
                    .statement_cache
                    .remove(statement, inferred_types);

                self.client.query_raw(&prepared_statement, parameters.to_vec()).await
            }
            result => result,
        }?;

        Ok(RawStream::from(rows))
    }

    #[tracing::instrument(skip(self))]
    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        tracing::trace!("Executing batch query on Connection");

        self.client.batch_execute(query).await?;

        Ok(())
    }
}

/// Wrapper around a raw JSON query that can be retried
async fn query_raw_json(
    client: &Client,
    statement: &str,
    prepared_statement: &Statement,
    parameters: &[&(dyn ToSql + Sync)],
) -> Result<RowStream, Error> {
    let rows = if prepared_statement.columns().is_empty() {
        // execute statements that return no data without modification
        client
            .client
            .query_raw(prepared_statement, parameters.to_vec())
            .await?
    } else {
        // wrap queries that return data in to_json()
        let json_statement = format!(
            "WITH cte AS ({})
            SELECT TO_JSON(__result) AS json
            FROM (SELECT * FROM cte) AS __result",
            &statement
        );

        let prepared_statement = client.client.prepare_cached(&json_statement).await?;

        client
            .client
            .query_raw(&prepared_statement, parameters.to_vec())
            .await?
    };

    Ok(rows)
}

/// [`deadpool_postgres`]-specific configuration variables, mapped to their [Postgres Environment
/// Variable equivalents](https://www.postgresql.org/docs/current/libpq-envars.html)
#[derive(Deserialize, Debug)]
pub struct Configuration {
    /// maximum size of each connection pool, defaulting to 4x the number of physical CPUs
    #[serde(default = "get_max_connection_pool_size")]
    pub max_connection_pool_size: usize,
    /// maximum amount of time to wait for a statement to complete (in milliseconds)
    #[serde(default, deserialize_with = "from_milliseconds_string")]
    pub statement_timeout: Option<Duration>,
    /// connection recycling method to use when connections are returned to the pool
    #[serde(default)]
    pub recycling_method: RecyclingMethod,
    /// Postgres database to connect to
    pub pgdbname: String,
    /// host to use for database connections
    #[serde(default = "get_localhost")]
    pub pghost: String,
    /// Password to use for database connections
    pub pgpassword: String,
    /// Port to use for database connections
    #[serde(default = "get_postgres_port")]
    pub pgport: u16,
    /// User to use for database connections
    pub pguser: String,
    /// Application name for Postgres session tracking
    #[serde(default = "get_application_name")]
    pub pgappname: String,
    /// SSL mode for upstream connections
    #[serde(default)]
    pub pgsslmode: Option<SslMode>,
}

impl Configuration {
    /// Create a Pool from this Configuration
    #[tracing::instrument(
        skip(self),
        fields(
            max_connection_pool_size = self.max_connection_pool_size,
            ?statement_timeout = self.statement_timeout,
            ?recycling_method = self.recycling_method,
            pgdbname = self.pgdbname,
            pghost = self.pghost,
            pgpassword = "******",
            pgport = self.pgport,
            pgappname = self.pgappname,
            ?pgsslmode = self.pgsslmode,
        )
    )]
    pub fn create_pool(self) -> Result<Pool, Error> {
        tracing::debug!("Creating deadpool-based connection pool from configuration");

        // set up TLS connectors
        #[cfg(feature = "ssl-native-tls")]
        let tls_connector = {
            let connector = TlsConnector::builder().build()?;
            MakeTlsConnector::new(connector)
        };
        #[cfg(not(feature = "ssl-native-tls"))]
        let tls_connector = tokio_postgres::NoTls;

        // configure the connection manager
        let manager = ManagerConfig {
            recycling_method: self.recycling_method.into(),
        };

        // configure the pool itself
        let pool = PoolConfig {
            max_size: self.max_connection_pool_size,
            ..PoolConfig::default()
        };

        // configure the underlying connection pool
        let config = deadpool_postgres::Config {
            dbname: Some(self.pgdbname),
            host: Some(self.pghost.to_string()),
            password: Some(self.pgpassword),
            port: Some(self.pgport),
            user: Some(self.pguser),
            application_name: Some(self.pgappname),
            ssl_mode: self.pgsslmode.map(Into::into),
            manager: Some(manager),
            pool: Some(pool),
            ..deadpool_postgres::Config::default()
        };

        // generate the pool from configuration
        let pool = config.create_pool(None, tls_connector)?;

        Ok(Pool {
            pool,
            statement_timeout: self.statement_timeout,
        })
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

/// Generate a default connection pool size
fn get_max_connection_pool_size() -> usize {
    num_cpus::get_physical() * 4
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

impl Default for Configuration {
    fn default() -> Self {
        Self {
            max_connection_pool_size: get_max_connection_pool_size(),
            statement_timeout: None,
            recycling_method: RecyclingMethod::default(),
            pgdbname: "postgres".to_owned(),
            pghost: get_localhost(),
            pgpassword: "".to_string(),
            pgport: get_postgres_port(),
            pguser: "postgres".to_owned(),
            pgappname: get_application_name(),
            pgsslmode: None,
        }
    }
}

#[allow(missing_docs)]
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "lowercase")]
pub enum RecyclingMethod {
    Fast,
    Verified,
    #[default]
    Clean,
}

impl From<RecyclingMethod> for deadpool_postgres::RecyclingMethod {
    fn from(method: RecyclingMethod) -> Self {
        match method {
            RecyclingMethod::Fast => Self::Fast,
            RecyclingMethod::Verified => Self::Verified,
            RecyclingMethod::Clean => Self::Clean,
        }
    }
}

#[allow(missing_docs)]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

impl From<SslMode> for deadpool_postgres::SslMode {
    fn from(mode: SslMode) -> Self {
        match mode {
            SslMode::Disable => Self::Disable,
            SslMode::Prefer => Self::Prefer,
            SslMode::Require => Self::Require,
        }
    }
}

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

// TODO: add unit tests
