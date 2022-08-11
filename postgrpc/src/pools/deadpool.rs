//! A Postgres connection pool built on `postgres-pool` and `deadpool_postgres` that is meant for
//! JSON-based querying from remote sources. This pool initiates connections from a single user,
//! then uses a Key that maps to a Postgres `ROLE` to `SET LOCAL ROLE` before each connection is used.
//! In addition, this pool limits inputs to a scalar `Parameter` subset of valid JSON values,
//! returning rows as a stream of JSON Objects.
use super::{Connection, FromRequest, Parameter};
use async_trait::async_trait;
use deadpool_postgres::tokio_postgres::{error::SqlState, RowStream, Statement};
use futures_util::{ready, Stream};
use pin_project_lite::pin_project;
use serde::{Deserialize, Deserializer};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tonic::{Request, Status};
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
    /// Bubbled-up `deadpool_postgres` connection pool errors
    #[error("Error fetching connection from the pool: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    /// Bubbled-up `tokio_postgres` SQL-level errors within a connection
    #[error("SQL Query error: {0}")]
    Query(#[from] deadpool_postgres::tokio_postgres::Error),
    /// ROLE-setting errors before connections are returned to users
    #[error("Unable to set the ROLE of the connection before use: {0}")]
    Role(deadpool_postgres::tokio_postgres::Error),
    /// JSON-formatted rows could not be properly converted between Postgres' built-in `to_json()` output and
    /// `serde_json::Value`. If this error occurs, it is probably a bug in `serde_json` or Postgres itself.
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = error.to_string();

        match error {
            Error::Params { .. } | Error::Role(..) | Error::Query(..) => {
                Status::invalid_argument(message)
            }
            Error::Pool(..) => Status::resource_exhausted(message),
            Error::InvalidJson => Status::internal(message),
        }
    }
}

/// Optionally-derived Role key for the default pool
type Role = Option<String>;

impl FromRequest for Role {
    type Error = Status;

    fn from_request<T>(request: &mut Request<T>) -> Result<Self, Self::Error> {
        let role = request
            .extensions_mut()
            .remove::<RoleExtension>()
            .ok_or_else(|| Status::internal("Failed to load extensions before handling request"))?
            .role;

        Ok(role)
    }
}

/// Deadpool-based pool implementation keyed by ROLE pointing to a single database
// database connections are initiated from a single user and shared through SET LOCAL ROLE
// this pool only supports binary encoding, so all non-JSON types must be hinted at in the query
pub struct Pool {
    pool: deadpool_postgres::Pool,
    statement_timeout: Option<Duration>,
}

impl Pool {
    /// Create a new pool from `deadpool_postgres`'s constituent parts
    pub fn new(pool: deadpool_postgres::Pool, statement_timeout: Option<Duration>) -> Self {
        Self {
            pool,
            statement_timeout,
        }
    }
}

#[async_trait]
impl super::Pool for Pool {
    type Key = Role;
    type Connection = Client;
    type Error = <Self::Connection as Connection>::Error;

    async fn get_connection(&self, key: Option<String>) -> Result<Self::Connection, Self::Error> {
        let client = self.pool.get().await?;

        // configure the connection's ROLE
        let local_role_statement = match key {
            Some(role) => format!(r#"SET ROLE "{}""#, role),
            None => "RESET ROLE".to_string(),
        };

        client
            .batch_execute(&local_role_statement)
            .await
            .map_err(Error::Role)?;

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
    /// The stream of gRPC-formatted rows returned by this pool's associated connection
    pub struct StructStream {
        #[pin]
        rows: RowStream,
    }
}

impl Stream for StructStream {
    type Item = Result<prost_types::Struct, Error>;

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

/// Wrapper around the client provided by deadpool_postgres
pub struct Client {
    client: deadpool_postgres::Client,
}

#[async_trait]
impl Connection for Client {
    type Error = Error;
    type RowStream = StructStream;

    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<Self::RowStream, Self::Error> {
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

        let rows = match query_raw(self, statement, &prepared_statement, parameters).await {
            // retry the query if the schema was poisoned
            Err(Error::Query(error)) if error.code() == Some(&SqlState::FEATURE_NOT_SUPPORTED) => {
                tracing::warn!("Schema poisoned underneath statement cache. Retrying query");

                self.client
                    .statement_cache
                    .remove(statement, inferred_types);

                query_raw(self, statement, &prepared_statement, parameters).await
            }
            result => result,
        }?;

        Ok(StructStream::from(rows))
    }

    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        self.client.batch_execute(query).await?;

        Ok(())
    }
}

/// Wrapper around a raw query that can be retried
async fn query_raw(
    client: &Client,
    statement: &str,
    prepared_statement: &Statement,
    parameters: &[Parameter],
) -> Result<RowStream, Error> {
    let rows = if prepared_statement.columns().is_empty() {
        // execute statements that return no data without modification
        client
            .client
            .query_raw(prepared_statement, parameters)
            .await?
    } else {
        // wrap queries that return data in to_json()
        let json_statement = format!(
            "WITH cte AS ({})
            SELECT TO_JSON(result) AS json
            FROM (SELECT * FROM cte) AS result",
            &statement
        );

        let prepared_statement = client.client.prepare_cached(&json_statement).await?;

        client
            .client
            .query_raw(&prepared_statement, parameters)
            .await?
    };

    Ok(rows)
}

#[cfg(feature = "role-header")]
const ROLE_HEADER: &str = "x-postgres-role";

/// X-postgres-* headers collected into a single extension
struct RoleExtension {
    role: Option<String>,
}

/// Interceptor function for collecting the extensions needed by the deadpool pool
pub fn interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    // derive the role from metadata
    #[cfg(feature = "role-header")]
    let role = request
        .metadata()
        .get(ROLE_HEADER)
        .map(|header| header.to_str())
        .transpose()
        .map_err(|error| {
            let message = format!("Invalid {} header: {}", ROLE_HEADER, error);

            Status::invalid_argument(message)
        })?
        .map(String::from);

    #[cfg(not(feature = "role-header"))]
    let role = None;

    // add the Postgres extension to the request
    request.extensions_mut().insert(RoleExtension { role });

    Ok(request)
}

/// Deadpool-specific configuration variables
// FIXME: expose the whole underlying configuration, if possible
#[derive(Deserialize, Debug)]
pub struct Configuration {
    /// maximum amount of time to wait for a statement to complete (in milliseconds)
    #[serde(default, deserialize_with = "from_milliseconds_string")]
    pub statement_timeout: Option<Duration>,
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
}

/// Generate a default "localhost" host value
fn get_localhost() -> String {
    "localhost".to_string()
}

/// Generate a default port for connecting to the postgres database
fn get_postgres_port() -> u16 {
    5432
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

/// Pool configuration errors
#[derive(Debug, Error)]
pub enum ConfigurationError {
    /// Bubbled-up configuration errors from the underlying `deadpool_postgres` configuration
    #[error("Error creating the connection pool: {0}")]
    Create(#[from] deadpool_postgres::CreatePoolError),
    #[cfg(feature = "ssl-native-tls")]
    /// TLS errors during setup of SSL connectors
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] native_tls::Error),
}

/// Derive a default pool from this configuration
// FIXME: allow for deeper pool configuration for each kind of pool
impl TryFrom<Configuration> for Pool {
    type Error = ConfigurationError;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        // set up TLS connectors
        #[cfg(feature = "ssl-native-tls")]
        let connector = TlsConnector::builder().build()?;
        #[cfg(feature = "ssl-native-tls")]
        let tls_connector = MakeTlsConnector::new(connector);
        #[cfg(not(feature = "ssl-native-tls"))]
        let tls_connector = tokio_postgres::NoTls;

        // configure the underlying connection pool
        let config = deadpool_postgres::Config {
            dbname: Some(configuration.pgdbname),
            host: Some(configuration.pghost.to_string()),
            password: Some(configuration.pgpassword),
            port: Some(configuration.pgport),
            user: Some(configuration.pguser),
            ..deadpool_postgres::Config::default()
        };

        // generate the pool from configuration
        let pool = config.create_pool(None, tls_connector)?;

        Ok(Self::new(pool, configuration.statement_timeout))
    }
}

/// Convert a serde_json::Value into a prost_types::Value
fn to_proto_value(json: serde_json::Value) -> prost_types::Value {
    let kind = match json {
        serde_json::Value::Null => prost_types::value::Kind::NullValue(0),
        serde_json::Value::Bool(boolean) => prost_types::value::Kind::BoolValue(boolean),
        serde_json::Value::Number(number) => match number.as_f64() {
            Some(number) => prost_types::value::Kind::NumberValue(number),
            None => prost_types::value::Kind::StringValue(number.to_string()),
        },
        serde_json::Value::String(string) => prost_types::value::Kind::StringValue(string),
        serde_json::Value::Array(array) => {
            prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: array.into_iter().map(to_proto_value).collect(),
            })
        }
        serde_json::Value::Object(map) => {
            prost_types::value::Kind::StructValue(to_proto_struct(map))
        }
    };

    prost_types::Value { kind: Some(kind) }
}

/// Convert a serde_json::Map into a prost_types::Struct
fn to_proto_struct(map: serde_json::Map<String, serde_json::Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: map
            .into_iter()
            .map(|(key, value)| (key, to_proto_value(value)))
            .collect(),
    }
}

// TODO: add unit tests
