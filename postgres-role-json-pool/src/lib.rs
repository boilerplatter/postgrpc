//! A Postgres connection pool built on `postgres-pool` and `deadpool_postgres` that is meant for
//! JSON-based querying from remote sources. This pool initiates connections from a single user,
//! then uses a Key that maps to a Postgres `ROLE` to `SET LOCAL ROLE` before each connection is used.
//! In addition, this pool limits inputs to a scalar `Parameter` subset of valid JSON values,
//! returning rows as a stream of JSON Objects.

#![deny(missing_docs, unreachable_pub)]

use futures_core::{ready, Stream};
#[cfg(feature = "http-types")]
use futures_util::TryStreamExt;
use pin_project_lite::pin_project;
use postgres_pool::Connection;
use serde::de::{Deserialize, Visitor};
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio_postgres::{
    error::SqlState,
    types::{to_sql_checked, IsNull, ToSql, Type},
    RowStream, Statement,
};

/// Configure the connection pool
pub mod configuration;

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
    Query(#[from] tokio_postgres::Error),
    /// ROLE-setting errors before connections are returned to users
    #[error("Unable to set the ROLE of the connection before use: {0}")]
    Role(tokio_postgres::Error),
    /// JSON-formatted rows could not be properly converted between Postgres' built-in `to_json()` output and
    /// `serde_json::Value`. If this error occurs, it is probably a bug in `serde_json` or Postgres itself.
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
}

#[cfg(feature = "http-types")]
impl From<Error> for http::StatusCode {
    fn from(error: Error) -> Self {
        match error {
            Error::Params { .. } | Error::Query(..) | Error::InvalidJson => Self::BAD_REQUEST,
            Error::Pool(..) | Error::Role(..) => Self::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Optionally-derived Role key for the default pool
#[derive(Debug, Default, Clone, Hash, PartialEq, Eq)]
pub struct Role(Option<String>);

impl Role {
    /// Create a new Role manually
    pub fn new(role: Option<String>) -> Self {
        Self(role)
    }

    /// Consume the role returning its contents
    pub fn into_inner(self) -> Option<String> {
        self.0
    }
}

#[cfg(feature = "http-types")]
impl From<http::Extensions> for Role {
    fn from(mut extensions: http::Extensions) -> Self {
        extensions.remove::<Self>().unwrap_or_default()
    }
}

/// Deadpool-based pool implementation keyed by ROLE pointing to a single database
// database connections are initiated from a single user and shared through SET LOCAL ROLE
// this pool only supports binary encoding, so all non-JSON types must be hinted at in the query
#[derive(Clone)]
pub struct Pool {
    config: deadpool_postgres::Config,
    pool: deadpool_postgres::Pool,
}

impl Pool {
    /// Create a new pool from `deadpool_postgres`'s constituent parts
    pub fn new(config: deadpool_postgres::Config, pool: deadpool_postgres::Pool) -> Self {
        Self { config, pool }
    }
}

#[async_trait::async_trait]
impl postgres_pool::Pool for Pool {
    type Key = Role;
    type Connection = Client;
    type Error = <Self::Connection as Connection>::Error;

    async fn get_connection(&self, key: Role) -> Result<Self::Connection, Self::Error> {
        let connection = self.pool.get().await?;

        let local_role_statement = match key.0 {
            Some(role) => format!(r#"SET ROLE "{}""#, role),
            None => "RESET ROLE".to_string(),
        };

        connection
            .batch_execute(&local_role_statement)
            .await
            .map_err(Error::Role)?;

        Ok(Client(connection))
    }
}

type Row = serde_json::Map<String, serde_json::Value>;

pin_project! {
    /// The stream of JSON-formatted rows returned by this pool's associated connection
    pub struct JsonStream {
        #[pin]
        rows: RowStream,
    }
}

impl Stream for JsonStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.rows.poll_next(context)?) {
            Some(row) => {
                if let serde_json::Value::Object(map) = row.try_get("json")? {
                    Poll::Ready(Some(Ok(map)))
                } else {
                    Poll::Ready(Some(Err(Error::InvalidJson)))
                }
            }
            None => Poll::Ready(None),
        }
    }
}

impl From<RowStream> for JsonStream {
    fn from(rows: RowStream) -> Self {
        Self { rows }
    }
}

#[cfg(feature = "http-types")]
impl From<JsonStream> for hyper::Body {
    fn from(json_stream: JsonStream) -> Self {
        hyper::Body::wrap_stream(json_stream.and_then(|row| async move {
            let json = serde_json::to_vec(&row).map_err(|_| Error::InvalidJson)?;

            Ok(hyper::body::Bytes::from(json))
        }))
    }
}

/// Newtype wrapper around the client provided by deadpool_postgres
pub struct Client(deadpool_postgres::Client);

#[async_trait::async_trait]
impl Connection for Client {
    type Error = Error;
    type Parameter = Parameter;
    type RowStream = JsonStream;

    async fn query(
        &self,
        statement: &str,
        parameters: &[Self::Parameter],
    ) -> Result<Self::RowStream, Self::Error> {
        let prepared_statement = self.0.prepare_cached(statement).await?;

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

                self.0.statement_cache.remove(statement, inferred_types);

                query_raw(self, statement, &prepared_statement, parameters).await
            }
            result => result,
        }?;

        Ok(JsonStream::from(rows))
    }

    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        self.0.batch_execute(query).await?;

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
        client.0.query_raw(prepared_statement, parameters).await?
    } else {
        // wrap queries that return data in to_json()
        let json_statement = format!(
            "WITH cte AS ({})
            SELECT TO_JSON(result) AS json
            FROM (SELECT * FROM cte) AS result",
            &statement
        );

        let prepared_statement = client.0.prepare_cached(&json_statement).await?;

        client.0.query_raw(&prepared_statement, parameters).await?
    };

    Ok(rows)
}

/// Accepted parameter types from JSON
#[derive(Debug)]
pub enum Parameter {
    /// JSON's `null`
    Null,
    /// JSON's boolean values
    Boolean(bool),
    /// JSON's number values
    Number(f64),
    /// JSON's string values, also used here as a catch-all for type inference
    Text(String),
}

// FIXME: implement serde's own Deserialize

impl Parameter {
    /// convert serde JSON values to scalar parameters
    // TODO: consider relaxing the scalar constraint for specific cases
    // (e.g. ListValues of a single type and JSON/serializable composite types for StructValues)
    pub fn from_json_value(value: serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::Array(..) | serde_json::Value::Object(..) => None,
            serde_json::Value::Null => Some(Parameter::Null),
            serde_json::Value::Bool(boolean) => Some(Parameter::Boolean(boolean)),
            serde_json::Value::Number(number) => {
                number.as_f64().map(|number| Parameter::Number(number))
            }
            serde_json::Value::String(text) => Some(Parameter::Text(text)),
        }
    }
}

/// Binary encoding for Parameters
impl ToSql for Parameter {
    fn to_sql(
        &self,
        type_: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Boolean(boolean) => boolean.to_sql(type_, out),
            Self::Text(text) => text.to_sql(type_, out),
            Self::Number(number) => match type_ {
                &Type::INT2 => (*number as i16).to_sql(type_, out),
                &Type::INT4 => (*number as i32).to_sql(type_, out),
                &Type::INT8 => (*number as i64).to_sql(type_, out),
                &Type::FLOAT4 => (*number as f32).to_sql(type_, out),
                &Type::FLOAT8 => (*number as f64).to_sql(type_, out),
                // ToSql should not be used for type-inferred parameters of format text
                _ => Err(format!("Cannot encode number as type {}", type_).into()),
            },
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl<'de> Deserialize<'de> for Parameter {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Parameter, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Parameter;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(Self::Value::Boolean(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(Self::Value::Number(value as f64))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(Self::Value::Number(value as f64))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(Self::Value::Number(value))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(String::from(value))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(Self::Value::Text(value))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(Self::Value::Null)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(Self::Value::Null)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

// TODO: add unit tests
