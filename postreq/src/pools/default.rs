use super::Connection;
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio_postgres::{
    error::SqlState,
    types::{to_sql_checked, IsNull, ToSql, Type},
    RowStream, Statement,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Expected {expected} parameters but found {actual} instead")]
    Params { expected: usize, actual: usize },
    #[error("Error fetching connection from the pool: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("SQL Query error: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("Unable to set the ROLE of the connection before use: {0}")]
    Role(tokio_postgres::Error),
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] native_tls::Error),
    #[error("Unable to aggregate rows from query into valid JSON")]
    InvalidJson,
}

/// Optionally-derived Role key for the default pool
type Role = Option<String>;

/// Deadpool-based pool implementation keyed by ROLE pointing to a single database
// database connections are initiated from a single user and shared through SET LOCAL ROLE
// this pool only supports binary encoding, so all non-JSON types must be hinted at in the query
#[derive(Clone)]
pub struct Pool {
    config: deadpool_postgres::Config,
    pool: deadpool_postgres::Pool,
}

impl Pool {
    pub fn new(config: deadpool_postgres::Config, pool: deadpool_postgres::Pool) -> Self {
        Self { config, pool }
    }
}

#[async_trait::async_trait]
impl super::Pool for Pool {
    type Key = Role;
    type Connection = deadpool_postgres::Client;
    type Error = <Self::Connection as Connection>::Error;

    async fn get_connection(&self, key: Option<String>) -> Result<Self::Connection, Self::Error> {
        let connection = self.pool.get().await?;

        let local_role_statement = match key {
            Some(role) => format!(r#"SET ROLE "{}""#, role),
            None => "RESET ROLE".to_string(),
        };

        connection
            .batch_execute(&local_role_statement)
            .await
            .map_err(Error::Role)?;

        Ok(connection)
    }
}

pin_project! {
    /// The stream of JSON-formatted rows returned by this pool's associated connection
    pub struct JsonStream {
        #[pin]
        rows: RowStream,
    }
}

impl Stream for JsonStream {
    type Item = Result<serde_json::Map<String, serde_json::Value>, Error>;

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

#[async_trait::async_trait]
impl Connection for deadpool_postgres::Client {
    type Error = Error;
    type Parameter = Parameter;
    type RowStream = JsonStream;

    async fn query(
        &self,
        statement: &str,
        parameters: &[Self::Parameter],
    ) -> Result<Self::RowStream, Self::Error> {
        let prepared_statement = self.prepare_cached(statement).await?;

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

                self.statement_cache.remove(statement, inferred_types);

                query_raw(self, statement, &prepared_statement, parameters).await
            }
            result => result,
        }?;

        Ok(JsonStream::from(rows))
    }

    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        self.batch_execute(query).await?;

        Ok(())
    }
}

/// Wrapper around a raw query that can be retried
async fn query_raw(
    client: &deadpool_postgres::Client,
    statement: &str,
    prepared_statement: &Statement,
    parameters: &[Parameter],
) -> Result<RowStream, Error> {
    let rows = if prepared_statement.columns().is_empty() {
        // execute statements that return no data without modification
        client.query_raw(prepared_statement, parameters).await?
    } else {
        // wrap queries that return data in to_json()
        let json_statement = format!(
            "SELECT TO_JSON(result) AS json FROM ({}) AS result",
            &statement
        );

        let prepared_statement = client.prepare_cached(&json_statement).await?;

        client.query_raw(&prepared_statement, parameters).await?
    };

    Ok(rows)
}

/// Accepted parameter types from JSON
#[derive(Debug)]
pub enum Parameter {
    Null,
    Boolean(bool),
    Number(f64),
    Text(String),
}

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

// TODO: add unit tests
