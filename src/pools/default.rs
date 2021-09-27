use super::{Connection, Parameter};
use crate::configuration::Configuration;
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use std::convert::TryFrom;
use thiserror::Error;
use tokio_postgres::{error::SqlState, RowStream, Statement};
use tonic::Status;

#[derive(Debug, Error)]
pub enum Error {
    #[error("No system cert found via OpenSSL probe")]
    CertMissing,
    #[error("Error configuring the connection pool: {0}")]
    Configuration(#[from] deadpool_postgres::config::ConfigError),
    #[error("Expected {expected} parameters but found {actual} instead")]
    Params { expected: usize, actual: usize },
    #[error("Error fetching connection from the pool: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("SQL Query error: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("Unable to set the ROLE of the connection before use: {0}")]
    Role(Box<Error>),
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] openssl::error::ErrorStack),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = format!("{}", &error);

        match error {
            Error::CertMissing
            | Error::Configuration(..)
            | Error::Pool(..)
            | Error::Role(..)
            | Error::Tls(..) => Self::internal(message),
            Error::Params { .. } | Error::Query(..) => Self::invalid_argument(message),
        }
    }
}

/// Deadpool-based pool implementation keyed by ROLE pointing to a single database
// database connections are initiated from a single user and shared through SET LOCAL ROLE
// this pool only supports binary encoding, so all non-JSON types must be hinted at in the query
#[derive(Clone)]
pub struct Pool {
    pool: deadpool_postgres::Pool,
}

impl TryFrom<Configuration> for Pool {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        // set up TLS connectors
        let ssl = SslConnector::builder(SslMethod::tls())?;
        let tls_connector = MakeTlsConnector::new(ssl.build());

        // configure the underlying connection pool
        let config = deadpool_postgres::Config {
            dbname: Some(configuration.pgdbname),
            host: Some(configuration.pghost.to_string()),
            password: Some(configuration.pgpassword),
            port: Some(configuration.pgport),
            user: Some(configuration.pguser),
            ..deadpool_postgres::Config::default()
        };

        // generate the pool from confiuration
        let pool = config.create_pool(tls_connector)?;

        Ok(Self { pool })
    }
}

#[tonic::async_trait]
impl super::Pool<Option<String>> for Pool {
    type Connection = deadpool_postgres::Client;
    type Error = Error;

    async fn get_connection(&self, key: Option<String>) -> Result<Self::Connection, Self::Error> {
        let connection = self.pool.get().await?;

        if let Some(role) = key {
            let local_role_statement = format!(r#"SET LOCAL ROLE "{}""#, role);

            connection
                .query(local_role_statement.as_str(), &[])
                .await
                .map_err(|error| Error::Role(Box::new(error)))?;
        }

        Ok(connection)
    }
}

#[tonic::async_trait]
impl Connection for deadpool_postgres::Client {
    type Error = Error;

    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<RowStream, Self::Error> {
        let prepared_statement = self.prepare_cached(statement).await?;

        // check parameter count to avoid panics
        let inferred_types = prepared_statement.params();

        if inferred_types.len() != parameters.len() {
            return Err(Error::Params {
                expected: inferred_types.len(),
                actual: parameters.len(),
            });
        }

        match query_raw(self, statement, &prepared_statement, parameters).await {
            // retry the query if the schema was poisoned
            Err(Error::Query(error)) if error.code() == Some(&SqlState::FEATURE_NOT_SUPPORTED) => {
                tracing::warn!("Schema poisoned underneath statement cache. Retrying query");

                self.statement_cache.remove(statement, inferred_types);

                query_raw(self, statement, &prepared_statement, parameters).await
            }
            result => result,
        }
    }

    async fn batch(&self, query: &str) -> Result<(), Self::Error> {
        self.batch_execute(query).await?;

        Ok(())
    }
}

/// Wrapper around a raw query that can be retried
async fn query_raw<'a>(
    client: &deadpool_postgres::Client,
    statement: &str,
    prepared_statement: &Statement,
    parameters: &[Parameter<'a>],
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

// TODO: add unit tests
