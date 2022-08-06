use super::deadpool::Pool;
use serde::{Deserialize, Deserializer};
use std::{
    convert::TryFrom,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use thiserror::Error;

#[cfg(feature = "ssl-native-tls")]
use native_tls::TlsConnector;
#[cfg(feature = "ssl-native-tls")]
use postgres_native_tls::MakeTlsConnector;

// FIXME: disambiguate between SERVER configuration and POOL configuration

/// Pool configuration errors
#[derive(Debug, Error)]
pub enum Error {
    /// Bubbled-up configuration errors from the underlying `deadpool_postgres` configuration
    #[error("Error creating the connection pool: {0}")]
    Create(#[from] deadpool_postgres::CreatePoolError),
    #[cfg(feature = "ssl-native-tls")]
    /// TLS errors during setup of SSL connectors
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] native_tls::Error),
}

/// Environment-derived configuration for the default pool
#[derive(Deserialize, Debug)]
pub struct Configuration {
    /// service host IP address
    #[serde(default = "get_v4_localhost")]
    pub host: IpAddr,
    /// service port
    #[serde(default = "get_service_port")]
    pub port: u16,
    /// maximum amount of time to wait for a statement to complete (in milliseconds)
    #[serde(default, deserialize_with = "from_milliseconds_string")]
    pub statement_timeout: Option<Duration>,
    /// termination grace period duration (in milliseconds)
    #[serde(default, deserialize_with = "from_milliseconds_string")]
    pub termination_period: Option<Duration>,
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

/// Generate a default Ipv4 pointing to localhost for configuration
fn get_v4_localhost() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
}

/// Generate a default service port for configuration
fn get_service_port() -> u16 {
    50051
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

/// Convert configurations into a valid SocketAddr
impl From<&Configuration> for SocketAddr {
    fn from(configuration: &Configuration) -> Self {
        SocketAddr::new(configuration.host, configuration.port)
    }
}

/// Derive a default pool from this configuration
// FIXME: allow for deeper pool configuration for each kind of pool
impl TryFrom<Configuration> for Pool {
    type Error = Error;

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
