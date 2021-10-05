use crate::pools::default::Pool;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize, Deserializer};
use std::{
    convert::TryFrom,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("No system cert found via OpenSSL probe")]
    CertMissing,
    #[error("Error configuring the connection pool: {0}")]
    Configuration(#[from] deadpool_postgres::config::ConfigError),
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
    /// termination grace period duration (in seconds)
    #[serde(default, deserialize_with = "from_seconds_string")]
    pub termination_period: Duration,
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

/// Deserializer for termination_period seconds, passed through the environment as a string
fn from_seconds_string<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let base_string = String::deserialize(deserializer)?;
    let parsed_seconds: u64 = base_string.parse().map_err(serde::de::Error::custom)?;
    let duration = Duration::from_secs(parsed_seconds);

    Ok(duration)
}

/// Convert configurations into a valid SocketAddr
impl From<&Configuration> for SocketAddr {
    fn from(configuration: &Configuration) -> Self {
        SocketAddr::new(configuration.host, configuration.port)
    }
}

/// Derive a default pool from this configuration
impl TryFrom<Configuration> for Pool {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        // set up TLS connectors
        // let ssl = SslConnector::builder(SslMethod::tls())?;
        // let tls_connector = MakeTlsConnector::new(ssl.build());
        let connector = TlsConnector::builder().build()?;
        let tls_connector = MakeTlsConnector::new(connector);

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

        Ok(Self::new(config, pool))
    }
}
