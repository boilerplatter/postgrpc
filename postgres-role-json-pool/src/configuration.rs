use super::Pool;
use serde::{Deserialize, Deserializer};
#[cfg(feature = "postguard")]
use std::str::FromStr;
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

/// Pool configuration errors
#[derive(Debug, Error)]
pub enum Error {
    /// Bubbled-up configuration errors from the underlying `deadpool_postgres` configuration
    #[error("Error configuring the connection pool: {0}")]
    Configuration(#[from] deadpool_postgres::config::ConfigError),
    #[cfg(feature = "ssl-native-tls")]
    /// TLS errors during setup of SSL connectors
    #[error("Error setting up TLS connection: {0}")]
    Tls(#[from] native_tls::Error),
}

// FIXME: include and implement the following
// MAX_CONNECTIONS -> the maximum number of connections allowed per key
// STATEMENT_TIMEOUT -> the maximum amount of time to wait for a statement over a connection before
// aborting the thing

/// Environment-derived configuration for the default pool
#[derive(Deserialize, Debug)]
pub struct Configuration {
    /// statement types that are allowed in queries
    #[cfg(feature = "postguard")]
    #[serde(default, deserialize_with = "from_statements_string")]
    pub allowed_statements: postguard::AllowedStatements,
    /// function (by name) that can be invoked in queries
    #[cfg(feature = "postguard")]
    #[serde(default, deserialize_with = "from_functions_string")]
    pub allowed_functions: postguard::AllowedFunctions,
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

#[cfg(feature = "postguard")]
/// Deserializer for allowed_statements, passed through the environment as comma-separated string
fn from_statements_string<'de, D>(deserializer: D) -> Result<postguard::AllowedStatements, D::Error>
where
    D: Deserializer<'de>,
{
    let base_string = String::deserialize(deserializer)?;
    let allowed_statements =
        postguard::AllowedStatements::from_str(&base_string).map_err(serde::de::Error::custom)?;

    Ok(allowed_statements)
}

#[cfg(feature = "postguard")]
/// Deserializer for allowed_functions, passed through the environment as comma-separated string
fn from_functions_string<'de, D>(deserializer: D) -> Result<postguard::AllowedFunctions, D::Error>
where
    D: Deserializer<'de>,
{
    let base_string = String::deserialize(deserializer)?;
    let allowed_functions =
        postguard::AllowedFunctions::from_str(&base_string).map_err(serde::de::Error::custom)?;

    Ok(allowed_functions)
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
        let pool = config.create_pool(tls_connector)?;

        #[cfg(feature = "postguard")]
        {
            // generate statement guards from the configuration
            let cors = postguard::Guard::new(
                configuration.allowed_statements,
                configuration.allowed_functions,
            );

            Ok(Self::new(config, pool, cors))
        }

        #[cfg(not(feature = "postguard"))]
        Ok(Self::new(config, pool))
    }
}
