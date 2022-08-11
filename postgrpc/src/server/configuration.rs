use serde::{Deserialize, Deserializer};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

/// Environment-derived server configuration
#[derive(Deserialize, Debug)]
pub struct Configuration {
    /// service host IP address
    #[serde(default = "get_v4_localhost")]
    pub host: IpAddr,
    /// service port
    #[serde(default = "get_service_port")]
    pub port: u16,
    /// termination grace period duration (in milliseconds)
    #[serde(default, deserialize_with = "from_milliseconds_string")]
    pub termination_period: Option<Duration>,
}

/// Generate a default Ipv4 pointing to localhost for configuration
fn get_v4_localhost() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
}

/// Generate a default service port for configuration
fn get_service_port() -> u16 {
    50051
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
