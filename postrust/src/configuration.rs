use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr};

pub type Error = envy::Error;

/// Postrust service configuration
#[derive(Debug, Deserialize)]
pub struct Configuration {
    /// proxy host IP address
    #[serde(default = "get_v4_localhost")]
    pub host: IpAddr,
    /// proxy port
    #[serde(default = "get_proxy_port")]
    pub port: u16,
}

impl Configuration {
    /// Derive a configuration from environment variables
    pub fn from_env() -> Result<Self, Error> {
        envy::from_env()
    }
}

/// Generate a default Ipv4 pointing to localhost for configuration
fn get_v4_localhost() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
}

/// Generate a default proxy port for configuration
fn get_proxy_port() -> u16 {
    6432
}
