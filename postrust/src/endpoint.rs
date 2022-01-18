use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};

/// Postrust's only supported protocol version
// TODO: see if we can support a range of these
const SUPPORTED_PROTOCOL_VERSION: i32 = 196608;

/// Database connection endpoint configuration
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Endpoint {
    pub user: String,
    pub password: String,
    pub database: String,
    host: IpAddr,
    port: u16,
    protocol_version: i32,
}

impl Endpoint {
    pub fn new(user: String, password: String, database: String, host: IpAddr, port: u16) -> Self {
        Self {
            user,
            password,
            database,
            host,
            port,
            protocol_version: SUPPORTED_PROTOCOL_VERSION,
        }
    }

    pub fn address(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Endpoint")
            .field("user", &self.user)
            .field("password", &"******")
            .field("database", &self.database)
            .field("address", &self.address())
            .finish()
    }
}

#[cfg(test)]
impl Default for Endpoint {
    fn default() -> Self {
        Self {
            user: String::new(),
            password: String::new(),
            database: String::new(),
            host: IpAddr::V4([127, 0, 0, 1].into()),
            port: 5432,
            protocol_version: SUPPORTED_PROTOCOL_VERSION,
        }
    }
}
