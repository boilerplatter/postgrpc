use crate::pool::Pool;
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::atomic::{AtomicUsize, Ordering},
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
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

/// Cyclical collection of connection pools for each set of endpoints in a cluster
/// for round-robin style load balancing between endpoints
pub struct Endpoints {
    index: AtomicUsize,
    endpoints: Vec<Pool>,
}

impl Endpoints {
    /// Create a new round-robin endpoint balancer over pooled connections
    pub fn new(endpoints: Vec<Pool>) -> Self {
        Self {
            index: AtomicUsize::new(0),
            endpoints,
        }
    }

    /// Iter-like next for selecting the next endpoint in the Round Robin queue
    pub fn next(&self) -> Option<&Pool> {
        let index = self.index.fetch_add(1, Ordering::Relaxed);

        self.endpoints.get(index % self.endpoints.len())
    }
}
