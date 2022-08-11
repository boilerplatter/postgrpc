/// Health-checking service that validates all of the other configured services
#[cfg(feature = "health")]
pub mod health;

/// Postgres service that exposes the basic querying interface
pub mod postgres;

/// Transaction service that enables distributed transactions over a database
#[cfg(feature = "transaction")]
pub mod transaction;
