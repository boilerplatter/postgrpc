/// Health-checking service that validates all of the other configured services
#[cfg_attr(doc, doc(cfg(feature = "health")))]
#[cfg(any(doc, feature = "health"))]
pub mod health;

/// Postgres service that exposes the basic querying interface
#[cfg_attr(doc, doc(cfg(feature = "postgres")))]
#[cfg(any(doc, feature = "postgres"))]
pub mod postgres;

/// Transaction service that enables distributed transactions over a database
#[cfg_attr(doc, doc(cfg(feature = "transaction")))]
#[cfg(any(doc, feature = "transaction"))]
pub mod transaction;
