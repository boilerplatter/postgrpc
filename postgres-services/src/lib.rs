//! Distributed query handlers for Postgres built on `postgres-pool`-style connection pools.
#![deny(missing_docs, unreachable_pub)]

/// Postgres database service handlers
pub mod postgres;
#[cfg(feature = "transaction")]
/// Postgres transaction handlers
pub mod transaction;
