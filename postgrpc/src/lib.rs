#![deny(unreachable_pub, missing_docs)]
/*!
tonic service implementations for PostgRPC: a gRPC wrapper around Postgres features.
!*/

/// Connection pool traits for implementing custom pools
pub mod pool;

/// Pool implementations for different use-cases
pub mod pools;

#[cfg(feature = "deadpool")]
mod json;
