#![deny(unreachable_pub, missing_docs)]
/*!
tonic service implementations for PostgRPC: a gRPC wrapper around Postgres features.
!*/

/// Pool implementations and pooling traits for custom connection pools
pub mod pools;

/// gRPC Service implementations for each feature
pub mod services;

/// Re-export of async_trait macro for implementing Pool traits
pub use async_trait::async_trait;
