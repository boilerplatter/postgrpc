#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unreachable_pub, missing_docs)]
//!
//! tonic service implementations for PostgRPC: a gRPC wrapper around Postgres features.
//!

/// Pool implementations and pooling traits for custom connection pools
pub mod pools;

/// gRPC Service implementations for each feature
pub mod services;

/// Request extension helpers and interceptors for use in connection pools
pub mod extensions;

/// Re-export of async_trait macro for implementing Pool traits
pub use tonic::async_trait;

/// Compiled file descriptors for implementing gRPC reflection
#[cfg_attr(doc, doc(cfg(feature = "reflection")))]
#[cfg(any(doc, feature = "reflection"))]
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("routes");
