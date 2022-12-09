#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unreachable_pub, missing_docs)]
#![doc = include_str!("../README.md")]

/// Pool implementations and pooling traits for custom connection pools.
pub mod pools;

/// [`tonic`](https://docs.rs/tonic/latest/tonic/)-compatible gRPC Service implementations for Postgres features.
pub mod services;

/// [Interceptors](https://docs.rs/tonic/latest/tonic/service/trait.Interceptor.html) for adding
/// [`tonic::Extensions`] to gRPC requests.
pub mod extensions;

/// Re-export of the [`async_trait`](https://docs.rs/async-trait) macro for implementing
/// [`pools::Pool`] and [`pools::Connection`]
pub use tonic::async_trait;

/// Re-export of shared libraries for code generation.
#[doc(hidden)]
#[cfg(feature = "codegen")]
pub mod codegen;

/// Compiled file descriptors for implementing [gRPC
/// reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md) with e.g.
/// [`tonic_reflection`](https://docs.rs/tonic-reflection).
#[cfg_attr(doc, doc(cfg(feature = "reflection")))]
#[cfg(any(doc, feature = "reflection"))]
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("routes");
