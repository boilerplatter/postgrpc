#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unreachable_pub, missing_docs)]
//!
//! [`postrpc`](https://github.com/boilerplatter/postgrpc) is a [gRPC](https://grpc.io/) wrapper around Postgres
//! databases that provides a dynamically-typed, JSON-compatible query interface.
//!
//! While most users will use [`postgrpc`](https://github.com/boilerplatter/postgrpc) in its standalone,
//! executable form, this library provides all of the building blocks needed to build your own gRPC server.
//! Customization can be done through feature-based conditional compilation and by implementing
//! [`pools::Pool`] and [`pools::Connection`] traits over customized connection management systems.
//!
//! ## How to use `postgrpc`
//!
//! By default, this crate ships with all of the features required by its executable. If you're
//! using `postgrpc` as a library, you will probably only want to enable a subset of those
//! features. A few of those uses are discussed below.
//!
//! #### gRPC handling as a `tonic::server::NamedService`
//!
//! By default, `postgrpc` ships a connection-pool-agnostic, [`tonic`](https://docs.rs/tonic/)-compatible gRPC Postgres service in
//! [`services::postgres`]. To use this service as a part of a [`tonic`](https://docs.rs/tonic)
//! app without needing to implement your own pool, be sure to enable the `deadpool` feature in
//! your `Cargo.toml`.
//!
//! ##### [`tonic`](https://docs.rs/tonic/latest/tonic/) Example
//! ```rust
//! use postgrpc::{pools::deadpool, services::postgres};
//! use std::sync::Arc;
//! use tonic::transport::Server;
//! // deadpool::Configuration implements serde::Deserialize,
//! // so envy can be used to deserialize it from environment variables
//! use envy::from_env;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!   // create the connection pool from environment variables
//!   let pool = from_env::<deadpool::Configuration>()?
//!     .create_pool()
//!     .map(Arc::new)?;
//!
//!   // configure and run the tonic server on port 8675
//!   Server::builder()
//!     .add_service(services::postgres::new(pool))
//!     .serve(([127, 0, 0, 1], 8675).into())
//!     .await?;
//!
//!   Ok(())
//! }
//! ```
//!
//! #### Custom connection pooling
//! See the documentation of [`pools::Pool`] and [`pools::Connection`] for how to implement your
//! own connection pools for [`services`]. Custom pools can be implemented without any default
//! features from this library.

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
