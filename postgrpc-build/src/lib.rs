//! `postgrpc-build` compiles `proto` files via `prost` and generates service stubs
//! and database-validated proto definitions for use with `postgrpc`.

#![deny(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![deny(rustdoc::broken_intra_doc_links)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod builder;
#[cfg(feature = "validation")]
mod proto;
mod protoc;
mod server;
#[cfg(all(test, feature = "validation"))]
mod setup;
#[cfg(feature = "validation")]
mod validator;

#[allow(unreachable_pub)]
mod annotations {
    include!("../gen/postgrpc.rs");
}

pub use builder::{compile_protos, configure, Builder};
