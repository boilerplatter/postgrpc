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

mod codegen;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
mod proto;
mod service_generator;
#[cfg(feature = "postgres")]
mod validator;

#[allow(unreachable_pub)]
mod annotations {
    include!("../gen/postgrpc.rs");
}

pub use codegen::{compile_protos, configure, Builder};
