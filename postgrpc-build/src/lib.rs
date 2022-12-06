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

mod code_gen;
#[cfg(feature = "postgres")]
mod proto;
#[cfg(all(test, feature = "postgres"))]
mod setup;
#[cfg(feature = "postgres")]
mod validator;

#[allow(unreachable_pub)]
mod annotations {
    include!("../gen/postgrpc.rs");
}

pub use code_gen::{compile_protos, configure, Builder};
