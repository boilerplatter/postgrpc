#![deny(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![deny(rustdoc::broken_intra_doc_links)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

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
    use tempfile::TempDir;

    static SOURCE: &str = include_str!("../proto/postgrpc/api/annotations.proto");

    /// generate proto annotations into a temporary directory
    pub(crate) fn generate(tmp: &TempDir) -> std::io::Result<()> {
        // write the annotations to the temp directory
        let annotations_directory = tmp.path().join("postgrpc").join("api");
        std::fs::create_dir_all(&annotations_directory)?;

        let annotations_path = annotations_directory.join("annotations.proto");
        std::fs::write(&annotations_path, SOURCE)
    }

    include!("../gen/postgrpc.rs");
}

pub use builder::{compile_protos, configure, Builder};
