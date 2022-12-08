use crate::proto::Services;
use prost::Message;
use std::{io, path::Path};

/// Compile service protos with the external `protoc` compiler
pub(crate) fn compile_services(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> io::Result<Services> {
    let tmp = tempfile::Builder::new().prefix("prost-build").tempdir()?;
    let file_descriptor_set_path = tmp.path().join("prost-descriptor-set");

    let protoc = prost_build::protoc_from_env();
    let mut cmd = std::process::Command::new(protoc.clone());

    cmd.arg("--include_imports")
        .arg("--include_source_info")
        .arg("-o")
        .arg(&file_descriptor_set_path);

    for include in includes {
        if include.as_ref().exists() {
            cmd.arg("-I").arg(include.as_ref());
        }
    }

    for proto in protos {
        cmd.arg(proto.as_ref());
    }

    // FIXME: use proper logging
    println!("Running: {:?}", cmd);

    let output = cmd.output().map_err(|error| {
        io::Error::new(
            error.kind(),
            // FIXME: copy #sourcing-protoc into our own docs
            format!("failed to invoke protoc (hint: https://docs.rs/prost-build/#sourcing-protoc): (path: {protoc:?}): {error}"),
        )
    })?;

    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("protoc failed: {}", String::from_utf8_lossy(&output.stderr)),
        ));
    }

    let buf = std::fs::read(&file_descriptor_set_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("unable to open file_descriptor_set_path: {file_descriptor_set_path:?}, OS: {error}"),
        )
    })?;

    // handle custom postgRPC annotations through the extensions API
    let mut extension_registry = prost::ExtensionRegistry::new();
    extension_registry.register(super::annotations::QUERY);

    prost_types::FileDescriptorSet::decode_with_extensions(&*buf, extension_registry)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid FileDescriptorSet: {error}"),
            )
        })
        .and_then(Services::from_file_descriptor_set)
}
