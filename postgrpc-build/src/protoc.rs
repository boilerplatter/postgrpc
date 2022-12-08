use crate::proto::Method;
use prost::Message;
use prost_types::FileDescriptorSet;
use std::{collections::HashMap, io, path::Path};

/// [`prost`]-based Proto types parsed from a [`prost_types::FileDescriptorSet`]
#[ouroboros::self_referencing]
pub(crate) struct Protos {
    file_descriptor_set: FileDescriptorSet,
    #[covariant]
    #[borrows(file_descriptor_set)]
    pub(crate) services: HashMap<String, io::Result<HashMap<String, Method<'this>>>>,
}

impl Protos {
    pub(crate) fn from_file_descriptor_set(
        file_descriptor_set: FileDescriptorSet,
    ) -> io::Result<Self> {
        let protos = ProtosBuilder {
            file_descriptor_set,
            services_builder: |file_descriptor_set: &FileDescriptorSet| {
                let mut services = HashMap::new();

                for file in &file_descriptor_set.file {
                    let package = file.package();

                    // group all messages across the file by full message path + name
                    let messages = file
                        .message_type
                        .iter()
                        .map(|message| (format!(".{package}.{}", message.name()), message))
                        .collect();

                    // group all enums across files by full enum path + name
                    let enums = file
                        .enum_type
                        .iter()
                        .map(|enum_type| (format!(".{package}.{}", enum_type.name()), enum_type))
                        .collect();

                    // group the services across files by full service path + name
                    // FIXME: use iterators + collect()
                    for service in file.service.iter() {
                        // extract the postgrpc-annotated methods
                        let methods = service
                            .method
                            .iter()
                            .filter_map(|method| {
                                super::proto::Method::from_method_descriptor(
                                    method, &messages, &enums,
                                )
                                .map(|method| {
                                    method.map(|method| (method.name().to_owned(), method))
                                })
                                .transpose()
                            })
                            .collect::<Result<_, _>>();

                        services.insert(format!(".{package}.{}", service.name()), methods);
                    }
                }

                services
            },
        }
        .build();

        Ok(protos)
    }
}

/// Compile protos with the external `protoc` compiler
pub(crate) fn compile_protos(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> io::Result<Protos> {
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
        .and_then(Protos::from_file_descriptor_set)
}
