use prost::Message;
use std::{io, path::Path};

/// Validate a set of `postgrpc`-annotated protos against a database
pub(crate) fn validate(
    connection_string: &str,
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> io::Result<()> {
    let file_descriptor_set = compile_file_descriptors(protos, includes)?;

    for file in file_descriptor_set.file.iter() {
        // extract the message structs from the original file descriptor
        let messages = file
            .message_type
            .iter()
            .map(|descriptor| (descriptor.name().to_string(), descriptor))
            .collect::<std::collections::HashMap<_, _>>();

        // extract the postgrpc-annotated methods from the original file descriptor
        let methods = file
            .service
            .iter()
            .flat_map(|service| service.method.iter())
            .filter_map(|method| {
                super::proto::Method::from_method_descriptor(method, &messages).transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        // set up the postgres client for validation
        // FIXME: share this client!
        let mut client = postgres::Client::connect(connection_string, postgres::NoTls) // FIXME: enable TLS
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

        // validate the inputs and outputs of each postgrpc-annotated method
        for method in methods {
            println!("Validating rpc {} against the database", method.name());

            // prepare the method's SQL query with the database
            let statement = client
                .prepare(method.query())
                .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

            // check the input message fields against the parameter types
            method.validate_input(statement.params())?;

            // check the output message fields against the column types
            method.validate_output(statement.columns())?;
        }
    }

    Ok(())
}

/// Compile file descriptors with external `protoc` compiler
fn compile_file_descriptors(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> io::Result<prost_types::FileDescriptorSet> {
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

    println!("Running: {:?}", cmd);

    let output = cmd.output().map_err(|error| {
        io::Error::new(
            error.kind(),
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

    prost_types::FileDescriptorSet::decode_with_extensions(&*buf, extension_registry).map_err(
        |error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid FileDescriptorSet: {}", error),
            )
        },
    )
}

#[cfg(test)]
mod test {
    // FIXME: turn this into an integration test instead of a unit test
    #[test]
    fn validates_protos() {
        super::validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./examples/bookstore/authors.proto"],
            &["./examples/bookstore", "./proto"],
        )
        .unwrap();
    }
}
