use prost::Message;
use std::{io, path::Path};

/// Validate a set of `postgrpc`-annotated protos against a database
pub(crate) fn validate(
    connection_string: &str,
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> io::Result<()> {
    // set up the postgres client for validation
    let mut client = postgres::Client::connect(connection_string, postgres::NoTls) // FIXME: conditionally enable TLS
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

    // compile the shared file descriptors
    let file_descriptor_set = compile_file_descriptors(protos, includes)?;

    for file in file_descriptor_set.file.iter() {
        // FIXME: double-check that this works with module imports from other files...
        // otherwise, consider flat-mapping all importable messages from across all files in the set
        // extract the message structs from the original file descriptor
        let messages = file
            .message_type
            .iter()
            .map(|descriptor| {
                (
                    format!(".{}.{}", file.package(), descriptor.name()),
                    descriptor,
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        // extract the enums from the original file descriptor
        let enums = file
            .enum_type
            .iter()
            .map(|descriptor| {
                (
                    format!(".{}.{}", file.package(), descriptor.name()),
                    descriptor,
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        // extract the postgrpc-annotated methods from the original file descriptor
        let methods = file
            .service
            .iter()
            .flat_map(|service| service.method.iter())
            .filter_map(|method| {
                super::proto::Method::from_method_descriptor(method, &messages, &enums).transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        // validate the inputs and outputs of each postgrpc-annotated method
        for method in methods {
            // FIXME: use proper logging
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

    prost_types::FileDescriptorSet::decode_with_extensions(&*buf, extension_registry).map_err(
        |error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid FileDescriptorSet: {error}"),
            )
        },
    )
}

#[cfg(test)]
mod test {
    use super::validate;
    use crate::setup;

    #[test]
    fn validates_inline_queries() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/inline_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_file_queries() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/file_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_scalar_fields() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/scalar_fields.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_enums() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/enums.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_composite_types() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/composites.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_fields_in_order() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/field_order.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ctes() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/cte.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ddl_changes() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/ddl.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap()
    }

    #[test]
    fn rejects_mismatched_field_names() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/mismatched_field_name.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with mismatched field name");
    }

    #[test]
    fn rejects_missing_messages() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/missing_message.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with missing message");
    }

    #[test]
    fn rejects_invalid_sql() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/invalid_sql.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with invalid SQL");
    }

    // FIXME: consider supporting pre-compiled transactions and multi-command statements
    // (for now, no... stick with CTEs instead)
    #[test]
    fn rejects_transactions() {
        setup::database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/transactions.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject multi-statement transaction");
    }

    // TOTEST:
    // validates_imported_descriptors
    // validates_json
    // validates_generic_records
    // validates_arrays
    // validates_repeated_fields
    // validates_well_known_types
}
