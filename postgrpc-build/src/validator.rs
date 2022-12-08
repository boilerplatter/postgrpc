use crate::protoc::Protos;
use std::io;

/// Validate a set of `postgrpc`-annotated protos against a database
pub(crate) fn validate_protos(connection_string: &str, protos: &Protos) -> io::Result<()> {
    // set up the postgres client for validation
    let mut client = postgres::Client::connect(connection_string, postgres::NoTls) // FIXME: conditionally enable TLS
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

    // validate the inputs and outputs of each postgrpc-annotated method
    for methods in protos.borrow_services().values() {
        for method in methods
            .as_ref()
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?
            .values()
        {
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

#[cfg(test)]
mod test {
    use super::validate_protos;
    use crate::{protoc::compile_protos, setup};
    use std::path::Path;

    fn validate(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>]) -> std::io::Result<()> {
        let protos = compile_protos(protos, includes)?;

        validate_protos(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &protos,
        )
    }

    #[test]
    fn validates_inline_queries() {
        setup::database();

        validate(
            &["./tests/proto/inline_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_file_queries() {
        setup::database();

        validate(
            &["./tests/proto/file_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_scalar_fields() {
        setup::database();

        validate(
            &["./tests/proto/scalar_fields.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_enums() {
        setup::database();

        validate(
            &["./tests/proto/enums.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_composite_types() {
        setup::database();

        validate(
            &["./tests/proto/composites.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_fields_in_order() {
        setup::database();

        validate(
            &["./tests/proto/field_order.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ctes() {
        setup::database();

        validate(&["./tests/proto/cte.proto"], &["./tests/proto", "./proto"]).unwrap();
    }

    #[test]
    fn validates_ddl_changes() {
        setup::database();

        validate(&["./tests/proto/ddl.proto"], &["./tests/proto", "./proto"]).unwrap()
    }

    #[test]
    fn rejects_mismatched_field_names() {
        setup::database();

        validate(
            &["./tests/proto/mismatched_field_name.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with mismatched field name");
    }

    #[test]
    fn rejects_missing_messages() {
        setup::database();

        validate(
            &["./tests/proto/missing_message.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with missing message");
    }

    #[test]
    fn rejects_invalid_sql() {
        setup::database();

        validate(
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
