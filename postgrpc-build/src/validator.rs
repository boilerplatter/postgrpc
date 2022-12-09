use crate::proto::Services;
use std::io;

/// Validate a set of `postgrpc`-annotated Services against a database
pub(crate) fn validate_services(connection_string: &str, services: &Services) -> io::Result<()> {
    // set up TLS connectors
    #[cfg(feature = "ssl-native-tls")]
    let tls_connector = {
        let connector = native_tls::TlsConnector::builder()
            .build()
            .map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error.to_string()))?;

        postgres_native_tls::MakeTlsConnector::new(connector)
    };

    #[cfg(not(feature = "ssl-native-tls"))]
    let tls_connector = postgres::NoTls;

    // set up the postgres client for validation
    let mut client = postgres::Client::connect(connection_string, tls_connector)
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

    // validate the inputs and outputs of each postgrpc-annotated method
    for service in services {
        let methods = service
            .as_ref()
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?
            .methods();

        for method in methods {
            // prepare the method's SQL query with the database
            let statement = client
                .prepare(method.query())
                .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

            // check the input and output types against database types
            method.validate_input(statement.params())?;
            method.validate_output(statement.columns())?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::validate_services;
    use crate::{protoc::compile_services, setup};
    use std::path::Path;

    fn validate(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>]) -> std::io::Result<()> {
        let services = compile_services(protos, includes)?;

        validate_services(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &services,
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
