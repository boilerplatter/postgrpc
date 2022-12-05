#[cfg(feature = "testing")]
mod test {
    use postgrpc_build::validate;

    // FIXME: include common setup code for starting up a database via docker

    #[test]
    fn validates_inline_queries() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/inline_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_file_queries() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/file_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_scalar_fields() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/scalar_fields.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_enums() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/enums.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ctes() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/cte.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ddl_changes() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/ddl.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap()
    }

    #[test]
    fn rejects_missing_messages() {
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/missing_message.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with missing message");
    }

    #[test]
    fn rejects_invalid_sql() {
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
        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/transactions.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject multi-statement transaction");
    }

    // TOTEST:
    // validates_field_param_order
    // validates_imported_descriptors
    // validates_nested_descriptors
    // validates_json
    // validates_custom_types
    // validates_generic_records
    // validates_arrays
}
