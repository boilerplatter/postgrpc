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

    // TOTEST:
    // validates_scalar_inputs
    // validates_scalar_outputs
    // rejects_missing_messages
    // rejects_invalid_sql
    // rejects_invalid_schemas
    // rejects_proto_sql_mismatches
    // validates_transactions
    // validates_enums
    // validates_json
    // validates_custom_types
}
