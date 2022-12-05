#[cfg(feature = "testing")]
mod test {
    use postgres::{Client, NoTls};
    use postgrpc_build::validate;
    use std::sync::Once;

    static DATABASE_SETUP: Once = Once::new();

    /// scaffold a test database once for tests
    fn set_up_database() {
        DATABASE_SETUP.call_once(|| {
            let mut client = Client::connect(
                "postgresql://postgres:supersecretpassword@localhost:5432",
                NoTls,
            )
            .unwrap();

            client
                .execute(
                    r#"do $$ begin
                        create type "Genre" as enum('FANTASY', 'SCI_FI', 'ROMANCE', 'NON_FICTION');
                    exception
                        when duplicate_object then null;
                    end $$"#,
                    &[],
                )
                .unwrap();

            client
                .execute(
                    r#"do $$ begin
                        create type "NestedGenre" as enum('FANTASY', 'SCI_FI', 'ROMANCE', 'NON_FICTION');
                    exception
                        when duplicate_object then null;
                    end $$"#,
                    &[],
                )
                .unwrap();

            client
                .execute(
                    r#"create table if not exists authors (
                        id serial primary key,
                        first_name text not null,
                        last_name text not null,
                        preferred_genre "Genre" not null
                    )"#,
                    &[],
                )
                .unwrap();
        });
    }

    #[test]
    fn validates_inline_queries() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/inline_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_file_queries() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/file_query.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_scalar_fields() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/scalar_fields.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_enums() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/enums.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_fields_in_order() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/field_order.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ctes() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/cte.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap();
    }

    #[test]
    fn validates_ddl_changes() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/ddl.proto"],
            &["./tests/proto", "./proto"],
        )
        .unwrap()
    }

    #[test]
    fn rejects_missing_messages() {
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/missing_message.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject method with missing message");
    }

    #[test]
    fn rejects_invalid_sql() {
        set_up_database();

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
        set_up_database();

        validate(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            &["./tests/proto/transactions.proto"],
            &["./tests/proto", "./proto"],
        )
        .expect_err("Failed to reject multi-statement transaction");
    }

    // TOTEST:
    // validates_imported_descriptors
    // validates_nested_descriptors
    // validates_json
    // validates_custom_types
    // validates_generic_records
    // validates_arrays
    // validates_repeated_fields
}
