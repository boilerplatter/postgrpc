use postgres::{Client, NoTls};
use std::sync::Once;

static DATABASE_SETUP: Once = Once::new();

/// scaffold a test database once for tests
pub(crate) fn database() {
    DATABASE_SETUP.call_once(|| {
        let mut client = Client::connect(
            "postgresql://postgres:supersecretpassword@localhost:5432",
            NoTls,
        )
        .unwrap();

        // create custom genre enums
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

        // create custom book composite types
        client
            .execute(
                r#"do $$ begin
                    create type "Book" as (
                        name text,
                        genre "Genre"
                    );
                exception
                    when duplicate_object then null;
                end $$"#,
                &[],
            )
            .unwrap();

        client
            .execute(
                r#"do $$ begin
                    create type "NestedBook" as (
                        name text,
                        genre "Genre"
                    );
                exception
                    when duplicate_object then null;
                end $$"#,
                &[],
            )
            .unwrap();

        // create the test authors table
        client
            .execute(
                r#"create table if not exists authors (
                    id serial primary key,
                    first_name text not null,
                    last_name text not null,
                    preferred_genre "Genre" not null,
                    favorite_book "Book" not null
                )"#,
                &[],
            )
            .unwrap();
    });
}
