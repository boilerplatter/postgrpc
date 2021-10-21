# Postguard
[![Latest Version](https://img.shields.io/crates/v/postguard.svg)](https://crates.io/crates/postguard)
[![Documentation](https://docs.rs/postguard/badge.svg)](https://docs.rs/postguard)
[![Crates.io](https://img.shields.io/crates/l/postguard)](LICENSE)

Test Postgres-compatible statements against a set of CORS-like rules

## Why

Postgres has a rich `ROLE` system for managing privileges around data in a database.
But those privileges are often too permissive by default, and are difficult to restrict by statement or function name.

`postguard` provides a `Guard` statement analyzer for protecting databases from malicious or invalid queries.
This `Guard` can be used in any Rust application that has access to statements (perhaps from untrusted sources) before they are run.
Under the hood, `postguard` uses the [`libpg_query`](https://github.com/pganalyze/libpg_query) library to parse queries down to a syntax tree
before checking the entire tree for disallowed nodes.

## Installation

Add `postguard` to your `Cargo.toml`: 

```toml
[dependencies]
postguard = "0.1"
```

## Usage

```rust
use postguard::{AllowedFunctions, AllowedStatements, Command, Guard};

// If AllowedFunctions and AllowedStatements both are set to their 'All' variants
// then no parsing is done and all statements pass the guard
#[test]
fn it_does_nothing_by_default() {
    Guard::new(AllowedStatements::All, AllowedFunctions::All)
        .guard("create table test (id serial primary key)")
        .expect("all statements are permitted by default");
}

// Statements are checked against the list of allowed statements when a 'List' variant
// is provided. Statement-checking is done recursively, so nested disallowed statements
// are also caught by the guard
#[test]
fn it_restricts_statements() {
    let statement_guard = Guard::new(AllowedStatements::List(vec![Command::Select]));

    statement_guard
        .guard("create table test (id serial primary key)")
        .expect_err("privileged statements are restricted");

    statement_guard
        .guard("select * from items")
        .expect("select statements are permitted");

    statement_guard
        .guard("insert into items default values")
        .expect_err("commands in top-level queries are restricted");

    statement_guard
        .guard("
            with cte as (
                insert into items default values
                returning id
            )
            select * from cte
        ")
        .expect_err("disallowed commands in nested queries or expressions are restricted");
}

// Functions are also guarded by name. To disallow all functions, leave the 'List' empty.
#[test]
fn it_restricts_functions() {
    let statement_guard = Guard::new(
        AllowedStatements::List(vec![Command::Select]),
        AllowedFunctions::List(vec!["uuid_generate_v4".to_string()])
    );

    statement_guard
        .guard("select pg_sleep(1)")
        .expect_err("built-in functions and stored procs are restricted");

    statement_guard
        .guard("select uuid_generate_v4() as id")
        .expect("allowed functions are permitted");
}
```
