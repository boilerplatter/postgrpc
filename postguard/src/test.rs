use super::{AllowedFunctions, AllowedStatements, Command, Error, Guard};
use once_cell::sync::Lazy;
use serde::Deserialize;
use tracing_test::traced_test;

/// Structured Function type for configuring test functions
#[derive(Debug, Deserialize, PartialEq)]
struct Function {
    name: String,
    arguments: Vec<serde_json::Value>,
}

impl Function {
    fn to_query(&self, command: &str) -> String {
        let argument_list = self
            .arguments
            .iter()
            .map(|argument| argument.to_string())
            .collect::<Vec<_>>()
            .join(",");

        format!("{} {}({})", command, self.name, argument_list)
    }
}

/// Individual SQL files for complex query testing
static INSERT_CTE: &str = include_str!("../data/insert_cte.sql");
static INSERT_SELECT_VALUES: &str = include_str!("../data/insert_select_values.sql");
const CASE_CARDINALITY_SUBQUERY: &str = include_str!("../data/case_cardinality_subquery.sql");
static COMPLEX_QUERIES: [&str; 3] = [INSERT_CTE, INSERT_SELECT_VALUES, CASE_CARDINALITY_SUBQUERY];

/// Grouped simple query components from JSON files
static INVALID_QUERIES: Lazy<Vec<String>> = Lazy::new(|| {
    serde_json::from_str(include_str!("../data/invalid_queries.json"))
        .expect("error reading query data from file")
});

static SIMPLE_QUERIES: Lazy<Vec<String>> = Lazy::new(|| {
    serde_json::from_str(include_str!("../data/simple_queries.json"))
        .expect("error reading query data from file")
});

static PRIVILEGED_QUERIES: Lazy<Vec<String>> = Lazy::new(|| {
    serde_json::from_str(include_str!("../data/privileged_queries.json"))
        .expect("error reading query data from file")
});

static FUNCTIONS: Lazy<Vec<Function>> = Lazy::new(|| {
    serde_json::from_str(include_str!("../data/functions.json"))
        .expect("error reading query data from file")
});

#[test]
#[traced_test]
fn does_nothing_without_restrictions() {
    // set up guard without restrictions
    let guard = Guard::default();

    // verify that even bad queries pass
    for query in &*INVALID_QUERIES {
        guard
            .guard(query)
            .expect("Rejected query even without restrictions")
    }
}

#[test]
#[traced_test]
fn rejects_invalid_queries() {
    // set up guard with limited restrictions
    let guard = Guard::new(AllowedStatements::List(vec![]), AllowedFunctions::All);

    // verify that invalid queries fail at the parsing step
    for query in &*INVALID_QUERIES {
        match guard.guard(query) {
            Err(Error::Parse(..)) => (),
            Ok(..) => panic!("Failed to reject invalid query"),
            Err(..) => panic!("Failed to reject invalid query at the parsing step"),
        }
    }
}

#[test]
#[traced_test]
fn parses_simple_queries() {
    // set up guard that does some query parsing
    let guard = Guard::new(
        AllowedStatements::List(vec![
            Command::Select,
            Command::Insert,
            Command::Update,
            Command::Delete,
        ]),
        AllowedFunctions::All,
    );

    // verify that every request to guard succeeds
    for query in &*SIMPLE_QUERIES {
        guard.guard(query).expect("Failed to parse valid query");
    }
}

#[test]
fn rejects_privileged_queries() {
    // set up guard that does some query parsing
    let guard = Guard::new(
        AllowedStatements::List(vec![
            Command::Select,
            Command::Insert,
            Command::Update,
            Command::Delete,
        ]),
        AllowedFunctions::All,
    );

    // assert that every privileged query fails
    for query in &*PRIVILEGED_QUERIES {
        guard
            .guard(query)
            .expect_err("Failed to reject privileged query");
    }
}

#[test]
fn parses_complex_queries() {
    // set up guard that does some query parsing
    let guard = Guard::new(
        AllowedStatements::List(vec![
            Command::Select,
            Command::Insert,
            Command::Update,
            Command::Delete,
        ]),
        AllowedFunctions::All,
    );

    // assert that every privileged query fails
    for query in COMPLEX_QUERIES {
        guard.guard(query).expect("Failed to parse valid query");
    }
}

#[test]
fn selectively_rejects_disallowed_statements_in_ctes() {
    // set up guard that only accepts select statements
    let guard = Guard::new(
        AllowedStatements::List(vec![Command::Select]),
        AllowedFunctions::All,
    );

    // assert that filters apply to CTEs
    guard
        .guard(INSERT_CTE)
        .expect_err("Failed to reject disallowed statement in CTE");
}

#[test]
#[traced_test]
fn rejects_top_level_disallowed_functions() {
    // set up guard that rejects all functions
    let guard = Guard::new(AllowedStatements::All, AllowedFunctions::List(vec![]));

    // assert that every function is rejected
    for function in &*FUNCTIONS {
        guard
            .guard(&function.to_query("select"))
            .expect_err("Failed to reject function invocation");
    }
}

#[test]
#[traced_test]
fn rejects_nested_disallowed_functions() {
    // set up guard that rejects all functions
    let guard = Guard::new(AllowedStatements::All, AllowedFunctions::List(vec![]));

    // assert that every function is rejected in a complex query
    guard
        .guard(CASE_CARDINALITY_SUBQUERY)
        .expect_err("Failed to reject function invocation");
}

#[test]
#[traced_test]
fn selectively_rejects_disallowed_functions() {
    // set up guard that rejects all functions but one
    let allowed_function = &FUNCTIONS[0];

    let guard = Guard::new(
        AllowedStatements::All,
        AllowedFunctions::List(vec![allowed_function.name.clone()]),
    );

    // assert that every function is rejected
    for function in &*FUNCTIONS {
        let result = guard.guard(&function.to_query("select"));

        if function == allowed_function {
            result.expect("Failed to allow permitted function");
        } else {
            result.expect_err("Failed to reject function invocation");
        }
    }
}
