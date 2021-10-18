use super::{AllowedFunctions, AllowedStatements, Command, Error, Guard};
use tracing_test::traced_test;

const INVALID_QUERIES: [&str; 4] = [
    "bad query",
    "alter 1234",
    "notify channel foo",
    "delete * from items",
];

const SIMPLE_QUERIES: [&str; 4] = [
    "select * from items",
    "insert into items (id, name) values ($1, $2)",
    "update items set id = $1 where name = $2",
    "delete from items",
];

const PRIVILEGED_QUERIES: [&str; 5] = [
    "set statement_timeout=123",
    "create table items (id serial primary key)",
    "create database testdb owner postgres",
    "listen channel",
    r#"notify channel '{"hello":"world"}'"#,
];

const INSERT_CTE: &str = "
    with cte as (
        insert into items (id, name)
        values ($1, $2)
        returning id
    )
    select id from cte";

const SELECT_VALUES: &str = "
    insert into items (name)
    select name from users";

const CASE_CARDINALITY_SUBQUERY: &str = "
    select
      tree.id,
      tree.name,
      case
        when cardinality(array_remove(array_agg(activations), null)) > 0
        then bool_or(activations.deactivated_at is null)
        else false
      end as active,
      (
        select name
        from branches
        where branches.id = tree.trunk_branch_id
      ) as trunk_branch_name
    from tree
      left join branches on branches.virtualized_instance_id = tree.id
      left join activations on activations.branch_id = branches.id
    where user_id = $1
    group by tree.id";

const COMPLEX_QUERIES: [&str; 3] = [INSERT_CTE, SELECT_VALUES, CASE_CARDINALITY_SUBQUERY];

const FUNCTIONS: [&str; 5] = [
    "to_json(1)",
    "pg_sleep(1)",
    "pg_terminate_backend(1)",
    "uuid_generate_v4()",
    "my_stored_proc()",
];

#[test]
#[traced_test]
fn does_nothing_without_restrictions() {
    // set up guard without restrictions
    let guard = Guard::default();

    // verify that even bad queries pass
    for query in INVALID_QUERIES {
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
    for query in INVALID_QUERIES {
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
    for query in SIMPLE_QUERIES {
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
    for query in PRIVILEGED_QUERIES {
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
    for function in FUNCTIONS {
        let query = format!("select {}", function);
        guard
            .guard(&query)
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
        .guard(&CASE_CARDINALITY_SUBQUERY)
        .expect_err("Failed to reject function invocation");
}

#[test]
#[traced_test]
fn selectively_rejects_disallowed_functions() {
    // set up guard that rejects all functions but to_json
    let to_json = "to_json";
    let guard = Guard::new(
        AllowedStatements::All,
        AllowedFunctions::List(vec![to_json.to_string()]),
    );

    // assert that every function is rejected
    for function in FUNCTIONS {
        let query = format!("select {}", &function);
        let result = guard.guard(&query);

        if function.to_lowercase().starts_with(to_json) {
            result.expect("Failed to allow permitted function");
        } else {
            result.expect_err("Failed to reject function invocation");
        }
    }
}
