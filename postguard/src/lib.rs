//! Test Postgres-compatible statements against a set of CORS-like rules
#![deny(missing_docs, unreachable_pub)]

use inflector::Inflector;
use pg_query_rust::{
    node, AArrayExpr, AExpr, CaseExpr, CaseWhen, CommonTableExpr, DeleteStmt, InferClause,
    InsertStmt, JoinExpr, List, Node, OnConflictClause, RawStmt, ResTarget, SelectStmt, UpdateStmt,
};
use std::{
    fmt::{self, Formatter},
    str::FromStr,
};
use thiserror::Error;

#[cfg(test)]
mod test;

/// Errors related to statement rejection or parsing failures
#[derive(Debug, Error)]
pub enum Error {
    /// Query was rejected for intentionally-unspecific reasons, usually because of the presence
    /// of a generally-banned node in the provided statement
    #[error("Permission denied to execute query")]
    QueryRejected,
    /// A top-level statement was not allowed, usually because of an AllowedStatements restriction
    #[error("{0} statements are not allowed")]
    StatementNotAllowed(String),
    /// A named function was not allowed, usually because of an AllowedFunctions restriction
    #[error("Executing {0} function not allowed")]
    FunctionNotAllowed(String),
    /// Query was rejected because it contained a function without a valid string name
    #[error("{0} is not a valid function name")]
    InvalidFunctionName(String),
    /// There was an error during the parsing step. This means that the provided statement
    /// was not valid Postgres-flavored SQL
    #[error("Invalid query: {0}")]
    Parse(String),
}

impl From<pg_query_rust::Error> for Error {
    fn from(error: pg_query_rust::Error) -> Self {
        match error {
            pg_query_rust::Error::Conversion(error) => Self::Parse(error.to_string()),
            pg_query_rust::Error::Decode(error) => Self::Parse(error.to_string()),
            pg_query_rust::Error::Parse(error) => Self::Parse(error),
        }
    }
}

/// Possible Commands to filter statements by. These commands are intentionally restricted to basic
/// CRUD operations only.
#[allow(missing_docs)]
#[derive(Debug, PartialEq)]
pub enum Command {
    Select,
    Update,
    Insert,
    Delete,
}

impl fmt::Display for Command {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let output = match self {
            Self::Select => "SELECT",
            Self::Update => "UPDATE",
            Self::Insert => "INSERT",
            Self::Delete => "DELETE",
        };

        formatter.write_str(output)
    }
}

/// Configurations for Allowed Statements
#[derive(Debug)]
pub enum AllowedStatements {
    /// No restrictions on statements, equivalent to `*` in a CORS-like configuration. This setting
    /// means that statement filtering of any kind is disabled, so use only in trusted
    /// environments.
    All,
    /// Set of allowed commands. An empty set means that all statements are disallowed.
    List(Vec<Command>),
}

impl AllowedStatements {
    fn contains(&self, command: &Command) -> bool {
        match self {
            Self::All => true,
            Self::List(statements) => statements.contains(command),
        }
    }
}

impl FromStr for AllowedStatements {
    type Err = Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "*" => Ok(Self::All),
            source => {
                let mut statements = vec![];

                for statement in source.to_lowercase().split(',') {
                    match statement.trim() {
                        "select" => statements.push(Command::Select),
                        "update" => statements.push(Command::Update),
                        "insert" => statements.push(Command::Insert),
                        "delete" => statements.push(Command::Delete),
                        statement => return Err(Error::StatementNotAllowed(statement.to_string())),
                    }
                }

                statements.dedup();

                Ok(Self::List(statements))
            }
        }
    }
}

impl Default for AllowedStatements {
    //allow all statements by default
    fn default() -> Self {
        Self::All
    }
}

/// Configurations for Allowed Functions (by name)
#[derive(Debug)]
pub enum AllowedFunctions {
    /// No restrictions on function execution, equivalent to `*` in a CORS-like configuration. This setting
    /// means that function filtering of any kind is disabled, so use only in trusted
    /// environments.
    All,
    /// Set of allowed functions by name. An empty set means that function exeuction of any kind is
    /// disallowed.
    List(Vec<String>),
}

impl FromStr for AllowedFunctions {
    type Err = Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "*" => Ok(Self::All),
            source => {
                let mut functions = vec![];

                for function in source.to_lowercase().split(',') {
                    let function = function.trim().to_string();

                    if function.is_snake_case() {
                        functions.push(function)
                    } else {
                        return Err(Error::InvalidFunctionName(function));
                    }
                }

                Ok(Self::List(functions))
            }
        }
    }
}

impl Default for AllowedFunctions {
    // allow all functions by default
    fn default() -> Self {
        Self::All
    }
}

/// CORS-like statement analyzer for guarding queryable methods
#[derive(Default)]
pub struct Guard {
    allowed_statements: AllowedStatements,
    allowed_functions: AllowedFunctions,
}

impl Guard {
    /// Create a new Guard from CORS-like ALLOWED-* configurations
    pub fn new(allowed_statements: AllowedStatements, allowed_functions: AllowedFunctions) -> Self {
        Self {
            allowed_statements,
            allowed_functions,
        }
    }

    /// Test a statement against the configuration
    pub fn guard(&self, statement: &str) -> Result<(), Error> {
        match (&self.allowed_statements, &self.allowed_functions) {
            // handle the most-permissive case first
            (AllowedStatements::All, AllowedFunctions::All) => Ok(()),
            // all other cases need statement parsing
            _ => {
                let statements = pg_query_rust::parse(statement)?
                    .stmts
                    .into_iter()
                    .filter_map(|RawStmt { stmt, .. }| stmt);

                self.test(statements)
            }
        }
    }

    fn test_select(&self, statement: &SelectStmt) -> Result<(), Error> {
        let SelectStmt {
            distinct_clause,
            target_list,
            from_clause,
            where_clause,
            group_clause,
            having_clause,
            window_clause,
            values_lists,
            limit_offset,
            limit_count,
            locking_clause,
            with_clause,
            larg,
            rarg,
            ..
        } = statement;

        self.test(distinct_clause)?;
        self.test(target_list)?;
        self.test(from_clause)?;
        self.test(where_clause.as_deref())?;
        self.test(group_clause)?;
        self.test(having_clause.as_deref())?;
        self.test(window_clause)?;
        self.test(values_lists)?;
        self.test(limit_offset.as_deref())?;
        self.test(limit_count.as_deref())?;
        self.test(locking_clause)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        // test each side of UNION opts as select statements only
        if let Some(statement) = larg {
            self.test_select(statement.as_ref())?;
        }

        if let Some(statement) = rarg {
            self.test_select(statement.as_ref())?;
        }

        Ok(())
    }

    fn test_update(&self, statement: &UpdateStmt) -> Result<(), Error> {
        let UpdateStmt {
            target_list,
            where_clause,
            from_clause,
            returning_list,
            with_clause,
            ..
        } = statement;

        self.test(target_list)?;
        self.test(where_clause.as_deref())?;
        self.test(from_clause)?;
        self.test(returning_list)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        Ok(())
    }

    fn test_insert(&self, statement: &InsertStmt) -> Result<(), Error> {
        let InsertStmt {
            cols,
            select_stmt,
            on_conflict_clause,
            returning_list,
            with_clause,
            ..
        } = statement;

        self.test(cols)?;
        self.test(select_stmt.as_deref())?;
        self.test(returning_list)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        if let Some(on_conflict_clause) = on_conflict_clause {
            let OnConflictClause {
                infer,
                target_list,
                where_clause,
                ..
            } = on_conflict_clause.as_ref();

            self.test(target_list)?;
            self.test(where_clause.as_deref())?;

            if let Some(infer_clause) = infer {
                let InferClause {
                    index_elems,
                    where_clause,
                    ..
                } = infer_clause.as_ref();

                self.test(index_elems)?;
                self.test(where_clause.as_deref())?;
            }
        }

        Ok(())
    }

    fn test_delete(&self, statement: &DeleteStmt) -> Result<(), Error> {
        let DeleteStmt {
            using_clause,
            where_clause,
            returning_list,
            with_clause,
            ..
        } = statement;

        self.test(using_clause)?;
        self.test(where_clause.as_deref())?;
        self.test(returning_list)?;

        if let Some(with_clause) = with_clause {
            self.test(&with_clause.ctes)?;
        }

        Ok(())
    }

    fn test_a_expr(&self, a_expr: &AExpr) -> Result<(), Error> {
        let AExpr {
            name, lexpr, rexpr, ..
        } = a_expr;

        self.test(name)?;
        self.test(lexpr.as_deref())?;
        self.test(rexpr.as_deref())?;

        Ok(())
    }

    fn test_a_array_expr(&self, a_array_expr: &AArrayExpr) -> Result<(), Error> {
        let AArrayExpr { elements, .. } = a_array_expr;

        self.test(elements)
    }

    fn test_cte(&self, cte: &CommonTableExpr) -> Result<(), Error> {
        let CommonTableExpr {
            aliascolnames,
            ctequery,
            ctecolnames,
            ctecoltypes,
            ctecoltypmods,
            ctecolcollations,
            ..
        } = cte;

        self.test(aliascolnames)?;
        self.test(ctequery.as_deref())?;
        self.test(ctecolnames)?;
        self.test(ctecoltypes)?;
        self.test(ctecoltypmods)?;
        self.test(ctecolcollations)?;

        Ok(())
    }

    fn test_res_target(&self, target: &ResTarget) -> Result<(), Error> {
        let ResTarget {
            indirection, val, ..
        } = target;

        self.test(indirection)?;
        self.test(val.as_deref())?;

        Ok(())
    }

    fn test_list(&self, list: &List) -> Result<(), Error> {
        let List { items } = list;

        self.test(items)
    }

    fn test_case_when(&self, case_when: &CaseWhen) -> Result<(), Error> {
        let CaseWhen { expr, result, .. } = case_when;

        self.test(expr.as_deref())?;
        self.test(result.as_deref())?;

        Ok(())
    }

    fn test_case_expr(&self, case_expr: &CaseExpr) -> Result<(), Error> {
        let CaseExpr {
            arg,
            args,
            defresult,
            ..
        } = case_expr;

        self.test(arg.as_deref())?;
        self.test(args)?;
        self.test(defresult.as_deref())?;

        Ok(())
    }

    fn test_join_expr(&self, join_expr: &JoinExpr) -> Result<(), Error> {
        let JoinExpr {
            larg,
            rarg,
            using_clause,
            quals,
            alias,
            ..
        } = join_expr;

        self.test(larg.as_deref())?;
        self.test(rarg.as_deref())?;
        self.test(using_clause)?;
        self.test(quals.as_deref())?;

        if let Some(alias) = alias {
            self.test(&alias.colnames)?;
        }

        Ok(())
    }

    fn test<I, N>(&self, nodes: I) -> Result<(), Error>
    where
        N: std::borrow::Borrow<Node>,
        I: IntoIterator<Item = N>,
    {
        for node in nodes {
            if let Some(node) = &node.borrow().node {
                match node {
                    node::Node::SelectStmt(select_statement) => {
                        let command = Command::Select;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_select(select_statement)?;
                    }
                    node::Node::UpdateStmt(update_statement) => {
                        let command = Command::Update;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_update(update_statement)?;
                    }
                    node::Node::InsertStmt(insert_statement) => {
                        let command = Command::Insert;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_insert(insert_statement)?;
                    }
                    node::Node::DeleteStmt(delete_statement) => {
                        let command = Command::Delete;

                        if !self.allowed_statements.contains(&command) {
                            return Err(Error::StatementNotAllowed(command.to_string()));
                        }

                        self.test_delete(delete_statement)?;
                    }
                    node::Node::AExpr(a_expr) => {
                        self.test_a_expr(a_expr)?;
                    }
                    node::Node::AArrayExpr(a_array_expr) => {
                        self.test_a_array_expr(a_array_expr)?;
                    }
                    node::Node::CommonTableExpr(common_table_expression) => {
                        self.test_cte(common_table_expression)?;
                    }
                    node::Node::ResTarget(res_target) => {
                        self.test_res_target(res_target)?;
                    }
                    node::Node::List(list) => {
                        self.test_list(list)?;
                    }
                    node::Node::CaseWhen(case_when) => {
                        self.test_case_when(case_when)?;
                    }
                    node::Node::CaseExpr(case_expr) => {
                        self.test_case_expr(case_expr)?;
                    }
                    node::Node::JoinExpr(join_expr) => {
                        self.test_join_expr(join_expr)?;
                    }
                    // reject privileged statements and commands out-of-hand
                    node::Node::AlterCollationStmt(..)
                    | node::Node::AlterDatabaseSetStmt(..)
                    | node::Node::AlterDatabaseStmt(..)
                    | node::Node::AlterDefaultPrivilegesStmt(..)
                    | node::Node::AlterDomainStmt(..)
                    | node::Node::AlterEnumStmt(..)
                    | node::Node::AlterEventTrigStmt(..)
                    | node::Node::AlterExtensionContentsStmt(..)
                    | node::Node::AlterExtensionStmt(..)
                    | node::Node::AlterFdwStmt(..)
                    | node::Node::AlterForeignServerStmt(..)
                    | node::Node::AlterFunctionStmt(..)
                    | node::Node::AlterObjectDependsStmt(..)
                    | node::Node::AlterObjectSchemaStmt(..)
                    | node::Node::AlterOpFamilyStmt(..)
                    | node::Node::AlterOperatorStmt(..)
                    | node::Node::AlterOwnerStmt(..)
                    | node::Node::AlterPolicyStmt(..)
                    | node::Node::AlterPublicationStmt(..)
                    | node::Node::AlterRoleSetStmt(..)
                    | node::Node::AlterRoleStmt(..)
                    | node::Node::AlterSeqStmt(..)
                    | node::Node::AlterStatsStmt(..)
                    | node::Node::AlterSubscriptionStmt(..)
                    | node::Node::AlterSystemStmt(..)
                    | node::Node::AlterTableCmd(..)
                    | node::Node::AlterTableMoveAllStmt(..)
                    | node::Node::AlterTableSpaceOptionsStmt(..)
                    | node::Node::AlterTableStmt(..)
                    | node::Node::AlterTypeStmt(..)
                    | node::Node::AlterUserMappingStmt(..)
                    | node::Node::CheckPointStmt(..)
                    | node::Node::ClosePortalStmt(..)
                    | node::Node::ClusterStmt(..)
                    | node::Node::CommentStmt(..)
                    | node::Node::CopyStmt(..)
                    | node::Node::CreateAmStmt(..)
                    | node::Node::CreateCastStmt(..)
                    | node::Node::CreateConversionStmt(..)
                    | node::Node::CreateDomainStmt(..)
                    | node::Node::CreateEnumStmt(..)
                    | node::Node::CreateEventTrigStmt(..)
                    | node::Node::CreateExtensionStmt(..)
                    | node::Node::CreateFdwStmt(..)
                    | node::Node::CreateForeignServerStmt(..)
                    | node::Node::CreateForeignTableStmt(..)
                    | node::Node::CreateFunctionStmt(..)
                    | node::Node::CreateOpClassItem(..)
                    | node::Node::CreateOpClassStmt(..)
                    | node::Node::CreateOpFamilyStmt(..)
                    | node::Node::CreatePolicyStmt(..)
                    | node::Node::CreatePublicationStmt(..)
                    | node::Node::CreateRangeStmt(..)
                    | node::Node::CreateRoleStmt(..)
                    | node::Node::CreateSchemaStmt(..)
                    | node::Node::CreateSeqStmt(..)
                    | node::Node::CreateStatsStmt(..)
                    | node::Node::CreateStmt(..)
                    | node::Node::CreateSubscriptionStmt(..)
                    | node::Node::CreateTableAsStmt(..)
                    | node::Node::CreateTableSpaceStmt(..)
                    | node::Node::CreateTransformStmt(..)
                    | node::Node::CreateTrigStmt(..)
                    | node::Node::CreateUserMappingStmt(..)
                    | node::Node::CreatedbStmt(..)
                    | node::Node::DeallocateStmt(..)
                    | node::Node::DeclareCursorStmt(..)
                    | node::Node::DefElem(..)
                    | node::Node::DefineStmt(..)
                    | node::Node::DiscardStmt(..)
                    | node::Node::DoStmt(..)
                    | node::Node::DropOwnedStmt(..)
                    | node::Node::DropRoleStmt(..)
                    | node::Node::DropStmt(..)
                    | node::Node::DropSubscriptionStmt(..)
                    | node::Node::DropTableSpaceStmt(..)
                    | node::Node::DropUserMappingStmt(..)
                    | node::Node::DropdbStmt(..)
                    | node::Node::ExecuteStmt(..)
                    | node::Node::ExplainStmt(..)
                    | node::Node::FetchStmt(..)
                    | node::Node::GrantRoleStmt(..)
                    | node::Node::GrantStmt(..)
                    | node::Node::ImportForeignSchemaStmt(..)
                    | node::Node::ListenStmt(..)
                    | node::Node::LoadStmt(..)
                    | node::Node::LockStmt(..)
                    | node::Node::NotifyStmt(..)
                    | node::Node::PartitionCmd(..)
                    | node::Node::PartitionElem(..)
                    | node::Node::PartitionRangeDatum(..)
                    | node::Node::PartitionSpec(..)
                    | node::Node::PrepareStmt(..)
                    | node::Node::RawStmt(..)
                    | node::Node::ReassignOwnedStmt(..)
                    | node::Node::RefreshMatViewStmt(..)
                    | node::Node::ReindexStmt(..)
                    | node::Node::RenameStmt(..)
                    | node::Node::ReplicaIdentityStmt(..)
                    | node::Node::RoleSpec(..)
                    | node::Node::TransactionStmt(..)
                    | node::Node::TriggerTransition(..)
                    | node::Node::UnlistenStmt(..)
                    | node::Node::VacuumRelation(..)
                    | node::Node::VacuumStmt(..)
                    | node::Node::VariableSetStmt(..)
                    | node::Node::VariableShowStmt(..)
                    | node::Node::ViewStmt(..)
                    | node::Node::XmlSerialize(..) => {
                        tracing::warn!(node = ?&node, "rejected query due to presence of banned node");

                        match self.allowed_statements {
                            AllowedStatements::All => (),
                            AllowedStatements::List(..) => return Err(Error::QueryRejected),
                        }
                    }
                    node::Node::FuncCall(function_call) => {
                        let funcname = &function_call.funcname;

                        match &self.allowed_functions {
                            AllowedFunctions::All => (),
                            AllowedFunctions::List(functions) => {
                                // ban unnamed functions outright
                                let name =
                                    funcname.first().ok_or(Error::QueryRejected)?.node.as_ref();

                                match name {
                                    Some(node::Node::String(pg_query_rust::String {
                                        str: name,
                                    })) => {
                                        // check the function name against the list
                                        if !functions.contains(name) {
                                            return Err(Error::FunctionNotAllowed(
                                                name.to_string(),
                                            ));
                                        }
                                    }
                                    // reject function names that aren't strings
                                    _ => return Err(Error::QueryRejected),
                                }
                            }
                        }
                    }
                    node => {
                        tracing::debug!(node = ?&node, "Skipping node");
                    }
                }
            }
        }

        Ok(())
    }
}
