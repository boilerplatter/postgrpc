//! Test Postgres-compatible statements against a set of CORS-like rules
#![deny(missing_docs, unreachable_pub)]

use inflector::Inflector;
use pg_query::ast::{
    CommonTableExpr, DeleteStmt, FuncCall, InferClause, InsertStmt, List, Node, OnConflictClause,
    ResTarget, SelectStmt, UpdateStmt,
};
use std::{
    fmt::{self, Formatter},
    str::FromStr,
};
use thiserror::Error;

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

impl From<pg_query::Error> for Error {
    fn from(error: pg_query::Error) -> Self {
        match error {
            pg_query::Error::ParseError(error)
            | pg_query::Error::InvalidAst(error)
            | pg_query::Error::InvalidJson(error) => Self::Parse(error),
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
    // TODO: test performance with fingerprinted statement cache
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
                let nodes = pg_query::parse(statement)?;

                self.test(&nodes)
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

        // test each part of the Select statement recursively
        if let Some(nodes) = distinct_clause {
            self.test(&nodes)?;
        }

        if let Some(nodes) = target_list {
            self.test(&nodes)?;
        }

        if let Some(nodes) = from_clause {
            self.test(&nodes)?;
        }

        if let Some(node) = where_clause {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = group_clause {
            self.test(&nodes)?;
        }

        if let Some(node) = having_clause {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = window_clause {
            self.test(&nodes)?;
        }

        if let Some(nodes) = values_lists {
            self.test(&nodes)?;
        }

        if let Some(node) = limit_offset {
            self.test(&[&**node])?;
        }

        if let Some(node) = limit_count {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = locking_clause {
            self.test(&nodes)?;
        }

        if let Some(nodes) = with_clause.as_ref().and_then(|with| with.ctes.as_ref()) {
            self.test(&nodes)?;
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

        if let Some(nodes) = target_list {
            self.test(&nodes)?;
        }

        if let Some(node) = where_clause {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = from_clause {
            self.test(&nodes)?;
        }

        if let Some(nodes) = returning_list {
            self.test(&nodes)?;
        }

        if let Some(nodes) = with_clause.as_ref().and_then(|with| with.ctes.as_ref()) {
            self.test(&nodes)?;
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

        if let Some(nodes) = cols {
            self.test(&nodes)?;
        }

        if let Some(node) = select_stmt {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = returning_list {
            self.test(&nodes)?;
        }

        if let Some(nodes) = with_clause.as_ref().and_then(|with| with.ctes.as_ref()) {
            self.test(&nodes)?;
        }

        if let Some(on_conflict_clause) = on_conflict_clause {
            let OnConflictClause {
                infer,
                target_list,
                where_clause,
                ..
            } = on_conflict_clause.as_ref();

            if let Some(nodes) = target_list {
                self.test(&nodes)?;
            }

            if let Some(node) = where_clause {
                self.test(&[&**node])?;
            }

            if let Some(infer_clause) = infer {
                let InferClause {
                    index_elems,
                    where_clause,
                    ..
                } = infer_clause.as_ref();

                if let Some(nodes) = index_elems {
                    self.test(&nodes)?;
                }

                if let Some(node) = where_clause {
                    self.test(&[&**node])?;
                }
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

        if let Some(nodes) = using_clause {
            self.test(&nodes)?;
        }

        if let Some(node) = where_clause {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = returning_list {
            self.test(&nodes)?;
        }

        if let Some(nodes) = with_clause.as_ref().and_then(|with| with.ctes.as_ref()) {
            self.test(&nodes)?;
        }

        Ok(())
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

        if let Some(nodes) = aliascolnames {
            self.test(&nodes)?;
        }

        if let Some(node) = ctequery {
            self.test(&[&**node])?;
        }

        if let Some(nodes) = ctecolnames {
            self.test(&nodes)?;
        }

        if let Some(nodes) = ctecoltypes {
            self.test(&nodes)?;
        }

        if let Some(nodes) = ctecoltypmods {
            self.test(&nodes)?;
        }

        if let Some(nodes) = ctecolcollations {
            self.test(&nodes)?;
        }

        Ok(())
    }

    fn test_res_target(&self, target: &ResTarget) -> Result<(), Error> {
        let ResTarget {
            indirection, val, ..
        } = target;

        if let Some(nodes) = indirection {
            self.test(&nodes)?;
        }

        if let Some(node) = val {
            self.test(&[&**node])?;
        }

        Ok(())
    }

    fn test_list(&self, list: &List) -> Result<(), Error> {
        let List { items } = list;

        if let Some(nodes) = items {
            self.test(&nodes)?;
        }

        Ok(())
    }

    fn test<N>(&self, nodes: &[N]) -> Result<(), Error>
    where
        N: std::borrow::Borrow<Node> + fmt::Debug,
    {
        for node in nodes {
            match node.borrow() {
                Node::SelectStmt(select_statement) => {
                    let command = Command::Select;

                    if !self.allowed_statements.contains(&command) {
                        return Err(Error::StatementNotAllowed(command.to_string()));
                    }

                    self.test_select(select_statement)?;
                }
                Node::UpdateStmt(update_statement) => {
                    let command = Command::Update;

                    if !self.allowed_statements.contains(&command) {
                        return Err(Error::StatementNotAllowed(command.to_string()));
                    }

                    self.test_update(update_statement)?;
                }
                Node::InsertStmt(insert_statement) => {
                    let command = Command::Insert;

                    if !self.allowed_statements.contains(&command) {
                        return Err(Error::StatementNotAllowed(command.to_string()));
                    }

                    self.test_insert(insert_statement)?;
                }
                Node::DeleteStmt(delete_statement) => {
                    let command = Command::Delete;

                    if !self.allowed_statements.contains(&command) {
                        return Err(Error::StatementNotAllowed(command.to_string()));
                    }

                    self.test_delete(delete_statement)?;
                }
                Node::CommonTableExpr(common_table_expression) => {
                    self.test_cte(common_table_expression)?;
                }
                Node::ResTarget(res_target) => {
                    self.test_res_target(res_target)?;
                }
                Node::List(list) => {
                    self.test_list(list)?;
                }
                // reject privileged statements and commands out-of-hand
                Node::AlterCollationStmt(..)
                | Node::AlterDatabaseSetStmt(..)
                | Node::AlterDatabaseStmt(..)
                | Node::AlterDefaultPrivilegesStmt(..)
                | Node::AlterDomainStmt(..)
                | Node::AlterEnumStmt(..)
                | Node::AlterEventTrigStmt(..)
                | Node::AlterExtensionContentsStmt(..)
                | Node::AlterExtensionStmt(..)
                | Node::AlterFdwStmt(..)
                | Node::AlterForeignServerStmt(..)
                | Node::AlterFunctionStmt(..)
                | Node::AlterObjectDependsStmt(..)
                | Node::AlterObjectSchemaStmt(..)
                | Node::AlterOpFamilyStmt(..)
                | Node::AlterOperatorStmt(..)
                | Node::AlterOwnerStmt(..)
                | Node::AlterPolicyStmt(..)
                | Node::AlterPublicationStmt(..)
                | Node::AlterRoleSetStmt(..)
                | Node::AlterRoleStmt(..)
                | Node::AlterSeqStmt(..)
                | Node::AlterStatsStmt(..)
                | Node::AlterSubscriptionStmt(..)
                | Node::AlterSystemStmt(..)
                | Node::AlterTSConfigurationStmt(..)
                | Node::AlterTSDictionaryStmt(..)
                | Node::AlterTableCmd(..)
                | Node::AlterTableMoveAllStmt(..)
                | Node::AlterTableSpaceOptionsStmt(..)
                | Node::AlterTableStmt(..)
                | Node::AlterTypeStmt(..)
                | Node::AlterUserMappingStmt(..)
                | Node::CheckPointStmt(..)
                | Node::ClosePortalStmt(..)
                | Node::ClusterStmt(..)
                | Node::CommentStmt(..)
                | Node::CopyStmt(..)
                | Node::CreateAmStmt(..)
                | Node::CreateCastStmt(..)
                | Node::CreateConversionStmt(..)
                | Node::CreateDomainStmt(..)
                | Node::CreateEnumStmt(..)
                | Node::CreateEventTrigStmt(..)
                | Node::CreateExtensionStmt(..)
                | Node::CreateFdwStmt(..)
                | Node::CreateForeignServerStmt(..)
                | Node::CreateForeignTableStmt(..)
                | Node::CreateFunctionStmt(..)
                | Node::CreateOpClassItem(..)
                | Node::CreateOpClassStmt(..)
                | Node::CreateOpFamilyStmt(..)
                | Node::CreatePLangStmt(..)
                | Node::CreatePolicyStmt(..)
                | Node::CreatePublicationStmt(..)
                | Node::CreateRangeStmt(..)
                | Node::CreateRoleStmt(..)
                | Node::CreateSchemaStmt(..)
                | Node::CreateSeqStmt(..)
                | Node::CreateStatsStmt(..)
                | Node::CreateStmt(..)
                | Node::CreateSubscriptionStmt(..)
                | Node::CreateTableAsStmt(..)
                | Node::CreateTableSpaceStmt(..)
                | Node::CreateTransformStmt(..)
                | Node::CreateTrigStmt(..)
                | Node::CreateUserMappingStmt(..)
                | Node::CreatedbStmt(..)
                | Node::DeallocateStmt(..)
                | Node::DeclareCursorStmt(..)
                | Node::DefElem(..)
                | Node::DefineStmt(..)
                | Node::DiscardStmt(..)
                | Node::DoStmt(..)
                | Node::DropOwnedStmt(..)
                | Node::DropRoleStmt(..)
                | Node::DropStmt(..)
                | Node::DropSubscriptionStmt(..)
                | Node::DropTableSpaceStmt(..)
                | Node::DropUserMappingStmt(..)
                | Node::DropdbStmt(..)
                | Node::ExecuteStmt(..)
                | Node::ExplainStmt(..)
                | Node::FetchStmt(..)
                | Node::GrantRoleStmt(..)
                | Node::GrantStmt(..)
                | Node::ImportForeignSchemaStmt(..)
                | Node::ListenStmt(..)
                | Node::LoadStmt(..)
                | Node::LockStmt(..)
                | Node::NotifyStmt(..)
                | Node::PartitionCmd(..)
                | Node::PartitionElem(..)
                | Node::PartitionRangeDatum(..)
                | Node::PartitionSpec(..)
                | Node::PrepareStmt(..)
                | Node::RawStmt(..)
                | Node::ReassignOwnedStmt(..)
                | Node::RefreshMatViewStmt(..)
                | Node::ReindexStmt(..)
                | Node::RenameStmt(..)
                | Node::ReplicaIdentityStmt(..)
                | Node::RoleSpec(..)
                | Node::TransactionStmt(..)
                | Node::TriggerTransition(..)
                | Node::UnlistenStmt(..)
                | Node::VacuumRelation(..)
                | Node::VacuumStmt(..)
                | Node::VariableSetStmt(..)
                | Node::VariableShowStmt(..)
                | Node::ViewStmt(..)
                | Node::XmlSerialize(..) => {
                    tracing::warn!(node = ?&node, "rejected query due to precense of banned node");

                    match self.allowed_statements {
                        AllowedStatements::All => (),
                        AllowedStatements::List(..) => return Err(Error::QueryRejected),
                    }
                }
                Node::FuncCall(FuncCall { funcname, .. }) => match &self.allowed_functions {
                    AllowedFunctions::All => (),
                    AllowedFunctions::List(functions) => {
                        // ban unnamed functions outright
                        let name = funcname
                            .as_ref()
                            .ok_or(Error::QueryRejected)?
                            .first()
                            .ok_or(Error::QueryRejected)?;

                        match name {
                            Node::String { value: Some(name) } => {
                                // check the function name against the list
                                if !functions.contains(name) {
                                    return Err(Error::FunctionNotAllowed(name.to_string()));
                                }
                            }
                            // reject function names that aren't strings
                            _ => return Err(Error::QueryRejected),
                        }
                    }
                },
                node => {
                    tracing::debug!(node = ?&node, "Skipping node");
                }
            }
        }

        Ok(())
    }
}
