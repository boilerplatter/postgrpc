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

impl From<libpgquery_sys::Error> for Error {
    fn from(error: libpgquery_sys::Error) -> Self {
        match error {
            libpgquery_sys::Error::Conversion(error) => Self::Parse(error.to_string()),
            libpgquery_sys::Error::Decode(error) => Self::Parse(error.to_string()),
            libpgquery_sys::Error::Parse(error) => Self::Parse(error),
        }
    }
}
