use std::fmt;
use tonic::{Request, Status};

/// Extension for extracting optional Postgres ROLEs from request headers
#[cfg_attr(doc, doc(cfg(feature = "role-header")))]
#[cfg(any(doc, feature = "role-header"))]
pub mod role_header;

/// Helper trait to encapsulate logic for deriving extension values from gRPC requests
pub trait FromRequest
where
    Self: Sized,
{
    /// Errors associated with deriving a value from a gRPC request
    type Error: std::error::Error + Into<Status>;

    /// Derive a value from a gRPC request
    fn from_request<T>(request: &mut Request<T>) -> Result<Self, Self::Error>;
}

/// Dummy error for default impl of derivation of unit structs from requests
#[derive(Debug)]
pub struct UnitConversion;

impl fmt::Display for UnitConversion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for UnitConversion {}

impl From<UnitConversion> for Status {
    fn from(_: UnitConversion) -> Self {
        Self::internal("Infallible unit conversion somehow failed")
    }
}

impl FromRequest for () {
    type Error = UnitConversion;

    fn from_request<T>(_: &mut Request<T>) -> Result<Self, Self::Error> {
        Ok(())
    }
}
