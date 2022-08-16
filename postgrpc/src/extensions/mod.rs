//!
//!
//! These modules export types that can be used as connection access `Key`s in a custom `Pool` implementation,
//! as well as the interceptors required to generate those `Key`s from a [`tonic::Request`].
//!
//! #### `role-header` Example:
//! ```rust
//! // requires the "role-header" feature
//! use postgrpc::{extensions, pools::Pool, services};
//! use tonic::codegen::InterceptedService;
//!
//! // somewhere in a tonic app
//! services::postgres::with_interceptor(pool, extensions::role_header::interceptor)
//! ```

use std::fmt;
use tonic::{Request, Status};

/// Extension for extracting optional Postgres `ROLE`s from [`tonic::Request`]s.
#[cfg_attr(doc, doc(cfg(feature = "role-header")))]
#[cfg(any(doc, feature = "role-header"))]
pub mod role_header;

/// Helper trait to encapsulate logic for deriving extension values from [`tonic::Request`]s.
///
/// This trait is used when implementing connection `Key`s for custom connection pools.
pub trait FromRequest
where
    Self: Sized,
{
    /// Errors associated with deriving a value from a gRPC request.
    type Error: std::error::Error + Into<Status>;

    /// Derive a value from a [`tonic::Request`].
    fn from_request<T>(request: &mut Request<T>) -> Result<Self, Self::Error>;
}

/// Dummy error for default impl of derivation of unit structs from requests.
///
/// This `impl` of [`FromRequest`] for `()` makes it easy to create `Key`-less connection pools.
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
