use super::FromRequest;
use tonic::{Request, Status};

const ROLE_HEADER: &str = "x-postgres-role";

/// Type alias for an optionally-provided Postgres `ROLE` that connection pools can use as a key
pub type Role = Option<String>;

impl FromRequest for Role {
    type Error = Status;

    fn from_request<T>(request: &mut Request<T>) -> Result<Self, Self::Error> {
        let role = request
            .extensions_mut()
            .remove::<RoleExtension>()
            .ok_or_else(|| Status::internal("Failed to load extensions before handling request"))?
            .role;

        Ok(role)
    }
}

/// X-postgres-* headers collected into a single extension
struct RoleExtension {
    role: Role,
}

/// Interceptor function for collecting user roles from request headers
pub fn interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    // derive the role from metadata
    let role = request
        .metadata()
        .get(ROLE_HEADER)
        .map(|header| header.to_str())
        .transpose()
        .map_err(|error| {
            let message = format!("Invalid {} header: {}", ROLE_HEADER, error);

            Status::invalid_argument(message)
        })?
        .map(String::from);

    // add the Postgres extension to the request
    request.extensions_mut().insert(RoleExtension { role });

    Ok(request)
}
