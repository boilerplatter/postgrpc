use postgres_role_json_pool::Role;
use tonic::{Request, Status};

#[cfg(feature = "role-header")]
const ROLE_HEADER: &'static str = "x-postgres-role";

/// Interceptor function for collecting the Role extension from X-postgres-role headers
pub fn role(mut request: Request<()>) -> Result<Request<()>, Status> {
    // derive the role from metadata
    #[cfg(feature = "role-header")]
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

    #[cfg(not(feature = "role-header"))]
    let role = Role::default();

    // add the Postgres extension to the request
    request.extensions_mut().insert(Role::new(role));

    Ok(request)
}
