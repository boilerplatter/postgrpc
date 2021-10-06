use tonic::{Request, Status};

#[cfg(feature = "role-header")]
const ROLE_HEADER: &'static str = "x-postgres-role";

/// X-postgres-* headers collected into a single extension
pub struct Postgres {
    pub role: Option<String>,
}

impl Postgres {
    /// Interceptor function for collecting the Postgres extension
    pub fn interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
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
        let role = None;

        // add the Postgres extension to the request
        request.extensions_mut().insert(Postgres { role });

        Ok(request)
    }
}
