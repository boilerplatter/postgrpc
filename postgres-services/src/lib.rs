//! Distributed query handlers for Postgres built on `postgres-pool`-style connection pools.
#![deny(missing_docs, unreachable_pub)]

/// Postgres database service handlers
pub mod postgres;
#[cfg(feature = "transaction")]
/// Postgres transaction handlers
pub mod transaction;

#[cfg(feature = "http-service")]
/// HTTP service that handles routing to underlying service handlers
pub mod http {
    use http::{Extensions, Request, Response, StatusCode};
    use hyper::Body;
    use postgres_pool::{Connection, Pool};
    use serde::Deserialize;
    use std::sync::Arc;
    use tower::{service_fn, steer::Steer, util::BoxService, Service};

    /// Type-erased service for routing
    type ErasedService = BoxService<Request<Body>, Response<Body>, http::Error>;

    /// Create an HTTP service that routes between the HTTP services in this crate
    pub fn service<P>(pool: Arc<P>) -> impl Service<Request<Body>, Error = http::Error>
    where
        P: Clone,
        P: Pool + 'static,
        P::Error: From<<P::Connection as Connection>::Error> + Into<StatusCode> + Send,
        P::Key: From<Extensions> + Send,
        <P::Connection as Connection>::Parameter: for<'de> Deserialize<'de>,
        <P::Connection as Connection>::RowStream: Into<Body>,
    {
        // create type-erased versions of each route-handling service
        let not_found = BoxService::new(service_fn(|_: Request<Body>| async move {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
        }));
        let postgres = BoxService::new(super::postgres::Postgres::new(pool));
        let services = [not_found, postgres];

        // line up the pickers in the SAME ORDER as the services starting at index 1
        let pickers = [super::postgres::http_service::pick];

        Steer::new(
            services,
            move |request: &Request<Body>, services: &[ErasedService]| {
                for (index, _service) in services.iter().enumerate().skip(1) {
                    if pickers[index](request) {
                        return index;
                    }
                }

                0 // Not Found
            },
        )
    }
}

// FIXME
// move specific gRPC service implementations to this crate
// expose both gRPC and JSON services based on features
