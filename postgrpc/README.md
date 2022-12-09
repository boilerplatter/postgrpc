[`postrpc`](https://github.com/boilerplatter/postgrpc) is a [gRPC](https://grpc.io/) wrapper around Postgres databases that provides a dynamically-typed, JSON-compatible query interface.

While most users will use [`postgrpc`](https://github.com/boilerplatter/postgrpc) in its standalone, executable form, this package is also usable as a library that provides all of the building blocks needed to build your own gRPC server around Postgres (or anything else that could be represented by a `Pool` of `Connections`). Customization can be done through feature-based conditional compilation and by implementing [`pools::Pool`] and [`pools::Connection`] traits over customized connection management systems.

## How to use `postgrpc`

By default, this crate ships with all of the features required by its executable. If you're using `postgrpc` as a library, you will probably only want to enable a subset of those features. A few of those uses are discussed below.

#### gRPC handling as a `tonic::server::NamedService`

By default, `postgrpc` ships a connection-pool-agnostic, [`tonic`](https://docs.rs/tonic/)-compatible gRPC Postgres service in [`services::postgres`]. To use this service as a part of a [`tonic`](https://docs.rs/tonic) app without needing to implement your own pool, be sure to enable the `deadpool` feature in your `Cargo.toml`.

##### [`tonic`](https://docs.rs/tonic/latest/tonic/) Example

```rust
use postgrpc::{pools::deadpool, services::postgres};
use std::sync::Arc;
use tonic::transport::Server;
// deadpool::Configuration implements serde::Deserialize,
// so envy can be used to deserialize it from environment variables
use envy::from_env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // create the connection pool from environment variables
  let pool = from_env::<deadpool::Configuration>()?
    .create_pool()
    .map(Arc::new)?;

  // configure and run the tonic server on port 8675
  Server::builder()
    .add_service(services::postgres::new(pool))
    .serve(([127, 0, 0, 1], 8675).into())
    .await?;

  Ok(())
}
```

#### Custom connection pooling

See the documentation of [`pools::Pool`] and [`pools::Connection`] for how to implement your own connection pools for [`services`]. Custom pools can be implemented without any default features from this library.
