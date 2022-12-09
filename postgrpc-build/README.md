[`postgrpc-build`](https://github.com/boilerplatter/postrpc) compiles `proto3` files via [`prost`] and generates service stubs and database-validated proto definitions for use with [`postgrpc`](https://docs.rs/postgrpc/latest/postgrpc/).

## How to use `postgrpc-build`

By default, this crate ships with a Postgres database query validator and gRPC service compiler. The goal is to generate service stubs that can be used in a [`tonic`](https://github.com/hyperium/tonic) application and that are validated against the schema of your database.

Currently, this crate is unpublished until [Extension support is officially added to `prost`](https://github.com/tokio-rs/prost/issues/674). In the meantime, you can install `postgrpc` and `postgrpc-build` from `https://github.com/boilerplatter/postgrpc` using the `compiled-queries` branch.

`postgrpc-build` also requires an installation of a Protocol Buffer Compiler (`protoc`). See installations for `protoc` [here](https://grpc.io/docs/protoc-installation/).

## Example

`postgrpc-build` works with `proto3` `Service`s that have been annotated with custom `(postgrpc.query)` options on each `rpc`. [Similar to the annotations used for JSON transcoding](https://cloud.google.com/endpoints/docs/grpc/transcoding), an example `postgrpc`-compatible proto would look something like this:

```proto3
// taken from the "bookstore" example in this repo
syntax = "proto3";
package authors.v1;

// Empty is a well-known-type supported by postgrpc-build
import "google/protobuf/empty.proto";

// this line imports the required method exensions
import "postgrpc/api/annotations.proto";

// all postgrpc Services must annotate every rpc,
// and every rpc should be server-streaming
service Authors {
  rpc GetAuthors (google.protobuf.Empty) returns (stream Author) {
    option (postgrpc.query) = {
      sql: "select id, first_name, last_name from authors"
    };
  }

  rpc CreateAuthor (CreateAuthorRequest) returns (stream Author) {
    option (postgrpc.query) = {
      sql: "insert into authors (first_name, last_name) values ($1, $2) returning first_name, last_name"
    };
  }
}

message CreateAuthorRequest {
  string first_name = 1;
  string last_name = 2;
}

message Author {
  int32 id = 1;
  string first_name = 2;
  string last_name = 3;
}
```

This `authors.proto` file can then be compiled into a [`postgrpc`](https://docs.rs/postgrpc/latest/postgrpc/)-compatible implementation with a `build.rs` that looks like this:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    postgrpc_build::compile_protos("./authors.proto")?;

    Ok(())
}
```

Finally, the compiled `authors.v1.Authors` service can be used in a binary's `main.rs` like so:

```rust
use postgrpc::{
    pools::deadpool,
    codegen::tonic::transport::Server, // requires the 'codegen' feature
};
use std::sync::Arc;

#[allow(unused)]
mod proto {
    include!(concat!(env!("OUT_DIR"), concat!("/", "authors.v1", ".rs")));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // create a deadpool-based connection pool
    let pool = deadpool::Configuration {
        pgpassword: "supersecretpassword".to_owned(),
        ..Default::default()
    }
    .create_pool()
    .map(Arc::new)?;

    // generate and run a tonic app that uses the Authors service
    Server::builder()
        .add_service(proto::new(pool))
        .serve(([0, 0, 0, 0], 8675).into())
        .await?;

    Ok(())
}
```

## Validation

The above example is compiled without any verification of the input or output types. To verify those types at build time, use the `verification` feature flag in `postgrpc-build` and provide the [`Builder`] with a connection string to your database, like so:

```rust
// replace this connection string with your own, of course
static CONNECTION_STRING: &str = "postgresql://postgres:supersecretpassword@localhost:5432";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    postgrpc_build::configure()
        .validate_with(CONNECTION_STRING.to_owned())
        .compile(&["./authors.proto"], &["."])?;

    Ok(())
}
```

This version of `build.rs` will `PREPARE` your annotated queries against the database, verifying that input parameters and output columns match up with the input and output messages of the `rpc`.

## Sourcing protoc

`postgrpc-build` depends on the Protocol Buffers compiler, `protoc`, to parse .proto files into a representation that can be transformed into Rust. If set, `postgrpc-build` uses the `PROTOC` environment variable for locating `protoc`. For example, on a macOS system where Protobuf is installed with Homebrew, set the environment variables to:

```
PROTOC=/usr/local/bin/protoc
```

and in a typical Linux installation:

```
PROTOC=/usr/bin/protoc
```

If no `PROTOC` environment variable is set, then `postgrpc-build` will search the current `PATH` for `protoc` or `protoc.exe`. If `postgrpc-build` can not find `protoc` via these methods, then compilation will fail.
