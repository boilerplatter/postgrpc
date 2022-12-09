# PostgRPC
[![Latest Version](https://img.shields.io/crates/v/postgrpc.svg)](https://crates.io/crates/postgrpc)
[![Documentation](https://docs.rs/postgrpc/badge.svg)](https://docs.rs/postgrpc)

Query your Postgres databases directly using gRPC, gRPC-web, or transcoded JSON.

## Table of Contents

1. [Introduction](#introduction)
    1. [Why](#why)
    2. [Similar Projects](#similar-projects)
    3. [Goals](#goals)
    4. [Non-Goals](#non-goals)
2. [Getting Started](#getting-started)
    1. [Installation](#installation)
    2. [Configuration](#configuration)
    3. [Usage](#usage)
3. [Examples](#examples)
    1. [JSON Transcoding](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/json-transcoding)
    2. [gRPC-web](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/grpc-web)
    3. [Load Balancing](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/load-balancing)
    4. [Auth](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/auth)
4. [FAQ](#faq)
5. [Roadmap](#roadmap)

## Introduction

### Why?

Sometimes you want to use the full power of a Postgres database, but you aren't able to make direct connections or you don't want to write a custom API service that wraps your database queries. PostgRPC gives you the power of SQL over [`gRPC`](https://grpc.io/) or JSON, handling distributed transactions and connection management on your behalf.

### Similar Projects

PostgRPC fills a similar niche as the excellent [PostgREST](https://postgrest.org/en/v8.0/) and [PostGraphile](https://www.graphile.org/postgraphile/) projects. Unlike those projects, PostgRPC lets you use SQL directly rather than wrapping the interface in another query language (i.e. a REST DSL or GraphQL, respectively). In addition, PostgRPC lets you work with lower-level database constructs like [transactions](https://www.postgresql.org/docs/current/tutorial-transactions.html) through the same `gRPC` or JSON interface used to query your database.

### Goals

- **Performance**: running a query over a persistent connection will always be the fastest option, but using PostgRPC should be the next-best option. Where concurrent queries are needed, and where those queries scale up faster than connections, PostgRPC should handle more concurrent query requests than any other direct-connection-based connection pool solution.
- **Primitive Focus**: where Postgres has a feature, PostgRPC should support that feature through the query interface. Where this is impossible, PostgRPC should strive to provide a distributed equivalent.
- **Ease-of-Use**: those looking to get started with PostgRPC should be able to spin it up as a service quickly on a variety of systems. PostgRPC should ship with sane defaults for most use-cases. For more limited cases, PostgRPC should be easy to configure through feature-gating and conditional compilation.
- **Type Inference by Default**: PostgRPC's dynamic `query` methods should accomdate the flexibility of JSON in inputs and outputs rather than mapping JSON or `gRPC` types to Postgres types. This includes leveraging Postgres's built-in type inference wherever possible.
- **Type Safety When Needed**: for those looking for end-to-end type safety and strict validation between `proto3` and Postgres types, `postgrpc-build` should give users the confidence to leverage protobuf definitions as reliable sources of type-safe truth.
- **Customization**: PostgRPC's provided binaries should be a reference implementation of a `gRPC` service. For those that need more flexibility, the `postgrpc` library is provided to handle custom connection pool logic, and `postgrpc-build` can be used to validate and generate entire type-safe implementations for use in a `tonic`-based application.

### Non-Goals

- **Auth**: PostgRPC does not include authentication or authorization mechanisms beyond those provided by the Postgres database itself. Setting the Postgres `ROLE` can be done through an `X-Postgres-Role` header (when the `role-header` feature is enabled), and correctly deriving the value of that header is the responsibility of other services better suited to the task (e.g. [Ory Oathkeeper](https://www.ory.sh/oathkeeper/docs/next/))
- **All-in-One**: PostgRPC does not replace your application stack. Instead, PostgRPC is a sharp tool that's easy to integrate into a toolbox that includes things like user management, load balancing, and routing of traffic from public endpoints. Do _not_ expose your database publicly through PostgRPC unless you know what you're doing (and are using the `compiled-queries` feature built into `postgrpc-build`).

## Getting Started

### Installation

For binary installations, use `cargo install postgrpc`. The compilation of the `postgrpc` executable can be customized with `--features`.

To use `postgrpc` as a library, run `cargo add postgrpc` within a `cargo`-managed Rust project.

To compile type-safe routes from `proto3` files for use in your own Rust projects, install the `postgrpc-build` library from the `compiled-queries` branch of the `postgrpc` GitHub repository with `cargo add postgrpc-build --git https://github.com/boilerplatter/postgrpc --branch compiled-queries`.

### Configuration

The `postgrpc` executable can be configured with the following environment variables:

- `HOST`: the host that the `postgrpc` service uses. Defaults to `127.0.0.1`.
- `PORT`: the port that the `postgrpc` service uses. Defaults to `50051`. 
- `TERMINATION_PERIOD`: the number of milliseconds `postgrpc` waits before shutting down on `SIGTERM` signals. `postgrpc` shuts down gracefully, waiting for requests to finish where possible. This value is useful for waiting for proxies like `envoy` to drain, allowing `postgrpc` to handle those requests without error as long as they take less than `TERMINATION_PERIOD` milliseconds.

In addition, the default connection pool can be configured with the following environment variables:

- `MAX_CONNECTION_POOL_SIZE`: the number of upstream database connections that the pool is allowed to hold onto. Defaults to 4x the number of CPUs available on the host machine.
- `STATEMENT_TIMEOUT`: the number of milliseconds `postgrpc` waits before aborting queries.
- `RECYCLING_METHOD`: the [recycling method](https://docs.rs/deadpool-postgres/latest/deadpool_postgres/enum.RecyclingMethod.html) run as connections are returned to the connection pool. Defaults to [`clean`](https://docs.rs/deadpool-postgres/latest/deadpool_postgres/enum.RecyclingMethod.html#variant.Clean).
- `PGDBNAME` (required): the name of the Postgres database to connect to.
- `PGHOST`: the host of the Postgres cluster to connect to. Defaults to `localhost`.
- `PGPASSWORD` (required): the password of the user to use when connecting to the Postgres database.
- `PGPORT`: the port of the Postgres cluster to connect to. Defaults to `5432`.
- `PGUSER` (required): the user to use when connecting to the Postgres database.
- `PGAPPNAME`: the application label to use when connecting to the Postgres database. Defaults to `postgrpc`.
- `PGSSLMODE`: the [`sslmode`](https://www.postgresql.org/docs/current/libpq-ssl.html) to use when connecting to the Postgres database. Supported values are `disable`, `prefer`, and `require`.

### Usage

With PostgRPC running on the default port and host, [`grpcurl`](https://github.com/fullstorydev/grpcurl) can be used to query the database using the dynamic `Query` endpoint:

```bash
grpcurl \
  -plaintext \
  -d '{"statement":"select 1 + 1 as two"}' \
  [::]:50051 postgres.v1.Postgres/Query

# { "two": 2 }
```

To use a different (pre-existing) `ROLE` than the one used to connect to the database initially, use the `X-Postgres-Role` header:

```bash
grpcurl \
  -plaintext \
  -d '{"statement":"select current_user"}' \
  -H 'X-Postgres-Role: my-other-user' \
  [::]:50051 postgres.v1.Postgres/Query

# { "current_user": "my-other-user" }
```

## Examples

All examples can be run from the `./examples` directory using `docker-compose`. Click on the links below to learn more about each example.

- [JSON Transcoding](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/json-transcoding)
- [gRPC-web](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/grpc-web)
- [Load Balancing](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/load-balancing)
- [Auth](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/auth)

## FAQ

1. **Who built PostgRPC?** The team at [Platter](https://platter.dev).
2. **Is PostgRPC ready for production?** PostgRPC should be considered alpha-level software, and no warranty is given or implied. If you still want to run PostgRPC yourself, be sure to run it as a part of a stack that includes robust authentication and authorization, and ensure that you harden your Postgres database against malicious queries! But you were doing that with your Postgres database anyway, right?
3. **Can I query PostgRPC directly from a web browser?** Through a Proxy like `envoy`, yes. But querying PostgRPC directly through the `Query` interfaces in your single-page-apps is not recommended, as malicious users can run all sorts of nasty queries on your database. Instead, use `postgrpc-build` to compile known, safe queries into services that you can use in your own gRPC applications.
4. **How do you pronounce PostgRPC?** "post-ger-puck"

## Contributing

Contributions are welcome in the form of bug reporting, feature requests, software fixes, and documentation updates. Please submit all code contributions as pull requests through GitHub.

## Roadmap

- [ ] Native JSON transcoding without needing an additional proxy
- [ ] [`LISTEN`/`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html)-based channels
- [ ] [`MATERIALIZED VIEW`](https://www.postgresql.org/docs/14/rules-materializedviews.html)-based update streams
- [ ] `protoc-gen-postgrpc` for generating PostgRPC services through `protoc` directly
- [ ] `postgrpc-build`-based auto-compilation during `postgrpc` binary installation
