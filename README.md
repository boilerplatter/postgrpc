# PostgRPC

Query your Postgres databases directly using gRPC or transcoded JSON.

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
    2. [Load Balancing](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/load-balancing)
    3. [Auth](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/auth)
4. [FAQ](#faq)

## Introduction

### Why?

Sometimes you want to use the full power of a Postgres database, but you aren't able to make direct connections or you don't want to write a custom API service that wraps your database queries. PostgRPC gives you the power of SQL over [`gRPC`](https://grpc.io/) or JSON, handling distributed transactions and connection management on your behalf.

### Similar Projects

PostgRPC fills a similar niche as the excellent [PostgREST](https://postgrest.org/en/v8.0/) and [PostGraphile](https://www.graphile.org/postgraphile/) projects. Unlike those projects, PostgRPC lets you use SQL directly rather than wrapping the interface in another query language (i.e. a REST DSL or GraphQL, respectively). In addition, PostgRPC lets you work with lower-level database constructs like [transactions](https://www.postgresql.org/docs/current/tutorial-transactions.html) through the same `gRPC` or JSON interface used to query your database.

### Goals

- **Performance**: running a query over a persistent connection will always be the fastest option, but using PostgRPC should be the next-best option. Where concurrent queries are needed, and where those queries scale up faster than connections, PostgRPC should handle more concurrent query requests than any other direct-connection-based connection pool solution.
- **Primitive Focus**: where Postgres has a feature, PostgRPC should support that feature through the query interface. Where this is impossible, PostgRPC should strive to provide a distributed equivalent.
- **Ease-of-Use**: those looking to get started with PostgRPC should be able to spin it up as a service quickly on a variety of systems. PostgRPC should ship with sane defaults for most use-cases.
- **Type Inference**: PostgRPC should accomdate the flexibility of JSON in inputs and outputs rather than mapping JSON or `gRPC` types to Postgres types. This includes leveraging Postgres's built-in type inference wherever possible.
- **Customization**: PostgRPC should be a reference implementation of a `gRPC` service that can be easily re-implemented over custom connection pool logic. `postgres-services` should be usable by anyone looking to use a different protocol to query connection pools that implement the `Pool` and `Connection` traits from `postgres-pool`.

### Non-Goals

- **Auth**: PostgRPC does not include authentication or authorization mechanisms beyond those provided by the Postgres database itself. Setting the Postgres `ROLE` can be done through an `X-Postgres-Role` header, and correctly deriving the value of that header is the responsibility of other services better suited to the task (e.g. [Ory Oathkeeper](https://www.ory.sh/oathkeeper/docs/next/))
- **Strict Request Types**: PostgRPC will not support binary encoding of custom or extension-provided types. Instead, all types will be inferred or coerced through Postgres itself. Where type inference or coercion is unsufficient, queries should use text parameters and type hints in the query itself to convert values appropriately.
- **All-in-One**: PostgRPC does not replace your application stack. Instead, PostgRPC is a sharp tool that's easy to integrate into a toolbox that includes things like user management, load balancing, and routing of traffic from public endpoints. Do _not_ expose your database publicly through PostgRPC unless you know what you're doing (and even then, consider alternatives like those found in the `examples` directory).

## Getting Started

### Installation

Make sure that you have `cargo` and appropriate Rust toolchains installed. Then clone this repo and run `cargo build --release` in the `postgrpc` subdirectory.

The final executable to run will be at `./target/release/postgrpc`.

### Configuration

PostgRPC can be configured with the following environment variables:

- `HOST`: the host that the `postgrpc` service uses. Defaults to `127.0.0.1`.
- `PORT`: the port that the `postgrpc` service uses. Defaults to `50051`. 
- `TERMINATION_PERIOD`: the number of milliseconds `postgrpc` waits before shutting down on `SIGTERM` signals. `postgrpc` shuts down gracefully, waiting for requests to finish where possible. This value is useful for waiting for proxies like `envoy` to drain, allowing `postgrpc` to handle those requests without error as long as they take less than `TERMINATION_PERIOD` milliseconds.
- `STATEMENT_TIMEOUT`: the number of milliseconds `postgrpc` waits before aborting queries.
- `PGDBNAME` (required): the name of the Postgres database to connect to.
- `PGHOST`: the host of the Postgres cluster to connect to. Defaults to `localhost`.
- `PGPASSWORD` (required): the password of the user to use when connecting to the Postgres database.
- `PGPORT`: the port of the Postgres cluster to connect to. Defaults to `5432`.
- `PGUSER` (required): the user to use when connecting to the Postgres database
- `ALLOWED_STATEMENTS`: [CORS-like](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) set of allowed statements in queries. Possible values are `*` for all statements (the default), an empty string for no allowed statements (effectively disabling the query interface), or a comma-separated list of allowed statements. Possible statements are `SELECT`, `INSERT`, `UPDATE`, and `DELETE`.
- `ALLOWED_FUNCTIONS`: [CORS-like](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) set of function names that can be executed in queries. Possible values are `*` for all functions (the default), an empty string for no allowed functions, or a comma-separated list of allowed function names, e.g. `to_json,pg_sleep`, etc.

### Usage

With PostgRPC running on the default port and host, [`grpcurl`](https://github.com/fullstorydev/grpcurl) can be used to query the database:

```bash
grpcurl \
  -plaintext \
  -d '{"statement":"select 1 + 1 as two"}' \
  [::]:50051 postgres.Postgres/Query

# { "two": 2 }
```

To use a different (pre-existing) `ROLE` than the one used to connect to the database initially, use the `X-Postgres-Role` header:

```bash
grpcurl \
  -plaintext \
  -d '{"statement":"select current_user"}' \
  -H 'X-Postgres-Role: my-other-user' \
  [::]:50051 postgres.Postgres/Query

# { "current_user": "my-other-user" }
```

## Examples

All examples can be run from the `./examples` directory using `docker-compose`. Click on the links below to learn more about each example.

- [JSON Transcoding](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/json-transcoding)
- [Load Balancing](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/load-balancing)
- [Auth](https://github.com/boilerplatter/postgrpc/tree/master/postgrpc/examples/auth)

## FAQ

1. **Who built PostgRPC?** The team at [Platter](https://platter.dev). Every Platter Postgres branching database comes with a `gRPC` and `JSON` interface much like this one.
2. **Is PostgRPC ready for production?** If you're running this yourself, be sure to run it as a part of a stack that includes robust authentication and authorization, and ensure that you harden your Postgres database against malicious queries! If that sounds daunting, consider using [Platter](https://platter.dev). 
3. **How do you pronounce PostgRPC?** "post-ger-puck"
