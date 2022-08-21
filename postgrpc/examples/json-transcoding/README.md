## JSON transcoding example

PostgRPC is built around easy-to-transcode `proto3` [`Value`](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Value) types and includes [`Http` annotations](https://cloud.google.com/endpoints/docs/grpc-service-config/reference/rpc/google.api#google.api.Http) on all methods. This makes PostgRPC easy-to-use as both a gRPC server and a RESTful JSON API as long as it's deployed behind a proxy that supports JSON transcoding. Popular proxies with built-in transcoding include [`envoy`](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_json_transcoder_filter) and [`grpc-gateway`](https://grpc-ecosystem.github.io/grpc-gateway/).

### Pre-requisites

- [`docker-compose`](https://docs.docker.com/compose/)
- a clone of this repo (i.e. `git clone git@github.com:boilerplatter/postgrpc.git`)
- [`curl`](https://curl.se/) for making JSON requests
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) (optional) for making gRPC requests
- [`jq`](https://stedolan.github.io/jq/) (optional) for pretty-printing JSON responses
- [`protoc`](https://grpc.io/docs/protoc-installation/) (optional) for compiling file descriptors

### Getting Started

This example uses [`docker-compose`](https://docs.docker.com/compose/) to spin up an `envoy` proxy in front of `postgrpc` and `postgres` services. From this directory, `docker-compose up` starts the example application.

Once all services have been built, JSON queries can be made against the `query` endpoint exposed on `envoy`'s public port `50051`, e.g.:

```bash
curl -s \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"statement":"select current_user"}' \
  http://localhost:50051/query | jq

# [{"current_user": "postgres"}]
```

For comparison, gRPC requests are also handled using the same port. The same request with `grpcurl` would be:

```bash
grpcurl \
  -plaintext \
  -d '{"statement":"select current_user"}' \
  [::]:50051 postgres.Postgres/Query | jq

# {"current_user": "postgres"}
```

### Building Descriptor Sets

Most JSON-transcoding proxies require compilation of a descriptor set. In this example, that descriptor set is included as `postgrpc.pb`. This includes all of the services in this repo as well as automatic gRPC to JSON status code conversions with `error_details.proto`.

When implementing JSON-transcoding in your own applications, it's perfectly fine to use `postgrpc.pb` from this example. But if you'd like to compile that descriptor set yourself, you can do so with `protoc`.

To build the descriptor set with `protoc`, use the following command:

```bash
protoc \
  -I /absolute/path/to/this/repo/postgrpc/proto \
  -I. \
  --include_imports \
  --include_source_info \
  --descriptor_set_out=postgrpc.pb \
  google/rpc/error_details.proto transaction.proto postgres.proto
```

To limit the services exposed by the proxy as JSON endpoints, omit those service's `.proto` files from compilation or from the `services` field in the envoy transcoding filter's configuration in `envoy.yaml`.

### Notes on `envoy` and large query responses

While PostgRPC takes care to stream rows to the user immediately as they are returned from the query, `envoy`'s current transcoding implementation does not. Instead, `envoy` will instead used a [`chunked`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding#directives) `Transfer-Encoding` to return rows as a single JSON Array. This means that consumers need to take care to wait for the entire payload to be delivered instead of attempting to respond to individual rows as they arrive.

A better approach would be to transcode streams to [newline-delimited JSON](https://github.com/grpc-ecosystem/grpc-httpjson-transcoding/issues/38). If you are concerned about processing large query responses in-memory (as the current `envoy` solution requires), then consider using [`grpc-gateway`](https://grpc-ecosystem.github.io/grpc-gateway/) instead.

We'd also welcome any contributions towards proper newline-delimited JSON transcoding support in PostgRPC directly.

### Security Disclaimer

DO NOT expose a JSON-transcoded PostgRPC service to the public web without additional security measures! For examples of how to lock down a database for "public" consumption, take a look at the [`auth` example](../auth).
