## Load-balancing example

PostgRPC's quick startup times and low resource overhead mean that it works well in containerized deployments. But it's important to remember that PostgRPC is _not_ stateless: connections are pooled and managed on behalf of users with the expectation that those users will tend to make all future requests against that same pool of connections and transactions.

For standard queries, this pooling behavior leads to better performance. For transactions, requests to active transactions _must_ be made against the PostgRPC instance where that transaction was originally created.

This means that there are advantages to running a single instance of PostgRPC, and this will suffice for many use-cases. For those cases where multiple replicas of PostgRPC are needed (e.g. using [rolling updates](https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/) in Kubernetes Deployments), it's important to implement load-balancing in a way that distributes initial requests among replicas, but maintains "sticky" sessions for each client when subsequent requests are made.

### Pre-requisites

- [`docker-compose`](https://docs.docker.com/compose/)
- a clone of this repo (i.e. `git clone git@github.com:boilerplatter/postgrpc.git`)
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) (optional) for making gRPC requests
- [`jq`](https://stedolan.github.io/jq/) (optional) for pretty-printing JSON responses

### Getting Started

This example uses [`docker-compose`](https://docs.docker.com/compose/) to spin up a `traefik` proxy in front of multiple replicas of a `postgrpc` service. From this directory, `docker-compose up` starts the example application.

Once all services have been built, gRPC requests can be made against the `query` endpoint exposed on `traefik`'s public port `50051`, e.g.:

```bash
grpcurl \
  -plaintext \
  [::]:50051 transaction.Transaction/Begin | jq

# {"id": "0fa9735c-4706-4e89-977a-e967be9d3dcd"}
```

As these requests are made, you'll notice that requests are handled by different replicas each time. This is what we'd expect when we hit the `postgrpc` entrypoint, which is managed by Traefik's [Docker provider](https://doc.traefik.io/traefik/routing/providers/docker/). Unfortunately, this behavior means that subsequent queries against active transactions will succeed on every third attempt thanks to Traefik's round-robin load-balancing.

But this example `traefik` service's load balancer has the [sticky sessions](https://doc.traefik.io/traefik/routing/services/#sticky-sessions) feature enabled. This feature sends back a `Set-Cookie` header on the initial request, letting clients make requests to the same replica over and over again.

Repeating the previous request with `grpcurl`'s "verbose" flag `-vv` shows us that the `Set-Cookie` header is being set in the response:

```bash
grpcurl \
  -vv \
  -plaintext \
  [::]:50051 transaction.Transaction/Begin

# Response headers received:
# content-type: application/grpc
# set-cookie: postgrpc-session=f7c4de8a828245d3; Path=/

# Response contents:
# {"id": "0fa9735c-4706-4e89-977a-e967be9d3dcd"}
```

As long as the transaction hasn't been cleaned up or the replica killed, queries can be run against the transaction properly by including the contents of `Set-Cookie` in a `Cookie` header on subsequent requests:

```bash
grpcurl \
  -plaintext \
  -H 'Cookie:postgrpc-session=f7c4de8a828245d3; Path=/'
  -d '{"id":"0fa9735c-4706-4e89-977a-e967be9d3dcd","statement":"select current_user"}'
  [::]:50051 transaction.Transaction/Query | jq

# {"current_user": "postgres"}

grpcurl \
  -plaintext \
  -H 'Cookie:postgrpc-session=f7c4de8a828245d3; Path=/'
  -d '{"id":"0fa9735c-4706-4e89-977a-e967be9d3dcd"}'
  [::]:50051 transaction.Transaction/Commit
```

### Error handling

Session cookies that route to killed instances will be gracefully sent to another instance in this implementation. This will still generate an error for clients, though, so it's best to include retries when working with transactions in environments where PostgRPC replicas are pre-emptible (like Kubernetes). All transactions are safely rolled back when their parent process is killed, so it's safe to retry any transaction that hasn't been succesfully committed.

### Alternative implementations

Cookie-based sticky sessions are supported in many proxies, including `envoy` and `nginx`.

But cookie-based sticky sessions are one of many implementations. Proxies like `envoy` also allow for custom [hash policies](https://www.envoyproxy.io/docs/envoy/v1.20.0/api-v3/config/route/v3/route_components.proto.html?highlight=hash_policy) used in hash-based load balancing. For load balancers like this, hashing by source IP address might be a good alternative to cookies.
