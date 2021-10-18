## Auth example

PostgRPC does not come with any authentication or authorization mechanisms of its own. Postgres itself has a deep permissions system based on [`ROLE`](https://www.postgresql.org/docs/current/sql-createrole.html), [`GRANT`](https://www.postgresql.org/docs/current/sql-grant.html), and [`REVOKE`](https://www.postgresql.org/docs/current/sql-revoke.html). PostgRPC lets users interact with that system through the `X-Postgres-Role` header, but does not verify any caller's ability to set that header.

Instead, PostgRPC should use unprivileged ROLEs when connecting to databases, and should limit request privileges to authorized users. In some applications, keeping PostgRPC disconnected from public networks will be sufficient. But in cases where it makes sense to let users query PostgRPC from otherwise untrusted sources, PostgRPC endpoints should be protected by proxies and services that can convert user credentials into that `X-Postgres-Role` header, and the underlying data in the database should be protected with careful permission `GRANT`s and [row-level security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

### Pre-requisites

- [`docker-compose`](https://docs.docker.com/compose/)
- a clone of this repo (i.e. `git clone git@github.com:boilerplatter/postgrpc.git`)
- [`curl`](https://curl.se/) for making JSON requests
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) (optional) for making gRPC requests
- [`jq`](https://stedolan.github.io/jq/) (optional) for pretty-printing JSON responses
- [`protoc`](https://grpc.io/docs/protoc-installation/) (optional) for compiling file descriptors

### Getting Started

This example uses [`docker-compose`](https://docs.docker.com/compose/) to spin up the following services:

- `app`: a single-page app based on [`create-react-app`](https://create-react-app.dev/)
- `envoy`: a JSON-transcoding proxy for `postgrpc` and `postgres` based on the `json-transcoding` example in this repo
- `kratos`: an Identity and User Management API from [`ory`](https://www.ory.sh/kratos/docs/)
- `oathkeeper`: an API Gateway from [`ory`](https://www.ory.sh/oathkeeper/docs/) that enforces Authorization and Authentication policies for upstream requests
- `postgrpc-admin`: a private `postgrpc` instance that `kratos` uses to perform privileged actions like `CREATE USER`

The web application is a very simple note-taking app with users managed by `kratos`. All user data is requested through `postgrpc` as JSON requests proxied through `envoy` and protected by `oathkeeper`.

From this directory, `docker-compose up` starts the example application. Once all services are up-and-running, visiting `http://127.0.0.1:4455` in the browser will direct you to the `app`. After registering as a new user (handled through the `kratos` [self-service API](https://www.ory.sh/kratos/docs/reference/api#tag/v0alpha1)), users are able to create, edit, and delete their own notes from the protected dashboard.

### Auth Architecture

`kratos` and `oathkeeper` work together to authenticate users. Upstream requests to `postgrpc` leverage `X-Postgres-Role` headers to authorize users to work with data through Postgres's built-in `ROLE` system and row-level security policies.

To see how this is done, let's step through the entire user lifecycle:

0. before the application starts, the Postgres database is configured with [an unprivileged `appuser` and row-level security on a `notes` table](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/init.sh)
1. new users register through the web application at `/auth/registration`. Registration form submissions are sent to the `kratos` API.
2. `kratos` validates the form submission, generates and stores an identity for this user, and executes [post-registration hooks](https://www.ory.sh/kratos/docs/self-service/hooks/#registration).
3. the [configured `kratos` hooks](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/kratos/kratos.yml#L61) make JSON-based requests to `postgrpc-admin` that [creates a new Postgres user from the new identity](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/kratos/create_user.jsonnet), [grants the user `appuser` the ability to set the active role to the new role](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/kratos/grant_role.jsonnet), [grants usage of the `public` schema to the new role](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/kratos/grant_usage.jsonnet), and [grants the new role CRUD permissions on the `notes` table](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/kratos/grant_ops.jsonnet). If any of these hooks fail, user registration fails.
4. Successful registration requests are returned from `kratos` with `Set-Cookie` headers that contain session information for the newly-registered user. These credentials will then be sent along with every request to `postgrpc`.
5. When a user is logged in (i.e. has a valid session cookie), they are redirected to the `app` dashboard. From this dashboard, they can create, read, update, and delete short notes. These actions are performed through JSON-based requests to the `oathkeeper` proxy, which guards requests to the otherwise-hidden `potgrpc` service.
6. When a user makes a request for note data (e.g. `select * from notes`), that request is sent to `oathkeeper`, which [retrieves session information from `kratos`](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/oathkeeper/oathkeeper.yml#L51) by forwarding the credentials included in the original request. If the included credentials are missing or invalid, the request is rejected.
7. If the credentials are valid, then `oathkeeper` then [mutates](https://www.ory.sh/oathkeeper/docs/pipeline/mutator#header) the request by [mapping the subject of the `kratos` session to the `X-Postgres-Role` header](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/oathkeeper/oathkeeper.yml#L73) and forwarding the request to the `envoy` proxy.
8. `envoy` transcodes the JSON request, retaining the `X-Postgres-Role` header, and forwards that request to `postgrpc`
9. `postgrpc` sets the Postgres `ROLE` to the value of `X-Postgres-Role`, thereby enforcing the permissions of the underlying `ROLE` when making the query. Thanks to the hooks in step 3 and the Postgres databases [row-level security policies on the `notes` table](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/init.sh#L34), users are able to work with their specific notes without being able to interact with any other users'.

Thanks to this interplay between Postgres, PostgRPC, Kratos, Oathkeeper, and browser-based Cookies, we can more confidently expose the PostgRPC query interface to untrusted environments like the public web.

### Additional Security

As with any public API, authentication and authorization are only a part of the security picture. This example also includes the following countermeasures against malicious users:

1. properly auth'd users can still spam an endpoint with requests, so this example uses [`envoy`'s `ratelimit` rate-limiting service](https://github.com/envoyproxy/ratelimit) to limit the total number requests by `X-Postgres-Role`.
2. malicious users can invoke queries that are computationally-expensive. This example sets a low `STATEMENT_TIMEOUT` to keep large queries from tying up too many connections and restricts the use of potentially expensive functions by preventing all function execution through the `ALLOWED_FUNCTIONS` configuration.
3. malicious users can tie up database connections by starting large numbers of transactions at once, so this example [disables the `transaction` service entirely](https://github.com/boilerplatter/postgrpc/blob/master/postgrpc/examples/auth/envoy.yaml#L34) at the proxy level.
4. because of the way connections are re-used in the pool, clever users could `SET` session-level values that affect subsequent queries. This example prevents these kinds of malicious queries by restricting statements to `SELECT`,`INSERT`,`UPDATE`, and `DELETE` in queries through the `ALLOWED_STATEMENTS` configuration.
