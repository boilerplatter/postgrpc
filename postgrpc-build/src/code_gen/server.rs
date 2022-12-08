use crate::proto::Service as ProtoService;
use proc_macro2::TokenStream;
use quote::quote;
use tonic_build::{CodeGenBuilder, Method as _, Service};

/// Generate a postgRPC Service implementation for use with the `tonic` + `tower` ecosystem
// FIXME: encapsulate the concept of a postgRPC-specific service that can be derived from a Service
pub(crate) fn generate<'a, S>(
    service: &S, // FIXME: derive ProtoService from a service (or at least impl Service)
    proto_service: &ProtoService<'a>,
    proto_path: &str,
) -> TokenStream
where
    S: Service,
{
    // FIXME: make this tonic generation configurable in case users have their own tonic compilation steps
    // generate the tonic dependencies
    let tonic_output = CodeGenBuilder::new().generate_server(service, proto_path);

    // derive the same dependencies as Tonic
    let server_service = quote::format_ident!("{}Server", service.name());
    let server_trait = quote::format_ident!("{}", service.name());
    let server_mod = quote::format_ident!("{}_server", naive_snake_case(service.name()));
    let methods = generate_methods(service, proto_service, proto_path);

    // FIXME: make sure that dependencies are properly resolved here!
    // we should namespace these in a "codegen" module in postgrpc_lib
    quote! {
        use postgrpc_lib::{
            extensions::FromRequest,
            pools::{Connection, Pool},
            codegen::*
        };
        use proto::#server_mod::{#server_trait as GrpcService, #server_service};

        // FIXME: export all of these from postgrpc_lib::codegen
        use futures_util::{pin_mut, StreamExt, TryStreamExt};
        use std::sync::Arc;
        use tokio::sync::mpsc::error::SendError;
        use tokio_stream::wrappers::UnboundedReceiverStream;
        use tonic::{codegen::InterceptedService, service::Interceptor, Request, Response, Status};
        use postgres_types::{BorrowToSql, Row};

        #[allow(unreachable_pub, missing_docs)]
        mod proto {
            #tonic_output
        }

        #[derive(Clone)]
        pub struct #server_trait<P> {
            pool: Arc<P>,
        }

        impl<P> #server_trait<P>
        where
            P: Pool<Row>,
        {
            /// Create a new postgRPC service from a reference-counted Pool
            fn new(pool: Arc<P>) -> Self {
                Self { pool }
            }

            /// Query the Postgres database, returning a stream of rows
            #[tracing::instrument(skip(self, parameters), err)]
            async fn query<B, I>(
                &self,
                key: P::Key,
                statement: &str,
                parameters: I,
            ) -> Result<<P::Connection as Connection<Row>>::RowStream, P::Error>
            where
                B: BorrowToSql,
                I: IntoIterator<Item = B> + Send + Sync + Clone,
                I::IntoIter: ExactSizeIterator + Send
            {
                tracing::info!("Querying postgres");

                let rows = self
                    .pool
                    .get_connection(key)
                    .await?
                    .query(statement, parameters) // FIXME: make compatible with Pool type
                    .await?;

                Ok(rows)
            }
        }

        /// concrete gRPC service implementation for the configured postgRPC service
        #[tonic::async_trait]
        impl<P> GrpcService for #server_trait<P>
        where
            P: Pool<Row> + 'static,
            P::Key: FromRequest,
        {
            #methods
        }

        /// Create a new service from a connection pool
        pub fn new<P>(pool: Arc<P>) -> #server_service<#server_trait<P>>
        where
            P: Pool<Row> + 'static,
            P::Key: FromRequest,
        {
            #server_service::new(#server_trait::new(pool))
        }

        /// Create a new service from a connection pool and an interceptor
        pub fn with_interceptor<P, I>(
            pool: Arc<P>,
            interceptor: I,
        ) -> InterceptedService<#server_service<#server_trait<P>>, I>
        where
            P: Pool<Row> + 'static,
            P::Key: FromRequest,
            I: Interceptor,
        {
            #server_service::with_interceptor(#server_trait::new(pool), interceptor)
        }
    }
}

/// generate tonic-compatible service methods
fn generate_methods<'a, S>(
    service: &S,
    proto_service: &ProtoService<'a>,
    proto_path: &str,
) -> TokenStream
where
    S: Service,
{
    let mut tokens = TokenStream::new();

    for method in service.methods() {
        let name = method.name();

        // FIXME: handle the missing annotation case either gracefully or completely or both
        let proto_method = proto_service
            .get_method(name)
            .unwrap_or_else(|| panic!("Expected method {name}, but it doesn't exist"));

        let query = proto_method.query();

        // FIXME: double-check that this shouldn't be snake-cased... is this a regression upstream?
        let name = quote::format_ident!("{name}");

        // FIXME: use compile_well_known_types from caller, if relevant
        let (request, response) = method.request_response_name(proto_path, false);

        // FIXME: either support all streaming combinations or complain loudly
        if !method.client_streaming() && method.server_streaming() {
            let stream = quote::format_ident!("{}Stream", method.identifier());
            let parameters = proto_method.input_type().fields().map(|field| field.name());
            let output_type = proto_method.output_type();

            // convert columns into valid output type fields
            let row_conversion = if output_type.name() == ".google.protobuf.Empty" {
                quote! { () }
            } else {
                let fields = proto_method.output_type().fields().map(|field| {
                    let name = field.name();

                    // FIXME: make this fallible! panic-ing at runtime is probably incorrect
                    quote! { #name: row.get("#name") }
                });

                quote! {
                    #response {
                        #(#fields),*
                    }
                }
            };

            let method = quote! {
                type #stream = UnboundedReceiverStream<Result<#response, tonic::Status>>;

                #[tracing::instrument(skip(self, request), err)]
                async fn #name(&self, mut request: tonic::Request<#request>) -> Result<tonic::Response<Self::#stream>, tonic::Status> {
                    // derive a key from extensions to use as a connection pool key
                    let key = P::Key::from_request(&mut request).map_err(Into::<tonic::Status>::into)?;

                    // get the rows, converting output to #response messages
                    let rows = self
                        .query(key, #query, &[#(#parameters),*])
                        .await
                        .map_err(Into::<tonic::Status>::into)?
                        .map_ok(|row| #row_conversion)
                        .map_err(Into::<tonic::Status>::into);

                    // create the row stream transmitter and receiver
                    let (transmitter, receiver) = tokio::sync::mpsc::unbounded_channel();

                    // emit the rows as a Send stream
                    tokio::spawn(async move {
                        pin_mut!(rows);

                        while let Some(row) = rows.next().await {
                            transmitter.send(row)?;
                        }

                        Ok::<_, SendError<_>>(())
                    });

                    Ok(tonic::Response::new(UnboundedReceiverStream::new(receiver)))
                }
            };

            tokens.extend(method);
        }
    }

    tokens
}

/// tonic-build-compatible snake case helper function
fn naive_snake_case(name: &str) -> String {
    let mut s = String::new();
    let mut it = name.chars().peekable();

    while let Some(x) = it.next() {
        s.push(x.to_ascii_lowercase());
        if let Some(y) = it.peek() {
            if y.is_uppercase() {
                s.push('_');
            }
        }
    }

    s
}
