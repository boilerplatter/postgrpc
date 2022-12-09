use crate::proto::Service as ProtoService;
use proc_macro2::TokenStream;
use quote::quote;
use tonic_build::{CodeGenBuilder, Method as _, Service};

/// Generate a postgRPC Service implementation for use with the `tonic` + `tower` ecosystem
pub(crate) fn generate<'a, S>(service: &S, proto_service: &ProtoService<'a>) -> TokenStream
where
    S: Service,
{
    // FIXME: make this tonic generation configurable in case users have their own tonic compilation steps
    // generate the tonic dependencies
    let tonic_output = CodeGenBuilder::new().generate_server(service, "super::super");

    // derive the same dependencies as Tonic
    let server_service = quote::format_ident!("{}Server", service.name());
    let server_trait = quote::format_ident!("{}", service.name());
    let server_mod = quote::format_ident!("{}_server", naive_snake_case(service.name()));
    let methods = generate_methods(service, proto_service);

    // FIXME: make sure that dependencies are properly resolved here!
    // we should namespace these in a "codegen" module in postgrpc
    quote! {
        use postgrpc::codegen::*;
        use proto::#server_mod::{#server_trait as GrpcService, #server_service};

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
            async fn query(
                &self,
                key: P::Key,
                statement: &str,
                parameters: &[&(dyn ToSql + Sync)],
            ) -> Result<<P::Connection as Connection<Row>>::RowStream, P::Error>
            {
                tracing::info!("Querying postgres");

                let rows = self
                    .pool
                    .get_connection(key)
                    .await?
                    .query(statement, parameters)
                    .await?;

                Ok(rows)
            }
        }

        /// concrete gRPC service implementation for the configured postgRPC service
        #[async_trait]
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
fn generate_methods<'a, S>(service: &S, proto_service: &ProtoService<'a>) -> TokenStream
where
    S: Service,
{
    let mut tokens = TokenStream::new();

    for method in service.methods() {
        let name = method.name();
        let identifier = method.identifier();

        // FIXME: handle the missing annotation case either gracefully or completely or both
        let proto_method = proto_service
            .get_method(identifier)
            .unwrap_or_else(|| panic!("Expected method {identifier}, but it doesn't exist"));

        let query = proto_method.query();

        // FIXME: double-check that this shouldn't be snake-cased... is this a regression upstream?
        let name = quote::format_ident!("{name}");

        // FIXME: either support all streaming combinations or complain loudly
        if !method.client_streaming() && method.server_streaming() {
            let stream = quote::format_ident!("{}Stream", method.identifier());

            // generate the request types
            let input_type = proto_method.input_type();
            let request = if input_type.name() == ".google.protobuf.Empty" {
                quote! { () }
            } else {
                let request = quote::format_ident!("{}", input_type.proto_type());

                quote! { #request }
            };
            let request_fields = input_type
                .fields()
                .map(|field| quote::format_ident!("{}", field.name()));

            // generate the response types
            let output_type = proto_method.output_type();
            let (response, row_conversion) = if output_type.name() == ".google.protobuf.Empty" {
                (TokenStream::new(), quote! { () })
            } else {
                let response = quote::format_ident!("{}", output_type.proto_type());
                let fields = output_type.fields().map(|field| {
                    let key = field.name();
                    let field = quote::format_ident!("{}", key);

                    // FIXME: make this fallible! panic-ing at runtime is probably incorrect,
                    // despite previous efforts to check and validate the types
                    quote! { #field: row.get(#key) }
                });

                (
                    quote! { #response },
                    quote! {
                        #response {
                            #(#fields),*
                        }
                    },
                )
            };

            let method = quote! {
                type #stream = UnboundedReceiverStream<Result<#response, Status>>;

                #[tracing::instrument(skip(self, request), err)]
                async fn #name(&self, mut request: Request<#request>) -> Result<Response<Self::#stream>, Status> {
                    // derive a key from extensions to use as a connection pool key
                    let key = P::Key::from_request(&mut request).map_err(Into::<Status>::into)?;
                    let request = request.into_inner();

                    // get the rows, converting output to #response messages
                    let rows = self
                        .query(key, #query, &[#(&request.#request_fields),*])
                        .await
                        .map_err(Into::<Status>::into)?
                        .map_ok(|row| #row_conversion)
                        .map_err(Into::<Status>::into);

                    // create the row stream transmitter and receiver
                    let (transmitter, receiver) = unbounded_channel();

                    // emit the rows as a Send stream
                    spawn(async move {
                        pin_mut!(rows);

                        while let Some(row) = rows.next().await {
                            transmitter.send(row)?;
                        }

                        Ok::<_, SendError<_>>(())
                    });

                    Ok(Response::new(UnboundedReceiverStream::new(receiver)))
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
