use super::codegen::Builder;
use proc_macro2::TokenStream;
use tonic_build::CodeGenBuilder;

/// Custom Prost-compatible service generator for implemtning gRPC service implemntations for
/// modules with postgrpc-annotated methods
pub(crate) struct Generator {
    build_client: bool,
    proto_path: String,
    clients: TokenStream,
    servers: TokenStream,
}

impl Generator {
    pub(crate) fn new(builder: &Builder) -> Self {
        Self {
            build_client: builder.build_client,
            proto_path: builder.proto_path.to_owned(),
            clients: TokenStream::default(),
            servers: TokenStream::default(),
        }
    }
}

impl prost_build::ServiceGenerator for Generator {
    // FIXME: generate postgrpc service implementations instead of tonic!
    fn generate(&mut self, service: prost_build::Service, _buffer: &mut String) {
        let server = CodeGenBuilder::new().generate_server(&service, &self.proto_path);

        self.servers.extend(server);

        if self.build_client {
            let client = CodeGenBuilder::new().generate_client(&service, &self.proto_path);

            self.clients.extend(client);
        }
    }

    fn finalize(&mut self, buffer: &mut String) {
        if self.build_client && !self.clients.is_empty() {
            let clients = &self.clients;

            let client_service = quote::quote! {
                #clients
            };

            let ast: syn::File = syn::parse2(client_service).expect("not a valid tokenstream");
            let code = prettyplease::unparse(&ast);
            buffer.push_str(&code);

            self.clients = TokenStream::default();
        }

        if !self.servers.is_empty() {
            let servers = &self.servers;

            let server_service = quote::quote! {
                #servers
            };

            let ast: syn::File = syn::parse2(server_service).expect("not a valid tokenstream");
            let code = prettyplease::unparse(&ast);
            buffer.push_str(&code);

            self.servers = TokenStream::default();
        }
    }

    fn finalize_package(&mut self, _package: &str, buf: &mut String) {
        // FIXME: consider package-specific finalizers where there is more than one package
        self.finalize(buf);
    }
}
