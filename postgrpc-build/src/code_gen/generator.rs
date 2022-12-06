use super::Builder;
use proc_macro2::TokenStream;
use tonic_build::CodeGenBuilder;

/// Custom Prost-compatible service generator for implemtning gRPC service implemntations for
/// modules with postgrpc-annotated methods
#[derive(Debug)]
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

#[cfg(test)]
mod test {
    use super::Generator;
    use crate::code_gen::Builder;
    use prost::ExtensionSet;
    use prost_build::{Comments, Method, Service, ServiceGenerator};
    use prost_types::{MethodOptions, ServiceOptions};

    // FIXME: add useful configuration options
    fn generate_test_service() -> Service {
        let comments = Comments {
            leading_detached: Vec::new(),
            leading: Vec::new(),
            trailing: Vec::new(),
        };

        let method_options = MethodOptions {
            deprecated: None,
            uninterpreted_option: Vec::new(),
            idempotency_level: None,
            // FIXME: use postgrpc extension set
            extension_set: ExtensionSet::default(),
        };

        let methods = vec![Method {
            name: "TestMethod".to_owned(),
            proto_name: "TestMethod".to_owned(),
            client_streaming: false,
            server_streaming: false, // FIXME: test streaming, too
            comments: comments.clone(),
            input_proto_type: "google.protobuf.Empty".to_owned(),
            input_type: "()".to_owned(),
            output_proto_type: "google.protobuf.Empty".to_owned(),
            output_type: "()".to_owned(),
            options: method_options,
        }];

        let service_options = ServiceOptions {
            extension_set: ExtensionSet::default(),
            deprecated: None,
            uninterpreted_option: Vec::new(),
        };

        Service {
            name: "TestService".to_owned(),
            proto_name: "TestService".to_owned(),
            package: "test.v1".to_owned(),
            options: service_options,
            comments,
            methods,
        }
    }

    #[test]
    fn generates_service_token_streams() {
        // generate the default test service
        let service = generate_test_service();
        let builder = Builder::default();
        let mut generator = Generator::new(&builder);
        generator.generate(service, &mut String::new());

        // assert that the servers were generated and the clients weren't
        assert!(generator.clients.is_empty());
        assert!(!generator.servers.is_empty());
    }

    #[test]
    fn finalizes_service_tokens() {
        // generate the default test service
        let service = generate_test_service();
        let mut output = String::new();
        let builder = Builder::default();
        let mut generator = Generator::new(&builder);

        // generate the prettified output
        generator.generate(service, &mut output);
        generator.finalize(&mut output);

        assert!(!output.is_empty());

        // FIXME: regression check against known good output
    }

    // TOTEST
    // finalizes_packages
    // generates_postgrpc_implementation
}
