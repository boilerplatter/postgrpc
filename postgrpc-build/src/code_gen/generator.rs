use super::{server, Builder};
use crate::proto::Services;
use proc_macro2::TokenStream;

/// Custom Prost-compatible service generator for implemtning gRPC service implemntations for
/// modules with postgrpc-annotated methods
pub(crate) struct Generator {
    proto_path: String,
    servers: TokenStream,
    services: Services,
}

impl Generator {
    pub(crate) fn new(builder: &Builder, services: Services) -> Self {
        Self {
            proto_path: builder.proto_path.to_owned(),
            servers: TokenStream::default(),
            services,
        }
    }
}

impl prost_build::ServiceGenerator for Generator {
    fn generate(&mut self, service: prost_build::Service, _buffer: &mut String) {
        let proto_service = self
            .services
            .try_get(&format!(".{}.{}", service.package, service.name))
            .expect("Error resolving service module")
            .expect("Service not found in the file descriptor set");

        let server = server::generate(&service, proto_service, &self.proto_path);

        self.servers.extend(server);
    }

    fn finalize(&mut self, buffer: &mut String) {
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
    use crate::annotations::{Query, QUERY};
    use crate::{code_gen::Builder, proto::Services};
    use once_cell::sync::Lazy;
    use prost::ExtensionSet;
    use prost_build::{Comments, Method, Service, ServiceGenerator};
    use prost_types::{
        FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, MethodOptions,
        ServiceDescriptorProto, ServiceOptions,
    };

    static SERVICE_NAME: &str = "TestService";
    static METHOD_NAME: &str = "TestMethod";
    static PACKAGE: &str = "test.v1";
    static EMPTY_MESSAGE: &str = ".google.protobuf.Empty";
    static FILE_DESCRIPTOR_SET: Lazy<FileDescriptorSet> = Lazy::new(|| {
        // create a test method annotation
        let mut extension_set = ExtensionSet::default();
        extension_set
            .set_extension_data(
                QUERY,
                Query {
                    source: Some(crate::annotations::query::Source::Sql(
                        "create table if not exists authors ()".to_owned(),
                    )),
                },
            )
            .unwrap();

        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                package: Some(PACKAGE.to_owned()),
                service: vec![ServiceDescriptorProto {
                    name: Some(SERVICE_NAME.to_owned()),
                    method: vec![MethodDescriptorProto {
                        name: Some(METHOD_NAME.to_owned()),
                        input_type: Some(EMPTY_MESSAGE.to_owned()),
                        output_type: Some(EMPTY_MESSAGE.to_owned()),
                        server_streaming: Some(true),
                        options: Some(MethodOptions {
                            extension_set,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    });

    // FIXME: add useful configuration options
    fn generate_test_service() -> (Service, Services) {
        let services = Services::from_file_descriptor_set(FILE_DESCRIPTOR_SET.clone()).unwrap();

        let comments = Comments {
            leading_detached: Vec::new(),
            leading: Vec::new(),
            trailing: Vec::new(),
        };

        let method_options = MethodOptions {
            deprecated: None,
            uninterpreted_option: Vec::new(),
            idempotency_level: None,
            extension_set: ExtensionSet::default(),
        };

        // FIXME: test all client + server streaming combinations
        let methods = vec![Method {
            name: METHOD_NAME.to_owned(),
            proto_name: METHOD_NAME.to_owned(),
            client_streaming: false,
            server_streaming: true,
            comments: comments.clone(),
            input_proto_type: EMPTY_MESSAGE.to_owned(),
            input_type: "()".to_owned(),
            output_proto_type: EMPTY_MESSAGE.to_owned(),
            output_type: "()".to_owned(),
            options: method_options,
        }];

        let service_options = ServiceOptions {
            extension_set: ExtensionSet::default(),
            deprecated: None,
            uninterpreted_option: Vec::new(),
        };

        (
            Service {
                name: SERVICE_NAME.to_owned(),
                proto_name: SERVICE_NAME.to_owned(),
                package: PACKAGE.to_owned(),
                options: service_options,
                comments,
                methods,
            },
            services,
        )
    }

    #[test]
    fn generates_service_token_streams() {
        // generate the default test service
        let (service, protos) = generate_test_service();
        let builder = Builder::default();
        let mut generator = Generator::new(&builder, protos);
        generator.generate(service, &mut String::new());

        // assert that the service implementations were generated
        assert!(!generator.servers.is_empty());
    }

    #[test]
    fn finalizes_service_tokens() {
        // generate the default test service
        let (service, protos) = generate_test_service();
        let mut output = String::new();
        let builder = Builder::default();
        let mut generator = Generator::new(&builder, protos);

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
