use super::{proto::Services, protoc, server, validator::validate_services};
use proc_macro2::TokenStream;
use std::{io, path::Path};

/// Configuration builder for proto compilation
#[derive(Debug, Default)]
pub struct Builder {
    proto_path: String,
    #[cfg(feature = "postgres")]
    connection_string: Option<String>,
}

impl Builder {
    #[cfg(feature = "postgres")]
    /// Provide a database connection string for type validation
    pub fn validate_with(mut self, connection_string: String) -> Self {
        self.connection_string = Some(connection_string);
        self
    }

    /// compile a set of protos and includes with the default build configuration
    pub fn compile(
        self,
        protos: &[impl AsRef<Path>],
        includes: &[impl AsRef<Path>],
    ) -> io::Result<()> {
        self.compile_with_config(prost_build::Config::new(), protos, includes)
    }

    /// compile protos using a [`prost_build::Config`]
    pub fn compile_with_config(
        self,
        mut config: prost_build::Config,
        protos: &[impl AsRef<Path>],
        includes: &[impl AsRef<Path>],
    ) -> io::Result<()> {
        let services = protoc::compile_services(protos, includes)?;

        #[cfg(feature = "postgres")]
        // validate Service methods against the database if there's a connection string
        if let Some(ref connection_string) = self.connection_string {
            validate_services(connection_string, &services)?;
        }

        // generate postgRPC Service implementations
        config.service_generator(Box::new(ServiceGenerator::new(&self, services)));
        config.compile_protos(protos, includes)?;

        Ok(())
    }
}

/// Configure `postgrpc-build` code generation.
pub fn configure() -> Builder {
    Builder {
        proto_path: "super".to_owned(),
        ..Default::default()
    }
}

/// Simple `.proto` compiling. Use [`configure`] instead if you need more options.
///
/// The `include` directory will be the parent folder of the specified path.
/// The package name will be the filename without the extension.
pub fn compile_protos(proto: impl AsRef<Path>) -> io::Result<()> {
    let proto_path: &Path = proto.as_ref();
    let proto_directory = proto_path.parent().ok_or(io::ErrorKind::NotFound)?;

    self::configure().compile(&[proto_path], &[proto_directory])?;

    Ok(())
}

/// Custom Prost-compatible service generator for implemtning gRPC service implemntations for
/// modules with postgrpc-annotated methods
struct ServiceGenerator {
    proto_path: String,
    servers: TokenStream,
    services: Services,
}

impl ServiceGenerator {
    fn new(builder: &Builder, services: Services) -> Self {
        Self {
            proto_path: builder.proto_path.to_owned(),
            servers: TokenStream::default(),
            services,
        }
    }
}

impl prost_build::ServiceGenerator for ServiceGenerator {
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

            println!("{server_service:#}");

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
    use super::{Builder, ServiceGenerator};
    use crate::{
        annotations::{Query, QUERY},
        proto::Services,
    };
    use once_cell::sync::Lazy;
    use prost::ExtensionSet;
    use prost_build::{Comments, Method, Service, ServiceGenerator as _};
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
        let mut generator = ServiceGenerator::new(&builder, protos);
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
        let mut generator = ServiceGenerator::new(&builder, protos);

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
