use super::annotations::QUERY;
use prost::{ExtensionRegistry, Message};
use prost_build::protoc;
use prost_types::FileDescriptorSet;
use std::{fs, io, path::Path, process::Command};

/// Configuration builder for proto compilation
#[derive(Debug, Default)]
pub struct Builder {
    build_client: bool,
    #[cfg(feature = "postgres")]
    connection_string: Option<String>, // FIXME: feature-gate this
}

impl Builder {
    /// Enable or disable gRPC client code generation.
    pub fn build_client(mut self, enable: bool) -> Self {
        self.build_client = enable;
        self
    }

    #[cfg(feature = "postgres")]
    /// Provide a database connection string for type validation
    pub fn validate_with(mut self, connection_string: String) -> Self {
        self.connection_string = Some(connection_string);
        self
    }

    /// compile protos using the configuration
    // FIXME: make sure that this behaves exactly the same as tonic-build, if possible!
    pub fn compile_protos(self, proto: impl AsRef<Path>) -> io::Result<()> {
        for file_descriptor_proto in compile_file_descriptors(proto)?.file {
            #[cfg(feature = "postgres")]
            // validate types if a connection string is provided
            if let Some(connection_string) = &self.connection_string {
                // extract the message structs from the original file descriptor
                let messages = file_descriptor_proto
                    .message_type
                    .into_iter()
                    .map(|descriptor| (descriptor.name().to_string(), descriptor))
                    .collect::<std::collections::HashMap<_, _>>();

                // extract the postgrpc-annotated methods from the original file descriptor
                let methods = file_descriptor_proto
                    .service
                    .into_iter()
                    .flat_map(|service| service.method.into_iter())
                    .filter_map(|method| {
                        super::proto::Method::from_method_descriptor(method, &messages).transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // set up the postgres client for validation
                let mut client =
                postgres::Client::connect(connection_string, postgres::NoTls) // FIXME: enable TLS
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

                // validate the inputs and outputs of each postgrpc-annotated method
                for method in methods {
                    println!("Validating rpc {} against the database", method.name());

                    // prepare the method's SQL query with the database
                    let statement = client
                        .prepare(method.query())
                        .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

                    // check the input message fields against the parameter types
                    method.validate_input(statement.params())?;

                    // check the output message fields against the column types
                    method.validate_output(statement.columns())?;
                }
            }

            // TODO: generate postgrpc service implementations (output to file system)
        }

        // let requests = file_descriptor_set
        //     .file
        //     .clone() // FIXME
        //     .into_iter()
        //     .map(|descriptor| {
        //         (
        //             Module::from_protobuf_package_name(descriptor.package()),
        //             descriptor,
        //         )
        //     })
        //     .collect::<Vec<_>>();

        // generate token stream of Prost types
        // FIXME: use prost AST instead of syn AST
        // we have to have information on method types and configured options that are impossible
        // to parse from the prost_build/Rust outputs
        // let mut prost_config = prost_build::Config::new();

        // FIXME: add back custom service generator(?)
        // prost_config.service_generator(Box::new(ServiceGenerator::new(&self)));
        // let prost_outputs = prost_config.generate(requests)?; // FIXME: do we NEED to generate here?

        Ok(())
    }
}

/// Configure `postgrpc-build` code generation.
pub fn configure() -> Builder {
    Builder::default()
}

/// Compile file descriptors with external `protoc` compiler
fn compile_file_descriptors(proto: impl AsRef<Path>) -> io::Result<FileDescriptorSet> {
    // generate target paths from the given proto path
    let proto_path: &Path = proto.as_ref();
    let proto_directory = proto_path.parent().ok_or(io::ErrorKind::NotFound)?;

    // compile the relevant file descriptors with protoc
    let tmp = tempfile::Builder::new().prefix("prost-build").tempdir()?;
    let file_descriptor_set_path = tmp.path().join("prost-descriptor-set");

    let protoc = protoc();
    let mut cmd = Command::new(protoc.clone());

    cmd.arg("--include_imports")
        .arg("--include_source_info")
        .arg("-o")
        .arg(&file_descriptor_set_path);

    // FIXME: include more configurable INCLUDEs
    if proto_directory.exists() {
        cmd.arg("-I").arg(proto_directory);
        // FIXME
        cmd.arg("-I").arg("./proto");
    } else {
        println!(
            "ignoring {} since it does not exist.",
            proto_directory.display()
        )
    }

    cmd.arg(proto_path);

    println!("Running: {:?}", cmd);

    let output = cmd.output().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to invoke protoc (hint: https://docs.rs/prost-build/#sourcing-protoc): (path: {protoc:?}): {error}"),
        )
    })?;

    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("protoc failed: {}", String::from_utf8_lossy(&output.stderr)),
        ));
    }

    let buf = fs::read(&file_descriptor_set_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("unable to open file_descriptor_set_path: {file_descriptor_set_path:?}, OS: {error}"),
        )
    })?;

    let mut extension_registry = ExtensionRegistry::new();
    extension_registry.register(QUERY);

    FileDescriptorSet::decode_with_extensions(&*buf, extension_registry).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid FileDescriptorSet: {}", error),
        )
    })
}

#[cfg(test)]
mod test {
    // FIXME: convert to real unit tests or real integration tests (instead of halfway between)
    use super::configure;

    #[test]
    fn runs_without_validation() {
        configure()
            .compile_protos("./examples/bookstore/authors.proto")
            .unwrap();
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn runs_with_validation() {
        configure()
            .validate_with("postgresql://postgres:supersecretpassword@localhost:5432".to_owned())
            .compile_protos("./examples/bookstore/authors.proto")
            .unwrap();
    }
}

// FIXME: implement custom service generator a la tonic_build
// struct ServiceGenerator {
//     build_client: bool,
//     proto_path: String,
//     clients: TokenStream,
//     servers: TokenStream,
// }

// impl ServiceGenerator {
//     fn new(builder: &Builder) -> Self {
//         Self {
//             build_client: builder.build_client,
//             proto_path: builder.proto_path.to_owned(),
//             clients: TokenStream::default(),
//             servers: TokenStream::default(),
//         }
//     }
// }

// impl prost_build::ServiceGenerator for ServiceGenerator {
//     // FIXME: generate postgrpc service implementations instead of tonic!
//     fn generate(&mut self, service: prost_build::Service, _buffer: &mut String) {
//         let server = CodeGenBuilder::new().generate_server(&service, &self.proto_path);

//         self.servers.extend(server);

//         if self.build_client {
//             let client = CodeGenBuilder::new().generate_client(&service, &self.proto_path);

//             self.clients.extend(client);
//         }
//     }

//     fn finalize(&mut self, buffer: &mut String) {
//         if self.build_client && !self.clients.is_empty() {
//             let clients = &self.clients;

//             let client_service = quote::quote! {
//                 #clients
//             };

//             let ast: syn::File = syn::parse2(client_service).expect("not a valid tokenstream");
//             let code = prettyplease::unparse(&ast);
//             buffer.push_str(&code);

//             self.clients = TokenStream::default();
//         }

//         if !self.servers.is_empty() {
//             let servers = &self.servers;

//             let server_service = quote::quote! {
//                 #servers
//             };

//             let ast: syn::File = syn::parse2(server_service).expect("not a valid tokenstream");
//             let code = prettyplease::unparse(&ast);
//             buffer.push_str(&code);

//             self.servers = TokenStream::default();
//         }
//     }

//     fn finalize_package(&mut self, _package: &str, buf: &mut String) {
//         // FIXME: consider package-specific finalizers where there is more than one package
//         self.finalize(buf);
//     }
// }
