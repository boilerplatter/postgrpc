use super::postgres::PostgresType;
use ::prost::{extension::ExtensionSetError, Extendable, ExtensionRegistry, Message};
use postgres::{Client, NoTls};
use prost_build::protoc;
use prost_types::{FileDescriptorSet, MethodDescriptorProto};
use std::{collections::HashMap, fs, io, path::Path, process::Command};

#[allow(unreachable_pub)]
mod annotations {
    include!("../gen/postgrpc.rs");
}

/// Configuration builder for proto compilation
#[derive(Debug, Default)]
pub struct Builder {
    build_client: bool,
    connection_string: Option<String>, // FIXME: feature-gate this
}

impl Builder {
    /// Enable or disable gRPC client code generation.
    pub fn build_client(mut self, enable: bool) -> Self {
        self.build_client = enable;
        self
    }

    /// Provide a database connection string for type validation
    pub fn connection_string(mut self, connection_string: String) -> Self {
        self.connection_string = Some(connection_string);
        self
    }

    /// compile protos using the configuration
    pub fn compile_protos(self, proto: impl AsRef<Path>) -> io::Result<()> {
        let connection_string = self.connection_string.as_ref();
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
                format!(
                    "failed to invoke protoc (hint: https://docs.rs/prost-build/#sourcing-protoc): (path: {:?}): {}",
                    &protoc,
                    error,
                ),
            )
        })?;

        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("protoc failed: {}", String::from_utf8_lossy(&output.stderr)),
            ));
        }

        let buf = fs::read(&file_descriptor_set_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "unable to open file_descriptor_set_path: {:?}, OS: {}",
                    &file_descriptor_set_path, e
                ),
            )
        })?;

        let mut extension_registry = ExtensionRegistry::new();
        extension_registry.register(annotations::QUERY);
        let file_descriptor_set =
            FileDescriptorSet::decode_with_extensions(&*buf, extension_registry).map_err(
                |error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid FileDescriptorSet: {}", error),
                    )
                },
            )?;

        // FIXME: iterate over every module instead of hard-coding
        let authors_proto = file_descriptor_set
            .file
            .into_iter()
            .find(|descriptor| descriptor.package() == "authors.v1");

        // validate types if a connection string is provided
        if let (Some(connection_string), Some(file_descriptor_proto)) = (
            connection_string,
            // FIXME: perform these checks for each relevant module, not just authors
            // (is it possible to filter to just those modules mentioned in protos?)
            authors_proto,
        ) {
            // extract the message structs from the original file descriptor
            let messages = file_descriptor_proto
                .message_type
                .into_iter()
                .map(|descriptor| (descriptor.name().to_string(), descriptor))
                .collect::<HashMap<_, _>>();

            // extract the postgrpc-annotated methods from the original file descriptor
            let postgrpc_methods = file_descriptor_proto
                .service
                .into_iter()
                .flat_map(|service| service.method.into_iter())
                .filter_map(|method| {
                    PostgrpcMethodProto::from_method_descriptor(method).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;

            // set up the postgres client for validation
            let mut client =
                Client::connect(connection_string, NoTls) // FIXME: enable TLS by default
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

            // validate the inputs and outputs of each postgrpc-annotated method
            for method in postgrpc_methods {
                // prepare the query with the database
                let statement = client
                    .prepare(&method.query)
                    .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;

                // check the input message fields against the parameter types
                match (method.input_type.as_str(), statement.params()) {
                    (".google.protobuf.Empty", params) => {
                        if !params.is_empty() {
                            return Err(
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "{} method has an Empty input, but {} parameters were expected by the SQL query",
                                        method.name,
                                        params.len()
                                    )
                                ));
                        }
                    }
                    (input, params) => {
                        if let Some(message_name) = input.split('.').last() {
                            let fields = &messages
                                .get(message_name)
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!(
                                            "Expected message {message_name}, but it doesn't exist"
                                        ),
                                    )
                                })?
                                .field;

                            if fields.len() != params.len() {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "Expected {} parameters, but {message_name} has {} fields",
                                        params.len(),
                                        fields.len(),
                                    ),
                                ));
                            }

                            for (field, param) in fields.iter().zip(params.iter()) {
                                if PostgresType::from(param) != field.r#type() {
                                    return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "Expected field {} of message {message_name} to be of type {param}, but it was incompatible proto type {:?}",
                                        field.name(),
                                        field.r#type(),
                                    )
                                ));
                                }
                            }
                        }
                    }
                }

                // check the output message fields against the column types
                match (method.output_type.as_str(), statement.columns()) {
                    (".google.protobuf.Empty", columns) => {
                        if !columns.is_empty() {
                            return Err(
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "{} method has an Empty output, but {} columns were returned by the SQL query",
                                        method.name,
                                        columns.len()
                                    )
                                ));
                        }
                    }
                    (output, columns) => {
                        if let Some(message_name) = output.split('.').last() {
                            let fields = &messages
                                .get(message_name)
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!(
                                            "Expected message {message_name}, but it doesn't exist"
                                        ),
                                    )
                                })?
                                .field;

                            if fields.len() != columns.len() {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "Expected {} columns, but {message_name} has {} fields",
                                        columns.len(),
                                        fields.len(),
                                    ),
                                ));
                            }

                            for (field, column) in fields.iter().zip(columns.iter()) {
                                let column = column.type_();

                                if PostgresType::from(column) != field.r#type() {
                                    return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "Expected field {} of message {message_name} to be of type {column}, but it was incompatible proto type {:?}",
                                        field.name(),
                                        field.r#type(),
                                    )
                                ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // TODO: generate postgrpc service implementations (output to file system)

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

        panic!("test ended");

        Ok(())
    }
}

/// Configure `postgrpc-build` code generation.
pub fn configure() -> Builder {
    Builder::default()
}

/// Postgrpc-specific variant of a service method
/// based on [`prost_types::MethodDescriptorProto`]
#[derive(Debug)]
struct PostgrpcMethodProto {
    name: String,
    input_type: String,
    output_type: String,
    query: String,
    client_streaming: bool,
    server_streaming: bool,
}

impl PostgrpcMethodProto {
    fn from_method_descriptor(method: MethodDescriptorProto) -> Result<Option<Self>, io::Error> {
        let name = method.name().to_string();
        let input_type = method.input_type().to_string();
        let output_type = method.output_type().to_string();
        let client_streaming = method.client_streaming();
        let server_streaming = method.server_streaming();

        if let Some(options) = method.options {
            match options.extension_data(annotations::QUERY) {
                Ok(annotations::Query {
                    source: Some(source),
                }) => {
                    let query = match source {
                        annotations::query::Source::Sql(sql) => sql.to_owned(),
                        annotations::query::Source::File(path) => fs::read_to_string(path)?,
                    };

                    return Ok(Some(Self {
                        name,
                        input_type,
                        output_type,
                        client_streaming,
                        server_streaming,
                        query,
                    }));
                }
                Ok(..) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "postgrpc query is missing a valid source",
                    ))
                }
                Err(ExtensionSetError::ExtensionNotFound) => return Ok(None),
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            };
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::configure;

    #[test]
    fn runs() {
        configure()
            .connection_string(
                "postgresql://postgres:supersecretpassword@localhost:5432".to_owned(),
            )
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
