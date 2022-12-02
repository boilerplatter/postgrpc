use super::service_generator::Generator;
use std::{io, path::Path};

/// Configuration builder for proto compilation
#[derive(Debug, Default)]
pub struct Builder {
    pub(crate) build_client: bool,
    pub(crate) proto_path: String,
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
        #[cfg(feature = "postgres")]
        // validate Service methods against the database if there's a connection string
        if let Some(ref connection_string) = self.connection_string {
            super::validator::validate(connection_string, protos, includes)?;
        }

        // generate postgRPC Service implementations
        config.service_generator(Box::new(Generator::new(&self)));
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

/// Simple `.proto` compiling. Use [`configure`] instead oif you need more options.
///
/// The `include` directory will be the parent folder of the specified path.
/// The package name will be the filename without the extension.
pub fn compile_protos(proto: impl AsRef<Path>) -> io::Result<()> {
    let proto_path: &Path = proto.as_ref();
    let proto_directory = proto_path.parent().ok_or(io::ErrorKind::NotFound)?;

    self::configure().compile(&[proto_path], &[proto_directory])?;

    Ok(())
}
