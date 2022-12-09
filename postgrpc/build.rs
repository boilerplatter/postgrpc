fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "server")]
    {
        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
        let descriptor_path = out_dir.join("routes.bin");

        #[allow(unused_mut)]
        let mut routes = vec!["./proto/postgres.proto"];
        let mut packages = vec![".postgres.v1"];

        #[cfg(feature = "transaction")]
        {
            routes.push("./proto/transaction.proto");
            packages.push(".transaction.v1");
        }

        #[allow(unused_mut)]
        let mut builder = tonic_build::configure();

        #[cfg(feature = "reflection")]
        {
            builder = builder.file_descriptor_set_path(&descriptor_path);
        };

        builder
            .build_client(false)
            .compile_well_known_types(true)
            .extern_path(".google.protobuf", "::pbjson_types")
            .compile(&routes, &["./proto"])?;

        let descriptor_set = std::fs::read(descriptor_path)?;

        pbjson_build::Builder::new()
            .register_descriptors(&descriptor_set)?
            .build(&packages)?;
    }

    Ok(())
}
