fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[allow(unused_mut)]
    let mut routes = vec!["./proto/postgres.proto"];

    #[cfg(feature = "health")]
    routes.push("./proto/health.proto");
    #[cfg(feature = "transaction")]
    routes.push("./proto/transaction.proto");

    #[allow(unused_mut)]
    let mut builder = tonic_build::configure();

    #[cfg(feature = "reflection")]
    {
        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
        let file_descriptor = out_dir.join("routes.bin");
        builder = builder.file_descriptor_set_path(file_descriptor)
    };

    builder.build_client(false).compile(&routes, &["./proto"])?;

    Ok(())
}
