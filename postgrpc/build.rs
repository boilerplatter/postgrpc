use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let file_descriptor = out_dir.join("routes.bin");
    let mut routes = vec!["./proto/postgres.proto"];

    #[cfg(feature = "health")]
    routes.push("./proto/health.proto");
    #[cfg(feature = "transaction")]
    routes.push("./proto/transaction.proto");

    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor)
        .build_client(false)
        .compile(&routes, &["./proto"])?;

    Ok(())
}
