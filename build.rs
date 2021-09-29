use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let file_descriptor = out_dir.join("routes.bin");

    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor)
        .build_client(false)
        .compile(
            &[
                "./proto/postgres.proto",
                "./proto/transaction.proto",
                "./proto/channel.proto",
                "./proto/health.proto",
            ],
            &["./proto"],
        )?;

    Ok(())
}
