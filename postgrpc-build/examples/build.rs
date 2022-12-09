fn main() -> Result<(), Box<dyn std::error::Error>> {
    // FIXME: include annotations.proto in postgrpc_build
    postgrpc_build::configure()
        .validate_with("postgresql://postgres:supersecretpassword@localhost:5432".to_owned())
        .compile(
            &["./bookstore/authors.proto"],
            &["./bookstore", "../../postgrpc-build/proto"],
        )?;

    Ok(())
}