use postgrpc::pools::deadpool;
use std::sync::Arc;
use tonic::transport::Server;

#[allow(unused)]
mod proto {
    // FIXME: turn this into an "include_protos" (or maybe use tonic's directly?)
    include!(concat!(env!("OUT_DIR"), concat!("/", "authors.v1", ".rs")));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = deadpool::Configuration {
        pgpassword: "supersecretpassword".to_owned(),
        ..Default::default()
    }
    .create_pool()
    .map(Arc::new)?;

    Server::builder()
        .add_service(proto::new(pool))
        .serve(([0, 0, 0, 0], 50051).into())
        .await?;

    Ok(())
}
