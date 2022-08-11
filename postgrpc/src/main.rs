// FIXME: document running the server here

mod server;

#[tokio::main]
async fn main() {
    if let Err(error) = server::run().await {
        tracing::error!(%error, "PostgRPC service error! Process stopped");
    }
}
