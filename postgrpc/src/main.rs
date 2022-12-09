// FIXME: document running the server here

#[cfg(feature = "server")]
mod server;

#[tokio::main]
async fn main() {
    #[cfg(feature = "server")]
    if let Err(error) = server::run().await {
        tracing::error!(%error, "PostgRPC service error! Process stopped");
    }
}
