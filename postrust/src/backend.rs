use std::io;
use tokio::{io::AsyncWriteExt, net::TcpStream};

/// Handshake backend message types for initiating connections
#[derive(Debug)]
pub enum Handshake {
    AuthenticationMd5Password { salt: [u8; 4] },
}

impl Handshake {
    pub async fn respond(&self, socket: &mut TcpStream) -> io::Result<()> {
        match self {
            Self::AuthenticationMd5Password { salt } => {
                socket.write_u8(b'R').await?;
                socket.write_i32(12).await?;
                socket.write_i32(5).await?;
                socket.write_all(salt).await?;

                Ok(())
            }
        }
    }
}
