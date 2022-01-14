use crate::{
    endpoint::Endpoint,
    protocol::{
        backend,
        frontend::{self, SASLInitialResponseBody, SASLResponseBody},
    },
    tcp::{self, Connection},
};
use futures_util::{SinkExt, TryStreamExt};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postguard::{AllowedFunctions, AllowedStatements, Guard};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Tcp(#[from] tcp::Error),
    #[error("Connection attempt failed authorization step")]
    Unauthorized,
}

// TODO: turn this into a proto for gRPC handlers, too
// FIXME: move this to a separate cluster module
/// Response from the auth query (remote or otherwise)
#[derive(Debug)]
pub struct ClusterConfiguration {
    pub leaders: Vec<Endpoint>,
    pub followers: Vec<Endpoint>,
    pub statement_guard: Guard,
}

impl Default for ClusterConfiguration {
    // FIXME: remove this impl
    fn default() -> Self {
        let leader = Endpoint::new(
            "postgres".into(),
            "supersecretpassword".into(),
            "postgres".into(),
            [127, 0, 0, 1].into(),
            5432,
        );

        // FIXME: make this something other than a noop
        let statement_guard = Guard::new(
            // AllowedStatements::List(vec![Command::Select]),
            AllowedStatements::All,
            AllowedFunctions::All,
            // AllowedFunctions::List(vec!["to_json".to_string(), "pg_sleep".to_string()]),
        );

        Self {
            leaders: vec![leader],
            followers: vec![],
            statement_guard,
        }
    }
}

/// Handle authentication of an upstream Postgres connection
pub async fn authenticate(
    upstream: &mut Connection<backend::Codec>,
    password: &[u8],
) -> Result<(), Error> {
    match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
        backend::Message::AuthenticationSASL { mechanisms } => {
            let mechanism = mechanisms.first().cloned().ok_or(Error::Unauthorized)?; // FIXME: better error here

            // construct a ScramSha256
            let channel_binding = ChannelBinding::unrequested(); // is this right?
            let mut scram_client = ScramSha256::new(password, channel_binding);

            // send out a SASLInitialResponse message
            upstream
                .send(frontend::Message::SASLInitialResponse(
                    SASLInitialResponseBody {
                        mechanism,
                        initial_response: scram_client.message().to_vec().into(),
                    },
                ))
                .await?;

            // wait for the SASL continuation message from upstream
            match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
                backend::Message::AuthenticationSASLContinue { data } => {
                    scram_client
                        .update(&data)
                        .map_err(|_| Error::Unauthorized)?;
                }
                _ => return Err(Error::Unauthorized),
            }

            // send out a SASLResponse message
            upstream
                .send(frontend::Message::SASLResponse(SASLResponseBody {
                    data: scram_client.message().to_vec().into(),
                }))
                .await?;

            // wait for the final SASL message from upstream
            match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
                backend::Message::AuthenticationSASLFinal { data } => {
                    scram_client
                        .finish(&data)
                        .map_err(|_| Error::Unauthorized)?;
                }
                _ => return Err(Error::Unauthorized),
            }

            // wait for AuthenticationOk then carry on
            match upstream.try_next().await?.ok_or(Error::Unauthorized)? {
                backend::Message::AuthenticationOk => (),
                _ => return Err(Error::Unauthorized),
            }

            tracing::debug!("SASL handshake completed");
        }
        backend::Message::AuthenticationOk => {
            // all good, carry on
        }
        _ => {
            // TODO: support other auth schemes
            // reject all other responses
            return Err(Error::Unauthorized);
        }
    }

    Ok(())
}
