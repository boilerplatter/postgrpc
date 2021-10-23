use super::{backend, buffer::Buffer};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use std::{collections::BTreeMap, io};
use tokio_util::codec::{Decoder, Encoder};

/// Startup-specific messages from the frontend
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Message {
    SslRequest,
    Startup {
        version: i32,
        user: Bytes,
        options: BTreeMap<Bytes, Bytes>,
    },
}

impl Message {
    /// Parse a single startup frame of known and established length
    #[inline]
    pub fn parse(buf: &mut BytesMut) -> io::Result<Option<Self>> {
        let len = buf.len() as usize;

        // length of 8 is a special case where SslRequest is sent first
        if len == 8 {
            return Ok(Some(Self::SslRequest));
        }

        // parse the rest of the message
        let mut buf = Buffer {
            bytes: buf.split_to(len).freeze(),
            idx: 4,
        };

        let version = buf.read_i32::<BigEndian>()?;
        let mut user = None;
        #[allow(clippy::mutable_key_type)]
        let mut options = BTreeMap::new();

        while !buf.is_empty() {
            let parameter = buf.read_cstr()?;

            if !parameter.is_empty() {
                if parameter == "user" {
                    user = buf.read_cstr().map(Option::Some)?;
                } else {
                    let value = buf.read_cstr()?;

                    options.insert(parameter, value);
                }
            }
        }

        let user = user.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid startup message: missing required user",
            )
        })?;

        Ok(Some(Self::Startup {
            user,
            version,
            options,
        }))
    }
}

/// Codec for decoding startup messages from TCP streams
pub struct Codec;

impl Encoder<backend::Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: backend::Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(item.write(dst))
    }
}

impl Decoder for Codec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        tracing::debug!(src = ?&src, "Decoding startup frame");

        // wait for at least enough data to determine message length
        if src.len() < 4 {
            let to_read = 4 - src.len();
            src.reserve(to_read);
            return Ok(None);
        }

        // get the length from the message itself
        let len = (&src[..4]).read_u32::<BigEndian>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: reading u32",
            )
        })? as usize;

        // check that we have the entire message to parse
        if src.len() < len {
            let to_read = len - src.len();
            src.reserve(to_read);
            return Ok(None);
        };

        let frame = src.split_to(len);

        // send the frame along
        Ok(Some(frame))
    }
}
