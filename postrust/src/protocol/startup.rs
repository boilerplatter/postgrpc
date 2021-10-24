use super::{backend, buffer::Buffer};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use std::{collections::BTreeMap, io};
use tokio_util::codec::{Decoder, Encoder};

/// Constants and magic numbers found in startup sequence
// https://www.postgresql.org/docs/current/protocol-message-formats.html
pub const SSL_REQUEST_CODE: i32 = 80877103;
pub const VERSION: i32 = 196608;

/// Startup-specific messages from the frontend
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Message {
    SslRequest {
        request_code: i32,
    },
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
            return Ok(Some(Self::SslRequest {
                request_code: SSL_REQUEST_CODE,
            }));
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

    /// Write a startup message to bytes
    #[inline]
    pub fn write<B>(self, bytes: &mut B)
    where
        B: BufMut + std::fmt::Debug,
    {
        match self {
            Self::SslRequest { request_code } => {
                bytes.put_i32(8);
                bytes.put_i32(request_code);
            }
            Self::Startup {
                version,
                user,
                options,
            } => {
                let options_length = options.iter().fold(0, |mut length, (key, value)| {
                    length += key.len();
                    length += 1;
                    length += value.len();
                    length += 1;
                    length
                }) as i32;

                let user_key = b"user";
                let user_key_length = user_key.len() + 1;
                let user_value_length = user.len() + 1;
                let user_length = (user_key_length + user_value_length) as i32;

                bytes.put_i32(options_length + user_length + 4 + 4 + 1);
                bytes.put_i32(version);

                bytes.put_slice(b"user");
                bytes.put_u8(0);
                bytes.put_slice(&user);
                bytes.put_u8(0);

                for (key, value) in options {
                    bytes.put_slice(&key);
                    bytes.put_u8(0);
                    bytes.put_slice(&value);
                    bytes.put_u8(0);
                }

                bytes.put_u8(0);
            }
        }
    }
}

/// Special codec for handling startup messages across TCP streams
pub struct Codec;

impl Encoder<backend::Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: backend::Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst);

        Ok(())
    }
}

impl Encoder<Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        tracing::trace!(src = ?&src, "Decoding startup message frame");

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
