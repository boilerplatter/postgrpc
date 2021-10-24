#![allow(clippy::enum_variant_names)]

// TODO: contribute more comprehensive frontend message parsing work to postgres_protocol
use super::{backend, buffer::Buffer};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Byte tags for relevant frontend message variants
const PASSWORD_MESSAGE_TAG: u8 = b'p';
const QUERY_TAG: u8 = b'Q';

/// Post-startup Postgres frontend message variants that Postrust cares about
pub enum Message {
    SASLInitialResponse(SASLInitialResponseBody),
    SASLResponse(SASLResponseBody),
    PasswordMessage(PasswordMessageBody),
    Query(QueryBody),
    // catchall for frames we don't care about
    // FIXME: audit other message types for ill intent
    // especially from programmatic clients
    Forward(Bytes),
}

impl Message {
    /// Parse a single message frame of known and established length
    #[inline]
    pub fn parse(buf: &mut BytesMut) -> io::Result<Option<Message>> {
        let tag = buf[0];
        let len = (&buf[1..5]).read_u32::<BigEndian>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: reading u32",
            )
        })?;

        let total_len = len as usize + 1;

        let mut buf = Buffer {
            bytes: buf.split_to(total_len).freeze(),
            idx: 5,
        };

        let message = match tag {
            PASSWORD_MESSAGE_TAG => {
                let password = buf.read_cstr()?;

                Message::PasswordMessage(PasswordMessageBody { password })
            }
            QUERY_TAG => {
                let query = buf.read_cstr()?;

                Message::Query(QueryBody { query })
            }
            _ => Message::Forward(buf.bytes),
        };

        Ok(Some(message))
    }

    /// Write a frontend message to bytes
    #[inline]
    pub fn write<B>(self, bytes: &mut B)
    where
        B: BufMut + std::fmt::Debug,
    {
        match self {
            Self::PasswordMessage(PasswordMessageBody { password }) => {
                bytes.put_u8(PASSWORD_MESSAGE_TAG);
                bytes.put_i32(4 + (password.len() as i32) + 1);
                bytes.put_slice(&password);
                bytes.put_u8(0);
            }
            Self::SASLInitialResponse(SASLInitialResponseBody {
                mechanism,
                initial_response,
            }) => {
                bytes.put_u8(PASSWORD_MESSAGE_TAG);
                bytes.put_i32(
                    4 + (mechanism.len() as i32 + 1) + 4 + (initial_response.len() as i32),
                );
                bytes.put_slice(&mechanism);
                bytes.put_u8(0);
                bytes.put_i32(initial_response.len() as i32);
                bytes.put_slice(&initial_response);
            }
            Self::SASLResponse(SASLResponseBody { data }) => {
                bytes.put_u8(PASSWORD_MESSAGE_TAG);
                bytes.put_i32(4 + (data.len() as i32));
                bytes.put_slice(&data);
            }
            Self::Query(QueryBody { query }) => {
                bytes.put_u8(QUERY_TAG);
                bytes.put_i32(4 + (query.len() as i32) + 1);
                bytes.put_slice(&query);
                bytes.put_u8(0);
            }
            Self::Forward(frame) => {
                bytes.put_slice(&frame);
            }
        }
    }
}

/// Body types for messages with payloads
#[derive(Debug)]
pub struct SASLInitialResponseBody {
    pub mechanism: Bytes,
    pub initial_response: Bytes,
}

#[derive(Debug)]
pub struct SASLResponseBody {
    pub data: Bytes,
}

#[derive(Debug)]
pub struct ParseBody {
    name: Bytes,
    query: Bytes,
    storage: Bytes,
}

pub struct PasswordMessageBody {
    password: Bytes,
}

impl PasswordMessageBody {
    pub fn cleartext_password(self) -> Bytes {
        self.password.slice(..)
    }
}

#[derive(Debug)]
pub struct QueryBody {
    query: Bytes,
}

impl QueryBody {
    #[inline]
    pub fn query(&self) -> Bytes {
        self.query.slice(..)
    }
}

/// Codec for handling frontend messages to and from TCP streams
pub struct Codec;

impl Encoder<backend::Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: backend::Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        tracing::trace!(src = ?&src, "Decoding frontend message frame");

        // wait for at least enough data to determine message length
        if src.len() < 5 {
            let to_read = 5 - src.len();
            src.reserve(to_read);
            return Ok(None);
        }

        // get the length from the message itself
        let len = (&src[1..5]).read_u32::<BigEndian>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: reading u32",
            )
        })? as usize;

        // check that we have the entire message to parse
        let total_len = len + 1;
        if src.len() < total_len {
            let to_read = total_len - src.len();
            src.reserve(to_read);
            return Ok(None);
        }

        let frame = src.split_to(total_len);

        Ok(Some(frame))
    }
}
