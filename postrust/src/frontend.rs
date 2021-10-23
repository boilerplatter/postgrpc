#![allow(clippy::enum_variant_names)]

// TODO: contribute this work to postgres_protocol
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use memchr::memchr;
use std::{
    cmp,
    collections::BTreeMap,
    io::{self, Read},
};
use tokio_util::codec::{Decoder, Encoder};

/// Byte tags for relevant frontend message variants
const PASSWORD_MESSAGE_TAG: u8 = b'p';
const QUERY_TAG: u8 = b'Q';

/// Codec for decoding frontend messages from TCP streams
pub struct MessageCodec;

// FIXME: implement Encoder for frontend and backend messages
impl Encoder<BytesMut> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item);

        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        tracing::debug!(src = ?&src, "Decoding message frame");

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

        // send the frame along
        Ok(Some(frame))
    }
}

// TODO: reorganize and get better names
pub struct HandshakeCodec;

// FIXME: encode _backend_ messages here instead of bare bytes
impl Encoder<BytesMut> for HandshakeCodec {
    type Error = io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item);

        Ok(())
    }
}

impl Decoder for HandshakeCodec {
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

/// Connection handshake's frontend process as an ordered enum
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Handshake {
    SslRequest,
    Startup {
        version: i32,
        user: Bytes,
        options: BTreeMap<Bytes, Bytes>,
    },
}

impl Handshake {
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

/// In-session Postgres frontend message variants that Postrust cares about
// FIXME: pare this list down to relevant messages plus a catchall message for forwarding
pub enum Message {
    PasswordMessage(PasswordMessageBody),
    Query(QueryBody),
    // catchall for frames we don't care about
    // TODO: audit other message types for ill intent
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
}

/// Body types for messages with payloads
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
    pub fn password(self, _salt: [u8; 4]) -> Bytes {
        // FIXME:
        // use md5 to check/decode? or re-hash with different salt?
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

/// Buffer wrapper type from postgres-protocol
// https://docs.rs/postgres-protocol/0.6.2/src/postgres_protocol/message/backend.rs.html#281
struct Buffer {
    bytes: Bytes,
    idx: usize,
}

impl Buffer {
    #[inline]
    fn slice(&self) -> &[u8] {
        &self.bytes[self.idx..]
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.slice().is_empty()
    }

    #[inline]
    fn read_cstr(&mut self) -> io::Result<Bytes> {
        match memchr(0, self.slice()) {
            Some(pos) => {
                let start = self.idx;
                let end = start + pos;
                let cstr = self.bytes.slice(start..end);
                self.idx = end + 1;
                Ok(cstr)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }
}

impl Read for Buffer {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = {
            let slice = self.slice();
            let len = cmp::min(slice.len(), buf.len());
            buf[..len].copy_from_slice(&slice[..len]);
            len
        };
        self.idx += len;
        Ok(len)
    }
}
