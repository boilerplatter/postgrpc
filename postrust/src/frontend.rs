#![allow(clippy::enum_variant_names)]

// TODO: contribute this work to postgres_protocol
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use memchr::memchr;
use std::{
    cmp,
    collections::BTreeMap,
    io::{self, Read},
};
use tokio_util::codec::{Decoder, Encoder};

/// Byte tags for each frontend message variant
const BIND_TAG: u8 = b'B';
const CLOSE_TAG: u8 = b'C';
const COPY_DATA_TAG: u8 = b'd';
const COPY_DONE_TAG: u8 = b'c';
const COPY_FAIL_TAG: u8 = b'f';
const DESCRIBE_TAG: u8 = b'D';
const EXECUTE_TAG: u8 = b'E';
const FLUSH_TAG: u8 = b'H';
const FUNCTION_CALL_TAG: u8 = b'F';
const PARSE_TAG: u8 = b'P';
const PASSWORD_MESSAGE_TAG: u8 = b'p';
const QUERY_TAG: u8 = b'Q';
const SYNC_TAG: u8 = b'S';
const TERMINATE_TAG: u8 = b'X';

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
        let mut idx = 0;

        // read until the next header
        while let Some(header) = Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;

            if src[idx..].len() < len {
                break;
            }

            idx += len
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(src.split_to(idx)))
        }
    }
}

/// Postgres message prefix of a message tag and content length
pub struct Header {
    len: i32,
}

#[allow(clippy::len_without_is_empty)]
impl Header {
    #[inline]
    pub fn parse(buf: &[u8]) -> io::Result<Option<Header>> {
        if buf.len() < 5 {
            return Ok(None);
        }

        let _tag = buf[0];
        let len = BigEndian::read_i32(&buf[1..]);

        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length: header length < 4",
            ));
        }

        Ok(Some(Header { len }))
    }

    #[inline]
    pub fn len(self) -> i32 {
        self.len
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
    /// Parse a single frame of known and established length
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

/// In-session Postgres frontend message variants
#[non_exhaustive]
pub enum Message {
    Bind(BindBody),
    CancelRequest,
    Close(CloseBody),
    CopyData(CopyDataBody),
    CopyDone,
    CopyFail(CopyFailBody),
    Describe(DescribeBody),
    Execute(ExecuteBody),
    Flush,
    FunctionCall(FunctionCallBody),
    Parse(ParseBody),
    PasswordMessage(PasswordMessageBody),
    Query(QueryBody),
    Sync,
    Terminate,
}

impl Message {
    #[inline]
    pub fn parse(buf: &mut BytesMut) -> io::Result<Option<Message>> {
        if buf.len() < 5 {
            let to_read = 5 - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let tag = buf[0];
        let len = (&buf[1..5]).read_u32::<BigEndian>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: reading u32",
            )
        })?;

        let total_len = len as usize + 1;
        if buf.len() < total_len {
            let to_read = total_len - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let mut buf = Buffer {
            bytes: buf.split_to(total_len).freeze(),
            idx: 5,
        };

        let message = match tag {
            BIND_TAG => {
                let portal = buf.read_cstr()?;
                let source = buf.read_cstr()?;
                let storage = buf.read_all();
                Message::Bind(BindBody {
                    portal,
                    source,
                    storage,
                })
            }
            CLOSE_TAG => {
                let target = buf.read_u8()?;
                let name = buf.read_cstr()?;

                Message::Close(CloseBody { target, name })
            }
            COPY_DATA_TAG => {
                let storage = buf.read_all();

                Message::CopyData(CopyDataBody { storage })
            }
            COPY_DONE_TAG => Message::CopyDone,
            COPY_FAIL_TAG => {
                let message = buf.read_cstr()?;

                Message::CopyFail(CopyFailBody { message })
            }
            DESCRIBE_TAG => {
                let target = buf.read_u8()?;
                let name = buf.read_cstr()?;

                Message::Describe(DescribeBody { target, name })
            }
            EXECUTE_TAG => {
                let name = buf.read_cstr()?;
                let max_rows = buf.read_i32::<BigEndian>()?;

                Message::Execute(ExecuteBody { name, max_rows })
            }
            FLUSH_TAG => Message::Flush,
            FUNCTION_CALL_TAG => {
                let oid = buf.read_i32::<BigEndian>()?;
                let storage = buf.read_all();

                Message::FunctionCall(FunctionCallBody { oid, storage })
            }
            PARSE_TAG => {
                let name = buf.read_cstr()?;
                let query = buf.read_cstr()?;
                let storage = buf.read_all();

                Message::Parse(ParseBody {
                    name,
                    query,
                    storage,
                })
            }
            PASSWORD_MESSAGE_TAG => {
                let password = buf.read_cstr()?;

                Message::PasswordMessage(PasswordMessageBody { password })
            }
            QUERY_TAG => {
                let query = buf.read_cstr()?;

                Message::Query(QueryBody { query })
            }
            SYNC_TAG => Message::Sync,
            TERMINATE_TAG => Message::Terminate,
            _ => return Ok(None),
        };

        Ok(Some(message))
    }
}

/// Body types for messages with payloads
pub struct BindBody {
    portal: Bytes,
    source: Bytes,
    storage: Bytes,
}

pub struct CloseBody {
    target: u8,
    name: Bytes,
}

pub struct CopyDataBody {
    storage: Bytes,
}

pub struct CopyFailBody {
    message: Bytes,
}

pub struct DescribeBody {
    target: u8,
    name: Bytes,
}

pub struct ExecuteBody {
    name: Bytes,
    max_rows: i32,
}

pub struct FunctionCallBody {
    oid: i32,
    storage: Bytes,
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
    pub fn password(self, salt: [u8; 4]) -> Bytes {
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

    #[inline]
    fn read_all(&mut self) -> Bytes {
        let buf = self.bytes.slice(self.idx..);
        self.idx = self.bytes.len();
        buf
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
