#![allow(clippy::enum_variant_names)]

// TODO: contribute more comprehensive frontend message parsing work to postgres_protocol
use super::{backend, buffer::Buffer};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Byte tags for relevant frontend message variants
const BIND_TAG: u8 = b'B';
const CLOSE_TAG: u8 = b'C';
const DESCRIBE_TAG: u8 = b'D';
const EXECUTE_TAG: u8 = b'E';
const PASSWORD_MESSAGE_TAG: u8 = b'p';
const PARSE_TAG: u8 = b'F';
const QUERY_TAG: u8 = b'Q';
const TERMINATE_TAG: u8 = b'X';

/// Post-startup Postgres frontend message variants that Postrust cares about
#[derive(Debug, Clone)]
pub enum Message {
    // TODO:
    // CancelRequest
    // CopyData
    // CopyDone
    // CopyFail
    // Flush
    // FunctionCall
    // Sync
    SASLInitialResponse(SASLInitialResponseBody),
    SASLResponse(SASLResponseBody),
    Bind(BindBody),
    Close(CloseBody),
    Describe(DescribeBody),
    Execute(ExecuteBody),
    Parse(ParseBody),
    PasswordMessage(PasswordMessageBody),
    Query(QueryBody),
    Terminate,
    // catchall for frames we don't care about (usually an error)
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
            BIND_TAG => {
                let portal = buf.read_cstr()?;
                let statement = buf.read_cstr()?;
                let parameter_format_code_length: usize =
                    buf.read_i16::<BigEndian>()?.try_into().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid parameter format code length format: reading i16",
                        )
                    })?;

                let mut parameter_format_codes = Vec::with_capacity(parameter_format_code_length);

                for _ in 0..parameter_format_code_length {
                    let format_code = buf.read_i16::<BigEndian>()?;
                    parameter_format_codes.push(format_code);
                }

                let parameter_count: usize =
                    buf.read_i16::<BigEndian>()?.try_into().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid parameter count format: reading i16",
                        )
                    })?;

                let mut parameters = Vec::with_capacity(parameter_count);

                for _ in 0..parameter_count {
                    let parameter_length =
                        buf.read_i16::<BigEndian>()?.try_into().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "invalid parameter length format: reading i16",
                            )
                        })?;

                    let mut parameter = Vec::with_capacity(parameter_length);
                    unsafe { parameter.set_len(parameter_length) };
                    std::io::Read::read(&mut buf, &mut parameter)?;
                    parameters.push(parameter.into());
                }

                let column_format_code_length: usize =
                    buf.read_i16::<BigEndian>()?.try_into().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid column format code length format: reading i16",
                        )
                    })?;

                let mut column_format_codes = Vec::with_capacity(column_format_code_length);

                for _ in 0..column_format_code_length {
                    let format_code = buf.read_i16::<BigEndian>()?;
                    column_format_codes.push(format_code);
                }

                Message::Bind(BindBody {
                    portal,
                    statement,
                    parameter_format_codes,
                    parameters,
                    column_format_codes,
                })
            }
            CLOSE_TAG => {
                let variant = buf.read_u8()?;
                let name = buf.read_cstr()?;

                let body = match variant {
                    b'P' => CloseBody::Portal { name },
                    b'S' => CloseBody::Statement { name },
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid close format: only statement or portal names supported",
                        ));
                    }
                };

                Message::Close(body)
            }
            DESCRIBE_TAG => {
                let variant = buf.read_u8()?;
                let name = buf.read_cstr()?;

                let body = match variant {
                    b'P' => DescribeBody::Portal { name },
                    b'S' => DescribeBody::Statement { name },
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid describe format: only statement or portal names supported",
                        ));
                    }
                };

                Message::Describe(body)
            }
            EXECUTE_TAG => {
                let portal = buf.read_cstr()?;
                let max_rows = buf.read_i32::<BigEndian>()?;

                Message::Execute(ExecuteBody { portal, max_rows })
            }
            PARSE_TAG => {
                let name = buf.read_cstr()?;
                let query = buf.read_cstr()?;
                let parameter_length: usize =
                    buf.read_i32::<BigEndian>()?.try_into().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid parameter length format: reading i32",
                        )
                    })?;

                let mut parameter_types = Vec::with_capacity(parameter_length);

                for _ in 0..parameter_length {
                    let type_ = buf.read_i32::<BigEndian>()?;
                    parameter_types.push(type_);
                }

                Message::Parse(ParseBody {
                    name,
                    query,
                    parameter_types,
                })
            }
            QUERY_TAG => {
                let query = buf.read_cstr()?;

                Message::Query(QueryBody { query })
            }
            TERMINATE_TAG => Message::Terminate,
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
            Self::Bind(BindBody {
                portal,
                statement,
                parameter_format_codes,
                parameters,
                column_format_codes,
            }) => {
                let parameter_format_count = parameter_format_codes.len() as i32;
                let column_format_count = column_format_codes.len() as i32;
                let parameter_count = parameters.len() as i32;
                let parameter_length = parameters.iter().fold(0, |mut length, parameter| {
                    length += 1;
                    length += parameter.len();
                    length
                }) as i32;

                bytes.put_u8(BIND_TAG);
                bytes.put_i32(
                    4 + (portal.len() as i32)
                        + 1
                        + (statement.len() as i32)
                        + 1
                        + 1
                        + parameter_format_count
                        + 1
                        + parameter_length
                        + 1
                        + column_format_count,
                );
                bytes.put_slice(&portal);
                bytes.put_u8(0);
                bytes.put_slice(&statement);
                bytes.put_u8(0);
                bytes.put_i16(parameter_format_count as i16);

                for format_code in parameter_format_codes {
                    bytes.put_i16(format_code);
                }

                bytes.put_i16(parameter_count as i16);

                for parameter in parameters {
                    bytes.put_i32(parameter.len() as i32);
                    bytes.put_slice(&parameter);
                }

                bytes.put_i16(column_format_count as i16);

                for format_code in column_format_codes {
                    bytes.put_i16(format_code);
                }
            }
            Self::Close(body) => {
                bytes.put_u8(CLOSE_TAG);

                let (name, variant) = match body {
                    CloseBody::Portal { name } => (name, b'P'),
                    CloseBody::Statement { name } => (name, b'S'),
                };

                bytes.put_i32(4 + 4 + (name.len() as i32) + 1);
                bytes.put_u8(variant);
                bytes.put_slice(&name);
                bytes.put_u8(0);
            }
            Self::Describe(body) => {
                bytes.put_u8(DESCRIBE_TAG);

                let (name, variant) = match body {
                    DescribeBody::Portal { name } => (name, b'P'),
                    DescribeBody::Statement { name } => (name, b'S'),
                };

                bytes.put_i32(4 + 4 + (name.len() as i32) + 1);
                bytes.put_u8(variant);
                bytes.put_slice(&name);
                bytes.put_u8(0);
            }
            Self::Execute(ExecuteBody { portal, max_rows }) => {
                bytes.put_u8(EXECUTE_TAG);
                bytes.put_i32(4 + (portal.len() as i32) + 1 + 1);
                bytes.put_slice(&portal);
                bytes.put_u8(0);
                bytes.put_i32(max_rows);
            }
            Self::Parse(ParseBody {
                name,
                query,
                parameter_types,
            }) => {
                let parameter_count = parameter_types.len() as i32;

                bytes.put_u8(PARSE_TAG);
                bytes.put_i32(
                    4 + (name.len() as i32) + 1 + (query.len() as i32) + 1 + 1 + parameter_count,
                );
                bytes.put_slice(&name);
                bytes.put_u8(0);
                bytes.put_slice(&query);
                bytes.put_u8(0);
                bytes.put_i32(parameter_count);

                for type_ in parameter_types {
                    bytes.put_i32(type_);
                }
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
            Self::Terminate => {
                bytes.put_u8(TERMINATE_TAG);
                bytes.put_i32(4);
            }
        }
    }
}

/// Body types for messages with payloads
#[derive(Debug, Clone)]
pub struct SASLInitialResponseBody {
    pub mechanism: Bytes,
    pub initial_response: Bytes,
}

#[derive(Debug, Clone)]
pub struct SASLResponseBody {
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub struct BindBody {
    portal: Bytes,
    statement: Bytes,
    parameter_format_codes: Vec<i16>,
    parameters: Vec<Bytes>,
    column_format_codes: Vec<i16>,
}

impl From<BindBody> for Message {
    fn from(body: BindBody) -> Self {
        Self::Bind(body)
    }
}

#[derive(Debug, Clone)]
pub enum CloseBody {
    Portal { name: Bytes },
    Statement { name: Bytes },
}

impl From<CloseBody> for Message {
    fn from(body: CloseBody) -> Self {
        Self::Close(body)
    }
}

#[derive(Debug, Clone)]
pub struct ExecuteBody {
    portal: Bytes,
    max_rows: i32,
}

impl From<ExecuteBody> for Message {
    fn from(body: ExecuteBody) -> Self {
        Self::Execute(body)
    }
}

#[derive(Debug, Clone)]
pub enum DescribeBody {
    Portal { name: Bytes },
    Statement { name: Bytes },
}

impl From<DescribeBody> for Message {
    fn from(body: DescribeBody) -> Self {
        Self::Describe(body)
    }
}

#[derive(Debug, Clone)]
pub struct ParseBody {
    name: Bytes,
    query: Bytes,
    parameter_types: Vec<i32>,
}

impl ParseBody {
    #[inline]
    pub fn query(&self) -> Bytes {
        self.query.slice(..)
    }
}

impl From<ParseBody> for Message {
    fn from(body: ParseBody) -> Self {
        Self::Parse(body)
    }
}

#[derive(Debug, Clone)]
pub struct PasswordMessageBody {
    password: Bytes,
}

impl PasswordMessageBody {
    pub fn cleartext_password(self) -> Bytes {
        self.password.slice(..)
    }
}

#[derive(Debug, Clone)]
pub struct QueryBody {
    query: Bytes,
}

impl QueryBody {
    #[inline]
    pub fn query(&self) -> Bytes {
        self.query.slice(..)
    }
}

impl From<QueryBody> for Message {
    fn from(body: QueryBody) -> Self {
        Self::Query(body)
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
