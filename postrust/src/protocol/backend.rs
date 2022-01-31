// TODO: contribute more comprehensive backend message serialization work to postgres_protocol
use super::{buffer::Buffer, frontend};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Byte tags for relevant backend message variants and fields
const AUTHENTICATION_TAG: u8 = b'R';
const ERROR_RESPONSE_TAG: u8 = b'E';
const ERROR_SEVERITY_TAG: u8 = b'S';
const ERROR_MESSAGE_TAG: u8 = b'M';
const ERROR_CODE_TAG: u8 = b'C';
const READY_FOR_QUERY_TAG: u8 = b'Z';
const SSL_NOT_SUPPORTED_TAG: u8 = b'N';

/// Postgres backend message variants that Postrust cares about
// FIXME: align with frontend message format style
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    AuthenticationMd5Password {
        salt: [u8; 4],
    },
    AuthenticationSASL {
        mechanisms: Vec<Bytes>,
    },
    AuthenticationSASLContinue {
        data: Bytes,
    },
    AuthenticationSASLFinal {
        data: Bytes,
    },
    AuthenenticationCleartextPassword,
    AuthenticationOk,
    ErrorResponse {
        severity: Severity,
        message: Bytes,
        code: Bytes,
    },
    ReadyForQuery {
        transaction_status: TransactionStatus,
    },
    SslResponse,
    Forward(Bytes), // handles frames we don't care about
}

impl Message {
    /// Parse a single message frame of known and established length
    #[inline]
    pub fn parse(buf: &mut BytesMut) -> io::Result<Option<Message>> {
        if buf.len() < 5 {
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

        if total_len > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: invalid length",
            ));
        }

        let mut buf = Buffer {
            bytes: buf.split_to(total_len).freeze(),
            idx: 5,
        };

        let message = match tag {
            AUTHENTICATION_TAG => match buf.read_i32::<BigEndian>()? {
                0 => Message::AuthenticationOk,
                5 => {
                    let salt = [
                        buf.read_u8()?,
                        buf.read_u8()?,
                        buf.read_u8()?,
                        buf.read_u8()?,
                    ];

                    Message::AuthenticationMd5Password { salt }
                }
                10 => {
                    let mut mechanisms = vec![];

                    while let Ok(mechanism) = buf.read_cstr() {
                        if !mechanism.is_empty() {
                            mechanisms.push(mechanism);
                        }
                    }

                    Message::AuthenticationSASL { mechanisms }
                }
                11 => {
                    let data = buf.read_all();

                    Message::AuthenticationSASLContinue { data }
                }
                12 => {
                    let data = buf.read_all();

                    Message::AuthenticationSASLFinal { data }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid response: authentication scheme not supported",
                    ))
                }
            },
            ERROR_RESPONSE_TAG => {
                let mut message = ErrorResponseBuilder::default();

                loop {
                    match buf.read_u8()? {
                        0 => break,
                        field_type => {
                            let field = buf.read_cstr()?;

                            // FIXME: support more error fields
                            match field_type {
                                ERROR_SEVERITY_TAG => {
                                    let severity = match field.as_ref() {
                                        b"WARNING" => Severity::Warning,
                                        b"NOTICE" => Severity::Notice,
                                        b"DEBUG" => Severity::Debug,
                                        b"ERROR" => Severity::Error,
                                        b"FATAL" => Severity::Fatal,
                                        b"PANIC" => Severity::Panic,
                                        b"INFO" => Severity::Info,
                                        b"LOG" => Severity::Log,
                                        _ => {
                                            return Err(io::Error::new(
                                                io::ErrorKind::InvalidInput,
                                                "invalid response: invalid error severity",
                                            ))
                                        }
                                    };
                                    message.severity = Some(severity);
                                }
                                ERROR_CODE_TAG => {
                                    message.code = Some(field);
                                }
                                ERROR_MESSAGE_TAG => {
                                    message.message = Some(field);
                                }
                                _ => (),
                            }
                        }
                    }
                }

                message.finish()?
            }
            READY_FOR_QUERY_TAG => {
                let transaction_status = match buf.read_u8()? {
                    b'I' => TransactionStatus::Idle,
                    b'T' => TransactionStatus::Transaction,
                    b'E' => TransactionStatus::Error,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid response: invalid transaction status",
                        ))
                    }
                };

                Message::ReadyForQuery { transaction_status }
            }
            _ => Message::Forward(buf.bytes),
        };

        Ok(Some(message))
    }

    /// Write a backend message to bytes
    #[inline]
    pub fn write<B>(self, bytes: &mut B)
    where
        B: BufMut + std::fmt::Debug,
    {
        match self {
            Self::AuthenticationMd5Password { salt } => {
                bytes.put_u8(AUTHENTICATION_TAG);
                bytes.put_i32(12);
                bytes.put_i32(5);
                bytes.put_slice(&salt);
            }
            Self::AuthenenticationCleartextPassword => {
                bytes.put_u8(AUTHENTICATION_TAG);
                bytes.put_i32(8);
                bytes.put_i32(3);
            }
            Self::AuthenticationSASL { mechanisms } => {
                let mechanisms_length = mechanisms.iter().fold(0, |mut length, mechanism| {
                    length += mechanism.len();
                    length += 1;
                    length
                }) as i32;

                bytes.put_u8(AUTHENTICATION_TAG);
                bytes.put_i32(4 + 3 + mechanisms_length + 1);
                bytes.put_i32(10);

                for mechanism in mechanisms {
                    bytes.put_slice(&mechanism);
                    bytes.put_u8(0);
                }

                bytes.put_u8(0);
            }
            Self::AuthenticationSASLContinue { data } => {
                bytes.put_u8(AUTHENTICATION_TAG);
                bytes.put_i32(4 + (data.len() as i32) + 4);
                bytes.put_i32(11);
                bytes.put_slice(&data);
            }
            Self::AuthenticationSASLFinal { data } => {
                bytes.put_u8(AUTHENTICATION_TAG);
                bytes.put_i32(4 + (data.len() as i32) + 4);
                bytes.put_i32(12);
                bytes.put_slice(&data);
            }
            Self::AuthenticationOk => {
                bytes.put_u8(AUTHENTICATION_TAG);
                bytes.put_i32(8);
                bytes.put_i32(0);
            }
            Self::ErrorResponse {
                severity,
                message,
                code,
            } => {
                let mut fields = BytesMut::new();
                fields.put_u8(ERROR_SEVERITY_TAG);
                severity.write(&mut fields);
                fields.put_u8(0);
                fields.put_u8(ERROR_CODE_TAG);
                fields.put_slice(&code);
                fields.put_u8(0);
                fields.put_u8(ERROR_MESSAGE_TAG);
                fields.put_slice(&message);
                fields.put_u8(0);
                fields.put_u8(0);
                let fields_length = fields.len() as i32;
                bytes.put_u8(ERROR_RESPONSE_TAG);
                bytes.put_i32(fields_length + 4);
                bytes.put_slice(&fields);
            }
            Self::ReadyForQuery { transaction_status } => {
                bytes.put_u8(READY_FOR_QUERY_TAG);
                bytes.put_i32(5);
                transaction_status.write(bytes);
            }
            Self::SslResponse => {
                // TODO: handle responses other than "not supported" here
                bytes.put_u8(SSL_NOT_SUPPORTED_TAG);
            }
            Self::Forward(frame) => {
                bytes.put_slice(&frame);
            }
        }
    }
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Severity {
    Warning,
    Notice,
    Debug,
    Error,
    Fatal,
    Panic,
    Info,
    Log,
}

impl Severity {
    fn write<B>(self, bytes: &mut B)
    where
        B: BufMut,
    {
        match self {
            Self::Warning => bytes.put_slice(b"WARNING"),
            Self::Notice => bytes.put_slice(b"NOTICE"),
            Self::Debug => bytes.put_slice(b"DEBUG"),
            Self::Error => bytes.put_slice(b"ERROR"),
            Self::Fatal => bytes.put_slice(b"FATAL"),
            Self::Panic => bytes.put_slice(b"PANIC"),
            Self::Info => bytes.put_slice(b"INFO"),
            Self::Log => bytes.put_slice(b"LOG"),
        }
    }
}

/// Possible transaction statuses for use in ReadyForQuery messages
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionStatus {
    Idle,
    Transaction,
    Error,
}

impl TransactionStatus {
    fn write<B>(self, bytes: &mut B)
    where
        B: BufMut,
    {
        match self {
            Self::Idle => bytes.put_u8(b'I'),
            Self::Transaction => bytes.put_u8(b'T'),
            Self::Error => bytes.put_u8(b'E'),
        }
    }
}

/// Helper for building error responses from partial data
#[derive(Default)]
struct ErrorResponseBuilder {
    code: Option<Bytes>,
    message: Option<Bytes>,
    severity: Option<Severity>,
}

impl ErrorResponseBuilder {
    fn finish(self) -> io::Result<Message> {
        let code = self.code.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: ErrorResponse missing required field 'code'",
            )
        })?;

        let message = self.message.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: ErrorResponse missing required field 'message'",
            )
        })?;

        let severity = self.severity.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message format: ErrorResponse missing required field 'severity'",
            )
        })?;

        Ok(Message::ErrorResponse {
            code,
            message,
            severity,
        })
    }
}

/// Codec for handling frontend messages to and from TCP streams
pub struct Codec;

impl Encoder<Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst);

        Ok(())
    }
}

impl Encoder<frontend::Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: frontend::Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write(dst);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        tracing::trace!(src = ?&src, "Decoding backend message frame");

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

#[cfg(test)]
mod test {
    use super::{Message, Severity, TransactionStatus};
    use crate::protocol::errors::{INVALID_TRANSACTION_STATE, TOO_MANY_CONNECTIONS};
    use bytes::{Bytes, BytesMut};

    // mock message payloads for decoding
    static FORWARDED_MESSAGE: Bytes = Bytes::from_static(
        b"T\0\0\0!\0\x01?column?\0\0\0\0\0\0\0\0\0\0\x19\xff\xff\xff\xff\xff\xff\0\0",
    );
    static AUTHENTICATION_MESSAGES: Bytes = Bytes::from_static(
        b"R\0\0\06\0\0\0\x0cv=veUyecS5U3NVXYF2igQ6J6sEKuOqXdeVJg5qoV6oBu0=R\0\0\0\x08\0\0\0\0",
    );
    static READY_FOR_QUERY_MESSAGE: Bytes = Bytes::from_static(b"Z\0\0\0\x05I");
    static ERROR_RESPONSE_MESSAGE: Bytes = Bytes::from_static(
        b"E\0\0\0USFATAL\0VFATAL\0C53300\0Msorry, too many clients already\0Fproc.c\0L359\0RInitProcess\0\0"
    );

    #[test]
    fn ignores_empty_bytes() {
        let mut input = BytesMut::new();
        input.extend_from_slice(b"");
        let result = Message::parse(&mut input).expect("Error parsing empty message");

        assert!(result.is_none(), "Failed to skip empty bytes");
    }

    #[test]
    fn does_not_parse_arbitrary_bytes() {
        let mut input = BytesMut::new();
        input.extend_from_slice(b"testing");
        let result = Message::parse(&mut input);

        assert!(result.is_err(), "Incorrectly parsed arbitrary bytes");
    }

    #[test]
    fn parses_valid_unhandled_messages() {
        let mut input = BytesMut::new();
        input.extend_from_slice(&FORWARDED_MESSAGE);
        let result = Message::parse(&mut input)
            .expect("Error parsing valid message")
            .expect("Error parsing complete message chunk");

        assert_eq!(
            result,
            Message::Forward(FORWARDED_MESSAGE.clone()),
            "Incorrectly parsed valid forwarded message"
        );
    }

    #[test]
    fn writes_arbitrary_forwarded_bytes() {
        let payload = Bytes::from_static(b"testing");
        let mut output = BytesMut::new();
        Message::Forward(payload.clone()).write(&mut output);

        assert_eq!(
            Bytes::from(output),
            payload,
            "Incorrectly wrote arbitrary bytes through Message::Forward"
        )
    }

    #[test]
    fn parses_authentication_messages() {
        let mut input = BytesMut::new();
        input.extend_from_slice(&AUTHENTICATION_MESSAGES);
        let result = Message::parse(&mut input)
            .expect("Error parsing valid message")
            .expect("Error parsing complete message chunk");

        if !matches!(result, Message::AuthenticationSASLFinal { .. }) {
            panic!("Incorrectly parsed valid AuthenticationSASLFinal message");
        }

        let result = Message::parse(&mut input)
            .expect("Error parsing valid message")
            .expect("Error parsing complete message chunk");

        assert_eq!(
            result,
            Message::AuthenticationOk,
            "Incorrectly parsed valid AuthenticationOk message"
        );
    }

    #[test]
    fn parses_ready_for_query_messages() {
        let mut input = BytesMut::new();
        input.extend_from_slice(&READY_FOR_QUERY_MESSAGE);
        let result = Message::parse(&mut input)
            .expect("Error parsing valid message")
            .expect("Error parsing complete message chunk");

        assert_eq!(
            result,
            Message::ReadyForQuery {
                transaction_status: TransactionStatus::Idle
            },
            "Incorrectly parsed valid ReadyForQuery message"
        );
    }

    #[test]
    #[tracing_test::traced_test]
    fn parses_error_response_messages() {
        let mut input = BytesMut::new();
        input.extend_from_slice(&ERROR_RESPONSE_MESSAGE);
        let result = Message::parse(&mut input)
            .expect("Error parsing valid message")
            .expect("Error parsing complete message chunk");

        assert_eq!(
            result,
            Message::ErrorResponse {
                code: TOO_MANY_CONNECTIONS.clone(),
                message: "sorry, too many clients already".into(),
                severity: Severity::Fatal
            },
            "Incorrectly parsed valid ReadyForQuery message"
        );
    }

    #[test]
    fn writes_error_response_messages() {
        let mut output = BytesMut::new();

        Message::ErrorResponse {
            code: INVALID_TRANSACTION_STATE.clone(),
            message: "Something went wrong".into(),
            severity: Severity::Error,
        }
        .write(&mut output);

        assert_eq!(
            Bytes::from(output),
            Bytes::from_static(b"E\0\0\0)SERROR\0C25000\0MSomething went wrong\0\0"),
            "Incorrectly wrote valid ErrorResponse"
        );
    }
}
