// TODO: contribute more comprehensive backend message serialization work to postgres_protocol
use bytes::{BufMut, Bytes, BytesMut};

/// Byte tags for relevant backend message variants and fields
const AUTHENTICATION_TAG: u8 = b'R';
const ERROR_RESPONSE_TAG: u8 = b'E';
const ERROR_SEVERITY_TAG: u8 = b'S';
const ERROR_MESSAGE_TAG: u8 = b'M';
const ERROR_CODE_TAG: u8 = b'C';
const READY_FOR_QUERY_TAG: u8 = b'Z';
const SSL_NOT_SUPPORTED_TAG: u8 = b'N';

/// Postgres backend message variants that Postrust needs to send to clients
pub enum Message {
    AuthenticationMd5Password {
        salt: [u8; 4],
    },
    AuthenticationOk,
    ErrorResponse {
        severity: Severity,
        message: Bytes,
        code: i32,
    },
    ReadyForQuery {
        transaction_status: TransactionStatus,
    },
    SslResponse,
}

impl Message {
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
                fields.put_slice(code.to_string().as_bytes());
                fields.put_u8(0);
                fields.put_u8(ERROR_MESSAGE_TAG);
                fields.put_slice(&message);
                fields.put_u8(0);
                fields.put_u8(0);
                let fields_length = fields.len() as i32;
                bytes.put_u8(ERROR_RESPONSE_TAG);
                bytes.put_i32(fields_length + 4);
                bytes.put_slice(&fields);

                tracing::debug!("{:?}", &bytes);
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
        }
    }
}

/// Error severity levels
#[allow(dead_code)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
}

impl Severity {
    fn write<B>(self, bytes: &mut B)
    where
        B: BufMut,
    {
        match self {
            Self::Error => bytes.put_slice(b"ERROR"),
            Self::Fatal => bytes.put_slice(b"FATAL"),
            Self::Panic => bytes.put_slice(b"PANIC"),
        }
    }
}

/// Possible transaction statuses for use in ReadyForQuery messages
#[allow(dead_code)]
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
