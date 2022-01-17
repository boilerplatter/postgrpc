use bytes::Bytes;

pub static CONNECTION_DOES_NOT_EXIST: Bytes = Bytes::from_static(b"08003");
pub static CONNECTION_EXCEPTION: Bytes = Bytes::from_static(b"08000");
pub static DUPLICATE_PREPARED_STATEMENT: Bytes = Bytes::from_static(b"42P05");
pub static FEATURE_NOT_SUPPORTED: Bytes = Bytes::from_static(b"0A000");
pub static INVALID_PASSWORD: Bytes = Bytes::from_static(b"28P01");
pub static INVALID_SQL_STATEMENT_NAME: Bytes = Bytes::from_static(b"26000");
pub static INVALID_TRANSACTION_STATE: Bytes = Bytes::from_static(b"25000");
pub static INSUFFICIENT_PRIVILEGE: Bytes = Bytes::from_static(b"42501");
pub static PROTOCOL_VIOLATION: Bytes = Bytes::from_static(b"08P01");

#[cfg(test)]
pub static TOO_MANY_CONNECTIONS: Bytes = Bytes::from_static(b"53300");
