use tokio_postgres::{
    types::{to_sql_checked, IsNull, ToSql, Type},
    RowStream,
};

mod role_pool;

pub use role_pool::{Error, RolePool};

/// Connection behavior across connection types
#[tonic::async_trait]
pub trait Connection {
    type Error;

    /// Run a query parameterized by proto-JSON Values, returning a stream of rows
    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<RowStream, Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations
#[tonic::async_trait]
pub trait Pool<K = String> {
    type Connection: Connection;
    type Error;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: K) -> Result<Self::Connection, Self::Error>;
}

/// Accepted parameter types from JSON
#[derive(Debug)]
pub enum Parameter<'a> {
    Null,
    Boolean(bool),
    Number(f64),
    Text(&'a str),
}

/// Binary encoding for Parameters
impl<'a> ToSql for Parameter<'a> {
    fn to_sql(
        &self,
        type_: &Type,
        out: &mut prost::bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Boolean(boolean) => boolean.to_sql(type_, out),
            Self::Text(text) => text.to_sql(type_, out),
            Self::Number(number) => match type_ {
                &Type::INT2 => (*number as i16).to_sql(type_, out),
                &Type::INT4 => (*number as i32).to_sql(type_, out),
                &Type::INT8 => (*number as i64).to_sql(type_, out),
                &Type::FLOAT4 => (*number as f32).to_sql(type_, out),
                &Type::FLOAT8 => (*number as f64).to_sql(type_, out),
                // FIXME: use a connection strategy that leverages built-in type inference
                _ => Err(format!("Cannot encode number as type {}", type_).into()),
            },
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}
