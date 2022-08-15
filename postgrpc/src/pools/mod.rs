use bytes::{BufMut, BytesMut};
use futures_util::TryStream;
use pbjson_types::{value::Kind, ListValue, Struct};
use std::fmt;
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};
use tonic::{async_trait, Request, Status};

#[cfg(feature = "deadpool")]
pub mod deadpool;
#[cfg(feature = "shared_connection_pool")]
pub mod shared;
#[cfg(feature = "transaction")]
pub mod transaction;

/// Newtype wrapper around dynamically-typed, JSON-compatible protobuf values
#[derive(Debug, Clone)]
pub struct Parameter(pbjson_types::Value);

/// Binary encoding for Parameters
impl ToSql for Parameter {
    fn to_sql(
        &self,
        type_: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        // handle JSON conversions for all non-ARRAY types first
        if matches!(type_, &Type::JSON | &Type::JSONB) {
            return serde_json::to_value(&self.0)?.to_sql(type_, out);
        }

        match self.encode_format(type_) {
            // handle non-JSON binary encoding
            Format::Binary => to_sql_binary(&self.0.kind, type_, out),
            // handle inferred/text encoding
            Format::Text => to_sql_text(&self.0.kind, type_, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, type_: &Type) -> Format {
        // decide if inputs should be inferred based on combination of input and db types
        if should_infer(&self.0.kind, type_) {
            Format::Text
        } else {
            Format::Binary
        }
    }

    to_sql_checked!();
}

impl From<pbjson_types::Value> for Parameter {
    fn from(value: pbjson_types::Value) -> Self {
        Self(value)
    }
}

/// Encode a proto Kind into binary-encoded SQL
fn to_sql_binary(
    kind: &Option<Kind>,
    type_: &Type,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
    match kind {
        Some(Kind::NullValue(..)) | None => Ok(IsNull::Yes),
        Some(Kind::BoolValue(boolean)) => match *type_ {
            Type::BOOL => boolean.to_sql(type_, out),
            _ => Err(format!("Cannot encode boolean as type {type_}").into()),
        },
        Some(Kind::StringValue(text)) => match *type_ {
            Type::TEXT | Type::VARCHAR => text.to_sql(type_, out),
            _ => Err(format!("Cannot encode text '{text}' as type {type_}").into()),
        },
        Some(Kind::NumberValue(number)) => match *type_ {
            // FIXME: avoid "as" conversions/overflow
            // FIXME: double-check f64 conversions for the largest numbers versus JSON Number::MAX
            Type::OID => (*number as u32).to_sql(type_, out),
            Type::INT2 => (*number as i16).to_sql(type_, out),
            Type::INT4 => (*number as i32).to_sql(type_, out),
            Type::INT8 => (*number as i64).to_sql(type_, out),
            Type::FLOAT4 => (*number as f32).to_sql(type_, out),
            Type::FLOAT8 => number.to_sql(type_, out),
            _ => Err(format!("Cannot encode {number} as type {type_}").into()),
        },
        Some(Kind::ListValue(ListValue { values })) => match type_.kind() {
            tokio_postgres::types::Kind::Array(..) => {
                // FIXME: handle ranges (?) and tuples as list pairs
                // FIXME: check that this path is taken correctly for binary-encoded arrays
                // FIXME: handle multi-level ARRAY types (even if just preventing panics!)
                values
                    .to_owned()
                    .into_iter()
                    .map(Parameter::from)
                    .collect::<Vec<_>>()
                    .to_sql(type_, out)
            }
            _ => Err(format!(
                "Cannot encode {} as an array of type {type_}",
                serde_json::to_value(values)?
            )
            .into()),
        },
        Some(Kind::StructValue(Struct { fields })) => match type_.kind() {
            tokio_postgres::types::Kind::Composite(composite_fields) => {
                // implementation taken directly from https://docs.rs/postgres-derive/latest/src/postgres_derive/tosql.rs.html#142
                out.extend_from_slice(&(composite_fields.len() as i32).to_be_bytes());

                for field in composite_fields {
                    out.extend_from_slice(&field.type_().oid().to_be_bytes());

                    let base = out.len();
                    out.extend_from_slice(&[0; 4]);

                    let name = field.name();

                    let parameter =
                        fields
                            .get(name)
                            .cloned()
                            .map(Parameter::from)
                            .ok_or(format!(
                                "Field '{name}' of composite type '{type_}' missing from {}",
                                serde_json::to_value(fields)?
                            ))?;

                    let result = parameter.to_sql(field.type_(), out);

                    let count = match result? {
                        IsNull::Yes => -1,
                        IsNull::No => {
                            let len = out.len() - base - 4;
                            if len > i32::max_value() as usize {
                                return Result::Err(Into::into("value too large to transmit"));
                            }
                            len as i32
                        }
                    };

                    out[base..base + 4].copy_from_slice(&count.to_be_bytes());
                }
                Ok(IsNull::No)
            }
            // FIXME: handle all non-JSON "struct-like" postgres types (including custom types!)
            _ => Err(format!(
                "Cannot encode struct {} as type {}",
                serde_json::to_value(fields)?,
                type_
            )
            .into()),
        },
    }
}

/// Encode a proto Kind into text-formatted SQL
fn to_sql_text(
    kind: &Option<Kind>,
    type_: &Type,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
    match kind {
        Some(Kind::NullValue(..)) | None => Ok(IsNull::Yes),
        Some(Kind::BoolValue(boolean)) => boolean.to_string().to_sql(type_, out),
        Some(Kind::StringValue(text)) => text.to_sql(type_, out),
        Some(Kind::NumberValue(number)) => number.to_string().to_sql(type_, out),
        Some(Kind::ListValue(ListValue { values })) => match type_.kind() {
            tokio_postgres::types::Kind::Array(..) => {
                // text-formatted arrays require manual construction using Postgres syntax
                let mut values = values.iter().peekable();
                out.put_slice(b"{");

                while let Some(value) = values.next() {
                    if let Some(Kind::NullValue(..)) | None = value.kind {
                        // handle "NULL" as a special case
                        out.put_slice(b"null");
                    } else {
                        // use recursive text formatting implementation for everything else
                        to_sql_text(&value.kind, type_, out)?;
                    }

                    if values.peek().is_some() {
                        out.put_slice(b",");
                    }
                }

                out.put_slice(b"}");

                Ok(IsNull::No)
            }
            _ => Err(format!(
                "Cannot encode {} as type {}",
                serde_json::to_value(values)?,
                type_
            )
            .into()),
        },
        Some(Kind::StructValue(Struct { fields })) => match type_.kind() {
            tokio_postgres::types::Kind::Composite(composite_fields) => {
                // text-formatted composite structs require row shorthand format
                let mut composite_fields = composite_fields.iter().peekable();
                out.put_slice(b"(");

                while let Some(field) = composite_fields.next() {
                    let name = field.name();

                    match fields.get(name) {
                        Some(value) => {
                            if let Some(Kind::NullValue(..)) | None = value.kind {
                                // handle "NULL" as a special case
                                out.put_slice(b"null");
                            } else {
                                // use recursive text formatting implementation for everything else
                                to_sql_text(&value.kind, field.type_(), out)?;
                            }

                            if composite_fields.peek().is_some() {
                                out.put_slice(b",");
                            }
                        }
                        None => {
                            return Err(format!(
                                "Field '{name}' of composite type '{type_}' missing from {}",
                                serde_json::to_value(fields)?
                            )
                            .into())
                        }
                    }
                }

                out.put_slice(b")");

                Ok(IsNull::No)
            }
            // FIXME: handle all non-JSON "struct-like" postgres types (including records, etc)
            _ => Err(format!(
                "Cannot encode struct {} as type {}",
                serde_json::to_value(fields)?,
                type_
            )
            .into()),
        },
    }
}

/// Decide if a parameter should be inferred or not based on the type given by statement prep
fn should_infer(kind: &Option<Kind>, type_: &Type) -> bool {
    // never infer JSON types, regardless of inputs
    if matches!(
        type_,
        &Type::JSON | &Type::JSONB | &Type::JSON_ARRAY | &Type::JSONB_ARRAY
    ) {
        return false;
    }

    match kind {
        // never infer null or boolean inputs on their own
        Some(Kind::NullValue(..) | Kind::BoolValue(..)) | None => false,
        // infer all stringly types besides those that are unambiguously textual
        Some(Kind::StringValue(..)) => !matches!(*type_, Type::TEXT | Type::VARCHAR),
        // infer all numeric types besides those that are unambiguously numeric
        Some(Kind::NumberValue(..)) => !matches!(
            *type_,
            Type::OID | Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT4 | Type::FLOAT8
        ),
        // valid lists are all of the same type
        // if any list element should be inferred, then they all should be inferred
        Some(Kind::ListValue(ListValue { values })) => match type_.kind() {
            tokio_postgres::types::Kind::Array(type_) => {
                values.iter().any(|value| should_infer(&value.kind, type_))
            }
            _ => false,
        },
        // if any of the struct fields should be inferred,
        // then the entire struct should be inferred
        Some(Kind::StructValue(Struct { fields })) => match type_.kind() {
            tokio_postgres::types::Kind::Composite(composite_fields) => composite_fields
                .iter()
                .any(|field| match fields.get(field.name()) {
                    Some(value) => should_infer(&value.kind, field.type_()),
                    None => false,
                }),
            _ => false,
        },
    }
}

/// gRPC-compatible connection behavior across database connection types. All inputs and outputs
/// are based on prost well-known-types like `Struct` and `Value`
#[async_trait]
pub trait Connection: Send + Sync {
    /// A fallible stream of rows returned from the database as protobuf structs
    type RowStream: TryStream<Ok = pbjson_types::Struct, Error = Self::Error> + Send + Sync;

    /// Error type on the connection encompassing top-level errors (i.e. "bad connection") and
    /// errors within a RowStream
    type Error: std::error::Error + Into<Status> + Send + Sync;

    /// Run a query parameterized by the Connection's associated Parameter, returning a RowStream
    async fn query(
        &self,
        statement: &str,
        parameters: &[Parameter],
    ) -> Result<Self::RowStream, Self::Error>;

    /// Run a set of SQL statements using the simple query protocol
    async fn batch(&self, query: &str) -> Result<(), Self::Error>;
}

/// Connection pool behavior that can be customized across async pool implementations
///
/// ## Example:
///
/// ```
/// use postgrpc::pools::{Pool, Connection};
/// use std::collections::BTreeMap;
/// use tokio::sync::RwLock;
/// use uuid::Uuid;
/// use tonic::Status;
///
/// // a simple connection wrapper
/// // (implementing postgrpc::Connection is an exercise for the reader)
/// #[derive(Clone)]
/// struct MyConnection(Arc<tokio_postgres::Client>);
///
/// // a toy pool wrapping a collection of tokio_postgres::Clients
/// // accessible by unique IDs that are provided by the caller
/// struct MyPool {
///     connections: RwLock<BTreeMap<Uuid, MyConnection>>,
///     config: tokio_postgres::config::Config
/// }
///
/// #[postgrpc::async_trait]
/// impl Pool for MyPool {
///     type Key: Uuid;
///     type Connection: MyConnection;
///     type Error: Status;
///
///     async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error> {
///         // get a connection from the pool or store one for later
///         let connections = self.connections.read().await;
///
///         match connections.get(&key) {
///             Some(connection) => Ok(Arc::clone(connection.0)),
///             None => {
///                 // drop the previous lock on the connections
///                 drop(connections);
///
///                 // connect to the database using the configuration
///                 let (client, connection) =
///                 self.config.connect(tokio_postgres::NoTls).map_error(|error| Status::internal(error.to_string())?;
///                 
///                 // spawn the raw connection off onto an executor
///                 tokio::spawn(async move {
///                     if let Err(error) = connection.await {
///                         eprintln!("connection error: {}", error);
///                     }
///                 });
///
///                 // store a reference to the connection for later
///                 let connection = MyConnection(Arc::new(client));
///                 self.connections.write().await.insert(key, connection);
///
///                 // return another reference to the connection for use
///                 Ok(connection)
///             }
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Pool: Send + Sync {
    /// The key by which connections are selected from the Pool, allowing for custom
    /// connection-fetching logic in Pool implementations
    type Key: fmt::Debug + Send + Sync;

    /// The underlying connection type returned from the Pool
    type Connection: Connection;

    /// Errors related to fetching Connections from the Pool
    type Error: std::error::Error
        + From<<Self::Connection as Connection>::Error>
        + Into<Status>
        + Send
        + Sync;

    /// Get a single connection from the pool using some key
    async fn get_connection(&self, key: Self::Key) -> Result<Self::Connection, Self::Error>;
}

/// Helper trait to encapsulate logic for deriving values from gRPC requests
pub trait FromRequest
where
    Self: Sized,
{
    /// Errors associated with deriving a value from a gRPC response
    type Error: std::error::Error + Into<Status>;

    /// Derive a value from a gRPC request
    fn from_request<T>(request: &mut Request<T>) -> Result<Self, Self::Error>;
}

/// Dummy error for default impl of derivation of unit structs from requests
#[derive(Debug)]
pub struct UnitConversion;

impl fmt::Display for UnitConversion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for UnitConversion {}

impl From<UnitConversion> for Status {
    fn from(_: UnitConversion) -> Self {
        Self::internal("Infallible unit conversion somehow failed")
    }
}

impl FromRequest for () {
    type Error = UnitConversion;

    fn from_request<T>(_: &mut Request<T>) -> Result<Self, Self::Error> {
        Ok(())
    }
}
