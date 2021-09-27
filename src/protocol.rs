use prost_types::value::Kind;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

/// Accepted parameter types from JSON
#[derive(Debug)]
pub enum Parameter<'a> {
    Null,
    Boolean(bool),
    Number(f64),
    Text(&'a str),
}

impl<'a> Parameter<'a> {
    /// convert proto JSON values to scalar parameters
    // TODO: consider relaxing the scalar constraint for specific cases
    // (e.g. ListValues of a single type and JSON/serializable composite types for StructValues)
    pub fn from_proto_value(value: &'a prost_types::Value) -> Option<Self> {
        match value.kind.as_ref() {
            Some(Kind::ListValue(..) | Kind::StructValue(..)) | None => None,
            Some(Kind::NullValue(..)) => Some(Parameter::Null),
            Some(Kind::BoolValue(boolean)) => Some(Parameter::Boolean(*boolean)),
            Some(Kind::NumberValue(number)) => Some(Parameter::Number(*number)),
            Some(Kind::StringValue(text)) => Some(Parameter::Text(text)),
        }
    }
}

/// Binary encoding for Parameters
// TODO: put behind a rust-postgres/type inference feature
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
                // ToSql should not be used for type-inferred parameters of format text
                _ => Err(format!("Cannot encode number as type {}", type_).into()),
            },
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// JSON-specific helpers converting between serde and prost structures
pub mod json {
    /// Convert a serde_json::Value into a prost_types::Value
    fn to_proto_value(json: serde_json::Value) -> prost_types::Value {
        let kind = match json {
            serde_json::Value::Null => prost_types::value::Kind::NullValue(0),
            serde_json::Value::Bool(boolean) => prost_types::value::Kind::BoolValue(boolean),
            serde_json::Value::Number(number) => match number.as_f64() {
                Some(number) => prost_types::value::Kind::NumberValue(number),
                None => prost_types::value::Kind::StringValue(number.to_string()),
            },
            serde_json::Value::String(string) => prost_types::value::Kind::StringValue(string),
            serde_json::Value::Array(array) => {
                prost_types::value::Kind::ListValue(prost_types::ListValue {
                    values: array.into_iter().map(to_proto_value).collect(),
                })
            }
            serde_json::Value::Object(map) => {
                prost_types::value::Kind::StructValue(to_proto_struct(map))
            }
        };

        prost_types::Value { kind: Some(kind) }
    }

    /// Convert a serde_json::Map into a prost_types::Struct
    pub fn to_proto_struct(map: serde_json::Map<String, serde_json::Value>) -> prost_types::Struct {
        prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(key, value)| (key, to_proto_value(value)))
                .collect(),
        }
    }
}
