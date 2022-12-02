use ::postgres::types::Type;
use prost_types::field_descriptor_proto::Type as FieldType;

/// Newtype wrapper around Postgres [`Type`] for equality-checking against Prost field descriptor types
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PostgresType<'a>(&'a Type);

impl<'a> From<&'a Type> for PostgresType<'a> {
    fn from(type_: &'a Type) -> Self {
        Self(type_)
    }
}

impl<'a> PartialEq<FieldType> for PostgresType<'a> {
    fn eq(&self, field: &FieldType) -> bool {
        let postgres_type = self.0;

        match field {
            FieldType::Bool => matches!(postgres_type, &Type::BOOL),
            FieldType::Double => matches!(postgres_type, &Type::FLOAT8 | &Type::NUMERIC),
            FieldType::Float => matches!(postgres_type, &Type::FLOAT4 | &Type::NUMERIC),
            FieldType::Int32
            | FieldType::Uint32
            | FieldType::Sint32
            | FieldType::Sfixed32
            | FieldType::Fixed32 => {
                matches!(postgres_type, &Type::INT4)
            }
            FieldType::Int64
            | FieldType::Uint64
            | FieldType::Sint64
            | FieldType::Sfixed64
            | FieldType::Fixed64 => {
                matches!(postgres_type, &Type::INT8)
            }
            // FIXME: Byte and String types should be used as type escape hatches, right?
            // or do we only allow Messages to be inferred (e.g. timestamps)?
            FieldType::Bytes => matches!(postgres_type, &Type::BYTEA),
            FieldType::String => matches!(postgres_type, &Type::TEXT | &Type::VARCHAR),
            // FIXME: support repeated types, too
            fixme => todo!("FIXME: support {fixme:#?}"),
        }
    }
}
