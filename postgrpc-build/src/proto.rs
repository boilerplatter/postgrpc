use super::annotations;
use once_cell::sync::Lazy;
use postgres::types::Type as PostgresType;
use prost::{extension::ExtensionSetError, Extendable};
use prost_types::{
    field_descriptor_proto::Type as FieldType, DescriptorProto, EnumDescriptorProto,
    FieldDescriptorProto, MethodDescriptorProto,
};
use std::{collections::HashMap, fs, io};

// Special case of the Empty protobuf name
static EMPTY_DESCRIPTOR: Lazy<DescriptorProto> = Lazy::new(|| DescriptorProto {
    name: Some(".google.protobuf.Empty".to_string()),
    ..Default::default()
});

/// Postgrpc-specific variant of a service method
/// based on [`prost_types::MethodDescriptorProto`]
#[derive(Debug)]
pub(crate) struct Method<'a> {
    // FIXME: handle nested descriptors
    // FIXME: handle external (but referenced) descriptors (is this the same as nested?)
    // TODO: add back MethodDescriptorProto fields needed for code generation
    // (e.g. name, server_streaming, etc)
    input_type: &'a DescriptorProto,
    output_type: &'a DescriptorProto,
    query: String,
    name: String,
    // FIXME: resolve referenced enums and messages into better input and output types
    enums: &'a HashMap<String, &'a EnumDescriptorProto>,
}

impl<'a> Method<'a> {
    /// Get the SQL query associated with this [`Method`]
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Get the name of this [`Method`]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Extract the postgrpc options from a `prost_types::MethodDescriptorProto`
    /// and pair the inputs and outputs with their Message descriptors to create a [`Method`]
    pub(crate) fn from_method_descriptor(
        method: &'a MethodDescriptorProto,
        messages: &'a HashMap<String, &'a DescriptorProto>,
        enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    ) -> Result<Option<Self>, io::Error> {
        let input_type = get_message(messages, method.input_type())?;
        let output_type = get_message(messages, method.output_type())?;
        let name = method.name().to_string();

        if let Some(options) = &method.options {
            match options.extension_data(annotations::QUERY) {
                Ok(annotations::Query {
                    source: Some(source),
                }) => {
                    let query = match source {
                        annotations::query::Source::Sql(sql) => sql.to_owned(),
                        annotations::query::Source::File(path) => fs::read_to_string(path)?,
                    };

                    return Ok(Some(Self {
                        input_type,
                        output_type,
                        query,
                        name,
                        enums,
                    }));
                }
                Ok(..) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "postgrpc query is missing a valid source",
                    ))
                }
                Err(ExtensionSetError::ExtensionNotFound) => return Ok(None),
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            };
        }

        Ok(None)
    }

    /// Validate the method's input against some Postgres statement parameter types
    pub(crate) fn validate_input(&self, params: &[postgres::types::Type]) -> io::Result<()> {
        let message_name = self.input_type.name();
        let fields = &self.input_type.field;

        if fields.len() != params.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected {} parameters, but input message {message_name} has {} fields",
                    params.len(),
                    fields.len(),
                ),
            ));
        }

        // FIXME: order the fields by tag/number, not by proto file order!
        for (field, param) in fields.iter().zip(params.iter()) {
            validate_field(self.enums, field, param, message_name)?;
        }

        Ok(())
    }

    /// Validate the method's output against some Postgres statement column types
    pub(crate) fn validate_output(&self, columns: &[postgres::Column]) -> io::Result<()> {
        let message_name = self.output_type.name();
        let fields = &self.output_type.field;

        if fields.len() != columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected {} columns, but output message {message_name} has {} fields",
                    columns.len(),
                    fields.len(),
                ),
            ));
        }

        // FIXME: order the fields by tag/number(field.number()), not by proto file order!
        for (field, column) in fields.iter().zip(columns.iter()) {
            validate_field(self.enums, field, column.type_(), message_name)?;
        }

        Ok(())
    }
}

/// Helper function to extract a Message by name
fn get_message<'a, 'b>(
    messages: &'a HashMap<String, &'a DescriptorProto>,
    message_name: &'b str,
) -> io::Result<&'a DescriptorProto> {
    if message_name == EMPTY_DESCRIPTOR.name() {
        Ok(&EMPTY_DESCRIPTOR)
    } else {
        message_name
            .split('.')
            .last()
            .and_then(|message_name| messages.get(message_name).copied())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Expected message {message_name}, but it doesn't exist"),
                )
            })
    }
}

/// Helper function to extract an Enum by name
fn get_enum<'a, 'b>(
    enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    enum_name: &'b str,
) -> io::Result<&'a EnumDescriptorProto> {
    enum_name
        .split('.')
        .last()
        .and_then(|enum_name| enums.get(enum_name).copied())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected enum {enum_name}, but it doesn't exist"),
            )
        })
}

/// Validates a single message field against known messages and enums
fn validate_field<'a>(
    enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    field: &'a FieldDescriptorProto,
    postgres_type: &'a PostgresType,
    message_name: &'a str,
) -> io::Result<()> {
    if !match field.r#type() {
        FieldType::Bool => matches!(postgres_type, &PostgresType::BOOL),
        FieldType::Double => {
            matches!(
                postgres_type,
                &PostgresType::FLOAT8 | &PostgresType::NUMERIC
            )
        }
        FieldType::Float => {
            matches!(
                postgres_type,
                &PostgresType::FLOAT4 | &PostgresType::NUMERIC
            )
        }
        FieldType::Int32
        | FieldType::Uint32
        | FieldType::Sint32
        | FieldType::Sfixed32
        | FieldType::Fixed32 => {
            matches!(postgres_type, &PostgresType::INT4)
        }
        FieldType::Int64
        | FieldType::Uint64
        | FieldType::Sint64
        | FieldType::Sfixed64
        | FieldType::Fixed64 => {
            matches!(postgres_type, &PostgresType::INT8)
        }
        FieldType::Bytes => matches!(postgres_type, &PostgresType::BYTEA),
        FieldType::String => {
            matches!(postgres_type, &PostgresType::TEXT | &PostgresType::VARCHAR)
        }
        FieldType::Enum => match postgres_type.kind() {
            // validate the enum name
            postgres::types::Kind::Enum(members)
                if Some(postgres_type.name())
                    == field
                        .type_name()
                        .split('.')
                        .last()
                        .map(|name| name.to_lowercase())
                        .as_deref() =>
            {
                // validate the enum members
                let enum_members: Vec<_> = get_enum(enums, field.type_name())?
                    .value
                    .iter()
                    .map(|member| member.name())
                    .collect();

                if &enum_members != members {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Expected field {} of message {message_name} to be an enum with members {members:?}, but found members {enum_members:?} instead",
                            field.name(),
                        )
                    ));
                } else {
                    true
                }
            }
            _ => false,
        },
        fixme => todo!("FIXME: support {fixme:#?}"),
    } {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Expected field {} of message {message_name} to be of type {postgres_type}, but it was incompatible proto type {:?}",
                field.name(),
                field.r#type(),
            )
        ));
    }

    Ok(())
}
