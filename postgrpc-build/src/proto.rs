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
// FIXME: handle the rest of protobuf's well-known-types
static EMPTY_DESCRIPTOR: Lazy<DescriptorProto> = Lazy::new(|| DescriptorProto {
    name: Some(".google.protobuf.Empty".to_string()),
    ..Default::default()
});

/// Internal representation of input and output messages
#[derive(Debug)]
struct Message<'a> {
    proto: &'a DescriptorProto,
    fields: Vec<Field<'a>>,
}

impl<'a> Message<'a> {
    /// Create a new message without attempting to resolve its fields
    fn new(proto: &'a DescriptorProto) -> Self {
        Self {
            proto,
            fields: Vec::new(),
        }
    }

    /// Create a new Message and resolve its fields' dependency graph along the way
    fn try_resolve(
        proto: &'a DescriptorProto,
        messages: &'a HashMap<String, &'a DescriptorProto>,
        enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    ) -> io::Result<Self> {
        // ensure fields are ordered by their tag/number instead of file order
        let mut fields = proto
            .field
            .iter()
            .map(|field| Field::try_resolve(field, messages, enums))
            .collect::<Result<Vec<_>, _>>()?;

        fields.sort_unstable_by_key(|field| field.number());

        Ok(Self { proto, fields })
    }

    fn name(&self) -> &str {
        self.proto.name()
    }
}

/// Internal representation of a field along with a reference to its fully-resolved composite type
#[derive(Debug)]
struct Field<'a> {
    proto: &'a FieldDescriptorProto,
    // CORRECTNESS: this should only be resolved for composite Message and Enum types
    composite_type: Option<CompositeType<'a>>,
}

impl<'a> Field<'a> {
    /// attempt to resolve a proper Field and its associated composite types
    fn try_resolve(
        proto: &'a FieldDescriptorProto,
        messages: &'a HashMap<String, &'a DescriptorProto>,
        enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    ) -> io::Result<Self> {
        match proto.r#type() {
            FieldType::Enum => {
                let enum_name = proto.type_name();

                // resolve both top-level and nested enum resolution within a single file
                let composite_type = match enums.get(enum_name).copied() {
                    Some(found_enum) => Some(CompositeType::Enum(found_enum)),
                    None => match enum_name.split('.').collect::<Vec<_>>()[..] {
                        [_, package, parent, enum_name] => messages
                            .get(&format!(".{package}.{parent}"))
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "Expected enum {}, but it doesn't exist",
                                        proto.type_name()
                                    ),
                                )
                            })?
                            .enum_type
                            .iter()
                            .find(|nested_enum| nested_enum.name() == enum_name)
                            .map(CompositeType::Enum),
                        _ => {
                            // FIXME: handle recursive and deeply-nested cases better
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "Expected enum {}, but it couldn't be found",
                                    proto.type_name()
                                ),
                            ));
                        }
                    },
                };

                Ok(Self {
                    proto,
                    composite_type,
                })
            }
            FieldType::Message => {
                let message_name = proto.type_name();

                // FIXME: check nested messages, too! Should be able to retrieve from the parent
                let composite_type = messages
                    .get(message_name)
                    .copied()
                    .map(|message| {
                        Message::try_resolve(message, messages, enums).map(CompositeType::Message)
                    })
                    .transpose()?
                    .map(Option::Some)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Expected message {}, but it doesn't exist",
                                proto.type_name()
                            ),
                        )
                    })?;

                Ok(Self {
                    proto,
                    composite_type,
                })
            }
            // scalar values need no resolution
            _ => Ok(Self {
                proto,
                composite_type: None,
            }),
        }
    }

    fn number(&self) -> i32 {
        self.proto.number()
    }

    fn name(&self) -> &str {
        self.proto.name()
    }

    fn r#type(&self) -> FieldType {
        self.proto.r#type()
    }

    fn type_name(&self) -> &str {
        self.proto.type_name()
    }
}

/// Supported composite types from protobuf definitions
#[derive(Debug)]
enum CompositeType<'a> {
    Message(Message<'a>),
    Enum(&'a EnumDescriptorProto),
}

/// Postgrpc-specific variant of a service method
/// based on [`prost_types::MethodDescriptorProto`]
#[derive(Debug)]
pub(crate) struct Method<'a> {
    // TODO: add back MethodDescriptorProto fields needed for code generation
    // (e.g. name, server_streaming, etc)
    input_type: Message<'a>,
    output_type: Message<'a>,
    query: String,
    name: &'a str,
}

impl<'a> Method<'a> {
    /// Get the SQL query associated with this [`Method`]
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Get the name of this [`Method`]
    pub(crate) fn name(&self) -> &str {
        self.name
    }

    /// Extract the postgrpc options from a `prost_types::MethodDescriptorProto`
    /// and pair the inputs and outputs with their Message descriptors to create a [`Method`]
    pub(crate) fn from_method_descriptor(
        method: &'a MethodDescriptorProto,
        messages: &'a HashMap<String, &'a DescriptorProto>,
        enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    ) -> Result<Option<Self>, io::Error> {
        let input_type = get_message(messages, enums, method.input_type())?;
        let output_type = get_message(messages, enums, method.output_type())?;
        let name = method.name();

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
        let fields = &self.input_type.fields;

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

        // validate fields and params
        for (field, param) in fields.iter().zip(params.iter()) {
            validate_field(field, param, message_name)?;
        }

        Ok(())
    }

    /// Validate the method's output against some Postgres statement column types
    pub(crate) fn validate_output(&self, columns: &[postgres::Column]) -> io::Result<()> {
        let message_name = self.output_type.name();
        let fields = &self.output_type.fields;

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

        // validate fields and params
        for (field, column) in fields.iter().zip(columns.iter()) {
            validate_field(field, column.type_(), message_name)?;
        }

        Ok(())
    }
}

/// Helper function to extract a Message from top-level messages by name
fn get_message<'a, 'b>(
    messages: &'a HashMap<String, &'a DescriptorProto>,
    enums: &'a HashMap<String, &'a EnumDescriptorProto>,
    message_name: &'b str,
) -> io::Result<Message<'a>> {
    if message_name == EMPTY_DESCRIPTOR.name() {
        Ok(Message::new(&EMPTY_DESCRIPTOR))
    } else {
        // get the message from the top-level context
        let message = messages.get(message_name).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected message {message_name}, but it doesn't exist"),
            )
        })?;

        // resolve the message's field graph
        Message::try_resolve(message, messages, enums)
    }
}

/// Validates a single message field against known messages and enums
// FIXME: make this a method on the Field type
// FIXME: remove the message_name requirement (wrap at the caller if needed)
fn validate_field<'a>(
    field: &'a Field<'a>,
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
        FieldType::Enum => match (postgres_type.kind(), field.composite_type.as_ref()) {
            (
                postgres::types::Kind::Enum(members),
                Some(CompositeType::Enum(EnumDescriptorProto {
                    value: enum_value, ..
                })),
            ) if Some(postgres_type.name()) == field.type_name().split('.').last() => {
                // validate the enum members
                let enum_members = enum_value
                    .iter()
                    .map(|member| member.name())
                    .collect::<Vec<_>>();

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
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Expected field {} of message {message_name} to be of type {postgres_type}, but it was incompatible proto type {}",
                        field.name(),
                        field.type_name(),
                    )
                ));
            }
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
