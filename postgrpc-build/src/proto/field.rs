use super::message::Message;
use postgres::types::Type as PostgresType;
use prost_types::{
    field_descriptor_proto::Type as FieldType, DescriptorProto, EnumDescriptorProto,
    FieldDescriptorProto,
};
use std::{collections::HashMap, io};

/// Internal representation of a [`Message`] field along with a reference to its fully-resolved composite type
#[derive(Debug)]
pub(crate) struct Field<'a> {
    proto: &'a FieldDescriptorProto,
    // CORRECTNESS: this should only be resolved for composite Message and Enum types
    composite_type: Option<CompositeType<'a>>,
}

impl<'a, 'b> Field<'a> {
    /// attempt to resolve a proper Field and its associated composite types
    pub(crate) fn try_resolve(
        proto: &'a FieldDescriptorProto,
        messages: &'b HashMap<String, &'a DescriptorProto>,
        enums: &'b HashMap<String, &'a EnumDescriptorProto>,
    ) -> io::Result<Self> {
        match proto.r#type() {
            FieldType::Enum => {
                let enum_name = proto.type_name();

                // resolve both top-level and nested enum resolution within a single file
                let composite_type = match enums.get(enum_name) {
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

                // resolve both top-level and nested message resolution within a single file
                let composite_type = match messages.get(message_name) {
                    Some(found_message) => Message::try_resolve(found_message, messages, enums)
                        .map(CompositeType::Message)
                        .map(Option::Some)?,
                    None => match message_name.split('.').collect::<Vec<_>>()[..] {
                        [_, package, parent, message_name] => messages
                            .get(&format!(".{package}.{parent}"))
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "Expected message {}, but it doesn't exist",
                                        proto.type_name()
                                    ),
                                )
                            })?
                            .nested_type
                            .iter()
                            .find(|nested_message| nested_message.name() == message_name)
                            .map(|found_message| {
                                Message::try_resolve(found_message, messages, enums)
                            })
                            .transpose()?
                            .map(CompositeType::Message),
                        _ => {
                            // FIXME: handle recursive and deeply-nested cases better
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "Expected message {}, but it couldn't be found",
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
            // scalar values need no resolution
            _ => Ok(Self {
                proto,
                composite_type: None,
            }),
        }
    }

    /// Validate a field against a validate-able Postgres type
    pub(crate) fn validate<T>(&'a self, comparator: &'a T) -> io::Result<()>
    where
        T: FieldValidate,
    {
        if let Some(name) = comparator.name() {
            if self.name() != name {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Expected field {} but found {name} instead", self.name()),
                ));
            }
        }

        let postgres_type = comparator.type_();

        if !match self.r#type() {
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
            FieldType::Int32 | FieldType::Sint32 | FieldType::Sfixed32 | FieldType::Fixed32 => {
                matches!(postgres_type, &PostgresType::INT4)
            }
            FieldType::Int64 | FieldType::Sint64 | FieldType::Sfixed64 | FieldType::Fixed64 => {
                matches!(postgres_type, &PostgresType::INT8)
            }
            FieldType::Bytes => matches!(postgres_type, &PostgresType::BYTEA),
            FieldType::String => {
                matches!(postgres_type, &PostgresType::TEXT | &PostgresType::VARCHAR)
            }
            FieldType::Enum => match (postgres_type.kind(), self.composite_type.as_ref()) {
                (
                    postgres::types::Kind::Enum(members),
                    Some(CompositeType::Enum(EnumDescriptorProto {
                        value: enum_value, ..
                    })),
                ) if Some(postgres_type.name()) == self.type_name().split('.').last() => {
                    // validate the enum members
                    let enum_members = enum_value
                        .iter()
                        .map(|member| member.name())
                        .collect::<Vec<_>>();

                    if &enum_members != members {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Expected field {} to be an enum with members {members:?}, but found members {enum_members:?} instead",
                                self.name(),
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
                            "Expected field {} to be of type {postgres_type}, but it was incompatible proto type {}",
                            self.name(),
                            self.type_name(),
                        )
                    ));
                }
            },
            FieldType::Message => match (postgres_type.kind(), self.composite_type.as_ref()) {
                (
                    postgres::types::Kind::Composite(fields),
                    Some(CompositeType::Message(message)),
                ) if Some(postgres_type.name()) == self.type_name().split('.').last() => {
                    message.validate_fields(fields)?;

                    true
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Expected field {} to be of type {postgres_type}, but it was incompatible proto type {}",
                            self.name(),
                            self.type_name(),
                        )
                    ));
                }
            },
            FieldType::Uint32 | FieldType::Uint64 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Unsigned integers are not supported by Postgres. Please use an integer type for field {}",
                        self.name(),
                    )
                ));
            }
            unsupported_field => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Fields of type {unsupported_field:?} are not supported by PostgRPC. Please use a different protobuf type.",
                    )
                ));
            }
        } {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected field {} to be of type {postgres_type}, but it was incompatible proto type {:?}",
                    self.name(),
                    self.r#type(),
                )
            ));
        }

        Ok(())
    }

    pub(crate) fn number(&self) -> i32 {
        self.proto.number()
    }

    pub(crate) fn name(&self) -> &str {
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

/// Helper trait to describe any Postgres type that can be validated against a field
pub(crate) trait FieldValidate {
    fn type_(&self) -> &PostgresType;
    fn name(&self) -> Option<&str> {
        None
    }
}

impl FieldValidate for PostgresType {
    fn type_(&self) -> &PostgresType {
        self
    }
}

impl FieldValidate for postgres::Column {
    fn type_(&self) -> &PostgresType {
        self.type_()
    }

    fn name(&self) -> Option<&str> {
        Some(self.name())
    }
}

impl FieldValidate for postgres::types::Field {
    fn type_(&self) -> &PostgresType {
        self.type_()
    }

    fn name(&self) -> Option<&str> {
        Some(self.name())
    }
}
