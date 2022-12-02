use super::annotations;
use once_cell::sync::Lazy;
use prost::{extension::ExtensionSetError, Extendable};
use prost_types::{DescriptorProto, MethodDescriptorProto};
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
    input_type: &'a DescriptorProto,
    output_type: &'a DescriptorProto,
    query: String,
    name: String,
    // TODO: add back MethodDescriptorProto fields needed for code generation
    // (e.g. name, server_streaming, etc)
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

        for (field, param) in fields.iter().zip(params.iter()) {
            if super::postgres::PostgresType::from(param) != field.r#type() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Expected field {} of message {message_name} to be of type {param}, but it was incompatible proto type {:?}",
                        field.name(),
                        field.r#type(),
                    )
                ));
            }
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

        for (field, column) in fields.iter().zip(columns.iter()) {
            let column = column.type_();

            if super::postgres::PostgresType::from(column) != field.r#type() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Expected field {} of message {message_name} to be of type {column}, but it was incompatible proto type {:?}",
                        field.name(),
                        field.r#type(),
                    )
                ));
            }
        }

        Ok(())
    }
}

/// Helper function to extract a reference to a Message by name
fn get_message<'a, 'b>(
    messages: &'a HashMap<String, &'a DescriptorProto>,
    message_name: &'b str,
) -> io::Result<&'a DescriptorProto> {
    if message_name == EMPTY_DESCRIPTOR.name() {
        Ok(&*EMPTY_DESCRIPTOR)
    } else {
        message_name
            .split('.')
            .last()
            .and_then(|message_name| messages.get(message_name).map(|message| *message))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Expected message {message_name}, but it doesn't exist"),
                )
            })
    }
}
