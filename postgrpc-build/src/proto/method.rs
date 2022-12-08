use super::message::{get_message, Message};
use crate::annotations;
use postgres::types::Type as PostgresType;
use prost::{extension::ExtensionSetError, Extendable};
use prost_types::{DescriptorProto, EnumDescriptorProto, MethodDescriptorProto};
use std::{collections::HashMap, fs, io};

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

impl<'a, 'b> Method<'a> {
    /// Get the SQL query associated with this [`Method`]
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Get the name of this [`Method`]
    pub(crate) fn name(&self) -> &str {
        self.name
    }

    /// Get the input type of this [`Method`]
    pub(crate) fn input_type(&self) -> &Message<'a> {
        &self.input_type
    }

    /// Get the output type of this [`Method`]
    pub(crate) fn output_type(&self) -> &Message<'a> {
        &self.output_type
    }

    /// Extract the postgrpc options from a `prost_types::MethodDescriptorProto`
    /// and pair the inputs and outputs with their Message descriptors to create a [`Method`]
    pub(crate) fn from_method_descriptor(
        method: &'a MethodDescriptorProto,
        messages: &'b HashMap<String, &'a DescriptorProto>,
        enums: &'b HashMap<String, &'a EnumDescriptorProto>,
    ) -> Result<Option<Self>, io::Error> {
        // FIXME: check for missing input/output types
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
                        // FIXME: give better file-not-found errors for these!
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
    pub(crate) fn validate_input(&self, params: &[PostgresType]) -> io::Result<()> {
        self.input_type.validate_fields(params)
    }

    /// Validate the method's output against some Postgres statement column types
    pub(crate) fn validate_output(&self, columns: &[postgres::Column]) -> io::Result<()> {
        self.output_type.validate_fields(columns)
    }
}
