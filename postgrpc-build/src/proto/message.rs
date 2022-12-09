use super::field::{Field, FieldValidate};
use once_cell::sync::Lazy;
use prost_types::{DescriptorProto, EnumDescriptorProto};
use std::{collections::HashMap, io};

// Special case of the Empty protobuf name
// FIXME: handle the rest of protobuf's well-known-types
static EMPTY_DESCRIPTOR: Lazy<DescriptorProto> = Lazy::new(|| DescriptorProto {
    name: Some(".google.protobuf.Empty".to_string()),
    ..Default::default()
});

/// Internal representation of input and output messages
#[derive(Debug)]
pub(crate) struct Message<'a> {
    name: &'a str,
    fields: Vec<Field<'a>>,
}

impl<'a, 'b> Message<'a> {
    /// Bulid an empty message corresponding to the Empty well-known-type
    pub(crate) fn empty() -> Self {
        Self {
            name: EMPTY_DESCRIPTOR.name(),
            fields: Vec::new(),
        }
    }

    /// Create a new Message and resolve its fields' dependency graph along the way
    pub(crate) fn try_resolve(
        proto: &'a DescriptorProto,
        messages: &'b HashMap<String, &'a DescriptorProto>,
        enums: &'b HashMap<String, &'a EnumDescriptorProto>,
    ) -> io::Result<Self> {
        // ensure fields are ordered by their tag/number instead of file order
        let mut fields = proto
            .field
            .iter()
            .map(|field| Field::try_resolve(field, messages, enums))
            .collect::<Result<Vec<_>, _>>()?;

        fields.sort_unstable_by_key(|field| field.number());

        Ok(Self {
            name: proto.name(),
            fields,
        })
    }

    pub(crate) fn proto_type(&self) -> &str {
        if self.fields.is_empty() {
            "()"
        } else {
            self.name()
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.name
    }

    pub(crate) fn fields(&'a self) -> impl Iterator<Item = &'a Field<'a>> {
        self.fields.iter()
    }

    /// Validate the [`Message`] fields against Postgres types
    pub(crate) fn validate_fields<T>(&self, comparators: &[T]) -> io::Result<()>
    where
        T: FieldValidate,
    {
        if self.fields.len() != comparators.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected {} fields, but found {}",
                    self.fields.len(),
                    comparators.len()
                ),
            ));
        }

        for (field, comparator) in self.fields.iter().zip(comparators) {
            field.validate(comparator)?;
        }

        Ok(())
    }
}

/// Helper function to extract a Message from top-level messages by name
pub(crate) fn get_message<'a, 'b>(
    messages: &'b HashMap<String, &'a DescriptorProto>,
    enums: &'b HashMap<String, &'a EnumDescriptorProto>,
    message_name: &'b str,
) -> io::Result<Message<'a>> {
    if message_name == EMPTY_DESCRIPTOR.name() {
        Ok(Message::empty())
    } else {
        // get the message from the top-level context
        let message = messages.get(message_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected message {message_name}, but it doesn't exist"),
            )
        })?;

        // resolve the message's field graph
        Message::try_resolve(message, messages, enums)
    }
}
