use super::Parameter;
use bytes::{BufMut, BytesMut};
use num::cast::ToPrimitive;
use pbjson_types::{value::Kind, ListValue, Struct};
use postgres_array::Array;
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};

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
        Some(Kind::NumberValue(number)) => {
            let has_fractional = number.fract() != 0.0;

            match *type_ {
                Type::OID if !has_fractional => number
                    .to_u32()
                    .ok_or(format!("Cannot encode {number} as {type_}"))?
                    .to_sql(type_, out),
                Type::INT2 if !has_fractional => number
                    .to_i16()
                    .ok_or(format!("Cannot encode {number} as {type_}"))?
                    .to_sql(type_, out),
                Type::INT4 if !has_fractional => number
                    .to_i32()
                    .ok_or(format!("Cannot encode {number} as {type_}"))?
                    .to_sql(type_, out),
                Type::INT8 if !has_fractional => number
                    .to_i64()
                    .ok_or(format!("Cannot encode {number} as {type_}"))?
                    .to_sql(type_, out),
                Type::FLOAT4 => number
                    .to_f32()
                    .ok_or(format!("Cannot encode {number} as {type_}"))?
                    .to_sql(type_, out),
                Type::FLOAT8 => number.to_sql(type_, out),
                _ => Err(format!("Cannot encode {number} as type {type_}").into()),
            }
        }
        Some(Kind::ListValue(ListValue { values })) => match type_.kind() {
            // FIXME: handle ranges (?) and tuples as list pairs
            tokio_postgres::types::Kind::Array(array_type) => {
                generate_array(array_type, values.to_owned())?.to_sql(type_, out)
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
            // FIXME: handle all non-JSON "struct-like" postgres types
            _ => Err(format!(
                "Cannot encode struct {} as type {type_}",
                serde_json::to_value(fields)?,
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
        Some(Kind::ListValue(ListValue { values })) => {
            match type_.kind() {
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
                    "Cannot encode {} as type {type_}",
                    serde_json::to_value(values)?,
                )
                .into()),
            }
        }
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
                "Cannot encode struct {} as type {type_}",
                serde_json::to_value(fields)?,
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
        Some(Kind::ListValue(ListValue { values })) => match type_.kind() {
            // if any list element should be inferred, then they all should be inferred
            tokio_postgres::types::Kind::Array(array_type) => {
                // multi-dimensional Arrays should use the parent type
                // since the rust_postgres type only goes one level deep
                let mut values = values.iter().peekable();

                let type_ = match values.peek().map(|value| value.kind.as_ref()).flatten() {
                    Some(Kind::ListValue(..)) => type_,
                    _ => array_type,
                };

                values.any(|value| should_infer(&value.kind, type_))
            }
            // let ToSql handle invalid types
            _ => false,
        },
        Some(Kind::StructValue(Struct { fields })) => match type_.kind() {
            // if any of the struct fields should be inferred, then the entire struct should be inferred
            tokio_postgres::types::Kind::Composite(composite_fields) => composite_fields
                .iter()
                .any(|field| match fields.get(field.name()) {
                    Some(value) => should_infer(&value.kind, field.type_()),
                    None => false,
                }),
            // let ToSql handle invalid types
            _ => false,
        },
    }
}

/// Recursively generate a potentially-multi-dimensional Array from a set of values
fn generate_array(
    array_type: &Type,
    values: Vec<pbjson_types::Value>,
) -> Result<Array<Parameter>, Box<dyn std::error::Error + Sync + Send>> {
    let mut values = values.into_iter().map(|value| value.kind).flatten();

    let array = match values.next() {
        // handle multi-dimensional ARRAYs
        Some(Kind::ListValue(ListValue { values: first_row })) => {
            let dimension = first_row.len();
            let mut array = generate_array(array_type, first_row)?;
            array.wrap(0);

            for value in values {
                match value {
                    Kind::ListValue(ListValue { values }) if values.len() == dimension => {
                        let nested_array = generate_array(array_type, values.to_owned())?;
                        array.push(nested_array);
                    }
                    _ => {
                        return Err(format!(
                            "Cannot encode {} as an element of {array_type}[{dimension}]",
                            serde_json::to_value(value)?,
                        )
                        .into())
                    }
                }
            }

            array
        }
        // handle single-dimension ARRAYs
        Some(value) => Array::from_vec(
            vec![value]
                .into_iter()
                .chain(values)
                .map(|kind| Parameter::from(pbjson_types::Value { kind: Some(kind) }))
                .collect::<Vec<_>>(),
            0,
        ),
        // handle empty ARRAYs
        None => Array::from_vec(vec![], 0),
    };

    Ok(array)
}
