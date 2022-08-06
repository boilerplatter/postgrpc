/// JSON-specific helpers converting between serde and prost structures

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
            prost_types::value::Kind::StructValue(map::to_proto_struct(map))
        }
    };

    prost_types::Value { kind: Some(kind) }
}

pub(crate) mod map {
    /// Convert a serde_json::Map into a prost_types::Struct
    pub(crate) fn to_proto_struct(
        map: serde_json::Map<String, serde_json::Value>,
    ) -> prost_types::Struct {
        prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(key, value)| (key, super::to_proto_value(value)))
                .collect(),
        }
    }
}
