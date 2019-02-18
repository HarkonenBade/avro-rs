use std::mem::transmute;
use std::sync::Arc;

use schema::{SchemaTree, SchemaParseContext};
use types::Value;
use util::{zig_i32, zig_i64};

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: &Value, schema: &SchemaTree, buffer: &mut Vec<u8>) {
    encode_ref_inner(&value, &Arc::new(schema.clone()), buffer, &mut SchemaParseContext::new())
}

pub(crate) fn encode_inner(value: &Value, schema: &Arc<SchemaTree>, buffer: &mut Vec<u8>, context: &mut SchemaParseContext) {
    encode_ref_inner(&value, schema, buffer, context)
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode(&Value::Long(bytes.len() as i64), &SchemaTree::Long, buffer);
    buffer.extend_from_slice(bytes);
}

fn encode_long(i: i64, buffer: &mut Vec<u8>) {
    zig_i64(i, buffer)
}

fn encode_int(i: i32, buffer: &mut Vec<u8>) {
    zig_i32(i, buffer)
}

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub(crate)  fn encode_ref_inner(value: &Value, schema: &Arc<SchemaTree>, buffer: &mut Vec<u8>, context: &mut SchemaParseContext) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if *b { 1u8 } else { 0u8 }),
        Value::Int(i) => encode_int(*i, buffer),
        Value::Long(i) => encode_long(*i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&unsafe { transmute::<f32, [u8; 4]>(*x) }),
        Value::Double(x) => buffer.extend_from_slice(&unsafe { transmute::<f64, [u8; 8]>(*x) }),
        Value::Bytes(bytes) => encode_bytes(bytes, buffer),
        Value::String(s) => match **schema {
            SchemaTree::String => {
                encode_bytes(s, buffer);
            },
            SchemaTree::Enum { ref symbols, .. } => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                }
            },
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i, buffer),
        Value::Union(item) => {
            if let SchemaTree::Union(ref inner) = **schema {
                // Find the schema that is matched here. Due to validation, this should always
                // return a value.
                let (idx, inner_schema) = inner
                    .find_schema(item, context)
                    .expect("Invalid Union validation occurred");
                encode_long(idx as i64, buffer);
                encode_ref_inner(&*item, &inner_schema, buffer, context);
            }
        },
        Value::Array(items) => {
            if let SchemaTree::Array(ref inner) = **schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for item in items.iter() {
                        encode_ref_inner(item, &inner, buffer, context);
                    }
                }
                buffer.push(0u8);
            }
        },
        Value::Map(items) => {
            if let SchemaTree::Map(ref inner) = **schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for (key, value) in items {
                        encode_bytes(key, buffer);
                        encode_ref_inner(value, inner, buffer, context);
                    }
                }
                buffer.push(0u8);
            }
        },
        Value::Record(fields) => {
            if let SchemaTree::Record {
                fields: ref schema_fields,
                ..
            } = **schema
            {
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    trace!("Encode field: {:?} with schema {:?}", value, &schema_fields[i].schema);
                    encode_ref_inner(value, &schema_fields[i].schema, buffer, context);
                }
            } else if let SchemaTree::TypeReference(ref n) = **schema {
                if let Some(ref_schema) = context.lookup_type(&n, &context) {
                    if let SchemaTree::Record {
                        fields: ref schema_fields,
                        ..
                    } = *ref_schema {
                        for (i, &(_, ref value)) in fields.iter().enumerate() {
                            trace!("Encode field: {:?} with schema {:?}", value, &schema_fields[i].schema);
                            encode_ref_inner(value, &schema_fields[i].schema, buffer, context);
                        }
                    }
                }
            }
        },
    }
}

pub fn encode_to_vec(value: &Value, schema: &SchemaTree) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(&value, schema, &mut buffer);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_encode_empty_array() {
        let mut buf = Vec::new();
        let empty: Vec<Value> = Vec::new();
        encode(
            &Value::Array(empty),
            &SchemaTree::Array(Arc::new(SchemaTree::Int)),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_encode_empty_map() {
        let mut buf = Vec::new();
        let empty: HashMap<String, Value> = HashMap::new();
        encode(
            &Value::Map(empty),
            &SchemaTree::Map(Arc::new(SchemaTree::Int)),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }
}
