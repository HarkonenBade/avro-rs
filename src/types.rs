//! Logic handling the intermediate representation of Avro values.
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use failure::Error;
use serde_json::Value as JsonValue;

use schema::{RecordField, Schema, SchemaKind, SchemaTree, SchemaParseContext, UnionSchema};

/// Describes errors happened while performing schema resolution on Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Decoding error: {}", _0)]
pub struct SchemaResolutionError(String);

impl SchemaResolutionError {
    pub fn new<S>(msg: S) -> SchemaResolutionError
    where
        S: Into<String>,
    {
        SchemaResolutionError(msg.into())
    }
}

/// Represents any valid Avro value
/// More information about Avro values can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// A `null` Avro value.
    Null,
    /// A `boolean` Avro value.
    Boolean(bool),
    /// A `int` Avro value.
    Int(i32),
    /// A `long` Avro value.
    Long(i64),
    /// A `float` Avro value.
    Float(f32),
    /// A `double` Avro value.
    Double(f64),
    /// A `bytes` Avro value.
    Bytes(Vec<u8>),
    /// A `string` Avro value.
    String(String),
    /// A `fixed` Avro value.
    /// The size of the fixed value is represented as a `usize`.
    Fixed(usize, Vec<u8>),
    /// An `enum` Avro value.
    ///
    /// An Enum is represented by a symbol and its position in the symbols list
    /// of its corresponding schema.
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Enum(i32, String),
    /// An `union` Avro value.
    Union(Box<Value>),
    /// An `array` Avro value.
    Array(Vec<Value>),
    /// A `map` Avro value.
    Map(HashMap<String, Value>),
    /// A `record` Avro value.
    ///
    /// A Record is represented by a vector of (`<record name>`, `value`).
    /// This allows schema-less encoding.
    ///
    /// See [Record](types.Record) for a more user-friendly support.
    Record(Vec<(String, Value)>),
}

/// Any structure implementing the [ToAvro](trait.ToAvro.html) trait will be usable
/// from a [Writer](../writer/struct.Writer.html).
pub trait ToAvro {
    /// Transforms this value into an Avro-compatible [Value](enum.Value.html).
    fn avro(self) -> Value;
}

macro_rules! to_avro(
    ($t:ty, $v:expr) => (
        impl ToAvro for $t {
            fn avro(self) -> Value {
                $v(self)
            }
        }
    );
);

to_avro!(bool, Value::Boolean);
to_avro!(i32, Value::Int);
to_avro!(i64, Value::Long);
to_avro!(f32, Value::Float);
to_avro!(f64, Value::Double);
to_avro!(String, Value::String);

impl ToAvro for () {
    fn avro(self) -> Value {
        Value::Null
    }
}

impl ToAvro for usize {
    fn avro(self) -> Value {
        (self as i64).avro()
    }
}

impl<'a> ToAvro for &'a str {
    fn avro(self) -> Value {
        Value::String(self.to_owned())
    }
}

impl<'a> ToAvro for &'a [u8] {
    fn avro(self) -> Value {
        Value::Bytes(self.to_owned())
    }
}

impl<T> ToAvro for Option<T>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        let v = match self {
            Some(v) => T::avro(v),
            None => Value::Null,
        };
        Value::Union(Box::new(v))
    }
}

impl<T, S: BuildHasher> ToAvro for HashMap<String, T, S>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(key, value)| (key, value.avro()))
                .collect::<_>(),
        )
    }
}

impl<'a, T, S: BuildHasher> ToAvro for HashMap<&'a str, T, S>
where
    T: ToAvro,
{
    fn avro(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(key, value)| (key.to_owned(), value.avro()))
                .collect::<_>(),
        )
    }
}

impl ToAvro for Value {
    fn avro(self) -> Value {
        self
    }
}

/*
impl<S: Serialize> ToAvro for S {
    fn avro(self) -> Value {
        use ser::Serializer;

        self.serialize(&mut Serializer::new()).unwrap()
    }
}
*/

/// Utility interface to build `Value::Record` objects.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_lookup: &'a HashMap<String, usize>,
}

impl<'a> Record<'a> {
    /// Create a `Record` given a `Schema`.
    ///
    /// If the `Schema` is not a `Schema::Record` variant, `None` will be returned.
    pub fn new<'b>(schema: &'b Schema) -> Option<Record<'b>> {
        Self::with_placeholder(schema, &Value::Null)
    }

    pub fn with_placeholder<'b>(schema: &'b Schema, placeholder: &Value) -> Option<Record<'b>> {
        match schema.ref_inner() {
            SchemaTree::Record {
                fields: schema_fields,
                lookup: schema_lookup,
                ..
            } => {
                let mut fields = Vec::with_capacity(schema_fields.len());
                for schema_field in schema_fields.iter() {
                    fields.push((schema_field.name.clone(), (*placeholder).clone()));
                }

                Some(Record {
                    fields,
                    schema_lookup,
                })
            },
            _ => None,
        }
    }

    /// Put a compatible value (implementing the `ToAvro` trait) in the
    /// `Record` for a given `field` name.
    ///
    /// **NOTE** Only ensure that the field name is present in the `Schema` given when creating
    /// this `Record`. Does not perform any schema validation.
    pub fn put<V>(&mut self, field: &str, value: V)
    where
        V: ToAvro,
    {
        if let Some(&position) = self.schema_lookup.get(field) {
            self.fields[position].1 = value.avro()
        }
    }
}

impl<'a> ToAvro for Record<'a> {
    fn avro(self) -> Value {
        Value::Record(self.fields)
    }
}

impl ToAvro for JsonValue {
    fn avro(self) -> Value {
        match self {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Boolean(b),
            JsonValue::Number(ref n) if n.is_i64() => Value::Long(n.as_i64().unwrap()),
            JsonValue::Number(ref n) if n.is_f64() => Value::Double(n.as_f64().unwrap()),
            JsonValue::Number(n) => Value::Long(n.as_u64().unwrap() as i64), // TODO: Not so great
            JsonValue::String(s) => Value::String(s),
            JsonValue::Array(items) => {
                Value::Array(items.into_iter().map(|item| item.avro()).collect::<_>())
            },
            JsonValue::Object(items) => Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| (key, value.avro()))
                    .collect::<_>(),
            ),
        }
    }
}

impl Value {

    /// Validate the value against the given [Schema](../schema/enum.Schema.html).
    ///
    /// See the [Avro specification](https://avro.apache.org/docs/current/spec.html)
    /// for the full set of rules of schema validation.
    pub fn validate(&self, schema: &Schema) -> bool {
        let mut context = schema.new_context();
        self.validate_inner(&schema.inner(), &mut context)
    }

    pub(crate) fn validate_inner(&self, schema: &Arc<SchemaTree>, context: &mut SchemaParseContext) -> bool {

        match (self, &**schema) {
            (&Value::Null, SchemaTree::Null) => true,
            (&Value::Boolean(_), SchemaTree::Boolean) => true,
            (&Value::Int(_), SchemaTree::Int) => true,
            (&Value::Long(_), SchemaTree::Long) => true,
            (&Value::Float(_), SchemaTree::Float) => true,
            (&Value::Double(_), SchemaTree::Double) => true,
            (&Value::Bytes(_), SchemaTree::Bytes) => true,
            (&Value::String(_), SchemaTree::String) => true,
            (&Value::Fixed(n, _), SchemaTree::Fixed { ref name, size }) => {
                if let Some(_) = name.name.namespace {
                    context.current_namespace = name.name.namespace.clone();
                }
                trace!("Val: Fixed({}) vs Fixed({:?}, {})", n, name, size);
                n == *size
            },
            (&Value::String(ref s), SchemaTree::Enum { ref symbols, ref name, .. }) => {
                if let Some(_) = name.name.namespace {
                    context.current_namespace = name.name.namespace.clone();
                }
                trace!("Val: String({}) vs Enum({:?})", s, name);
                symbols.contains(s)
            },
            (&Value::Enum(i, ref s), SchemaTree::Enum { ref symbols, ref name, .. }) => {
                if let Some(_) = name.name.namespace {
                    context.current_namespace = name.name.namespace.clone();
                }
                trace!("Val: Enum({}) vs Enum({:?})", s, name);
                symbols
                    .get(i as usize)
                    .map(|ref symbol| symbol == &s)
                    .unwrap_or(false)
            },
            (&Value::Union(ref value), SchemaTree::Union(ref inner)) => {
                trace!("Val: Union({:?}) vs Union({:?})", value, inner);
                inner.find_schema(value, context).is_some()
            },
            (&Value::Array(ref items), SchemaTree::Array(ref inner)) => {
                trace!("Val: Array() vs Array()");
                items.iter().all(|item| item.validate_inner(inner, context))
            },
            (&Value::Map(ref items), SchemaTree::Map(ref inner)) => {
                trace!("Val: Map() vs Map()");
                items.iter().all(|(_, value)| value.validate_inner(inner, context))
            },
            (&Value::Record(ref record_fields), SchemaTree::Record { ref fields, ref name, .. }) => {
                if let Some(_) = name.name.namespace {
                    context.current_namespace = name.name.namespace.clone();
                }
                trace!("Val: Record({:?}) vs Record({:?})", record_fields, name);
                fields.len() == record_fields.len() && fields.iter().zip(record_fields.iter()).all(
                    |(field, &(ref name, ref value))| {
                        field.name == *name && value.validate_inner(&field.schema, context)
                    },
                )
            },
            (r @ Value::Record(..), SchemaTree::Union(ref inner)) => {
                trace!("Val: Record() vs Union()");
                inner.find_schema(r, context).is_some()
            },
            (&Value::Record(_), SchemaTree::TypeReference (ref name)) => {
                trace!("Val: Record() vs Ref({:?})", name);
                match context.lookup_type(name, context) {
                    Some(ref s) => self.validate_inner(s, context),
                    None => false
                }
            },
            (&Value::Fixed(n, _), SchemaTree::TypeReference (ref name)) => {
                trace!("Val: Fixed({}) vs Ref({:?})", n, name);
                match context.lookup_type(name, context) {
                    Some(ref s) => self.validate_inner(s, context),
                    None => false
                }
            },
            (x, y) => {
                trace!("Failed match ({:?}, {:?})", x, y);
                false
            },
        }
    }

    /// Attempt to perform schema resolution on the value, with the given
    /// [Schema](../schema/enum.Schema.html).
    ///
    /// See [Schema Resolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
    /// in the Avro specification for the full set of rules of schema
    /// resolution.
    pub fn resolve(mut self, schema: &Arc<SchemaTree>, context: &mut SchemaParseContext) -> Result<Self, Error> {
        // Check if this schema is a union, and if the reader schema is not.
        if SchemaKind::from(&self) == SchemaKind::Union
            && SchemaKind::from(&**schema) != SchemaKind::Union
        {
            // Pull out the Union, and attempt to resolve against it.
            let v = match self {
                Value::Union(b) => *b,
                _ => unreachable!(),
            };
            self = v;
        }
        match **schema {
            SchemaTree::Null => self.resolve_null(),
            SchemaTree::Boolean => self.resolve_boolean(),
            SchemaTree::Int => self.resolve_int(),
            SchemaTree::Long => self.resolve_long(),
            SchemaTree::Float => self.resolve_float(),
            SchemaTree::Double => self.resolve_double(),
            SchemaTree::Bytes => self.resolve_bytes(),
            SchemaTree::String => self.resolve_string(),
            SchemaTree::Fixed { size, .. } => self.resolve_fixed(size),
            SchemaTree::Union(ref inner) => self.resolve_union(&inner.clone(), context),
            SchemaTree::Enum { ref symbols, .. } => self.resolve_enum(symbols),
            SchemaTree::Array(ref inner) => self.resolve_array(inner, context),
            SchemaTree::Map(ref inner) => self.resolve_map(inner, context),
            SchemaTree::Record { ref fields,  .. } => {
                self.resolve_record(fields, context)
            } ,
            SchemaTree::TypeReference(ref name) => context.lookup_type(name, &context)
                .map_or_else(|| Err(SchemaResolutionError::new(format!("Couldn't resolve type reference: {:?}", name)).into()),
                             |s| self.resolve(&s, context)),

        }
    }

    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => {
                Err(SchemaResolutionError::new(format!("Null expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => {
                Err(SchemaResolutionError::new(format!("Boolean expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => Ok(Value::Int(n as i32)),
            other => {
                Err(SchemaResolutionError::new(format!("Int expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => {
                Err(SchemaResolutionError::new(format!("Long expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_float(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            Value::Double(x) => Ok(Value::Float(x as f32)),
            other => {
                Err(SchemaResolutionError::new(format!("Float expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_double(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Double(f64::from(n))),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(f64::from(x))),
            Value::Double(x) => Ok(Value::Double(x)),
            other => {
                Err(SchemaResolutionError::new(format!("Double expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_bytes(self) -> Result<Self, Error> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            other => {
                Err(SchemaResolutionError::new(format!("Bytes expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) => Ok(Value::String(String::from_utf8(bytes)?)),
            other => {
                Err(SchemaResolutionError::new(format!("String expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_fixed(self, size: usize) -> Result<Self, Error> {
        match self {
            Value::Fixed(n, bytes) => if n == size {
                Ok(Value::Fixed(n, bytes))
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Fixed size mismatch, {} expected, got {}",
                    size, n
                )).into())
            },
            other => {
                Err(SchemaResolutionError::new(format!("String expected, got {:?}", other)).into())
            },
        }
    }

    fn resolve_enum(self, symbols: &[String]) -> Result<Self, Error> {
        let validate_symbol = |symbol: String, symbols: &[String]| {
            if let Some(index) = symbols.iter().position(|ref item| item == &&symbol) {
                Ok(Value::Enum(index as i32, symbol))
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Enum default {} is not among allowed symbols {:?}",
                    symbol, symbols,
                )).into())
            }
        };

        match self {
            Value::Enum(i, s) => if i > 0 && i < symbols.len() as i32 {
                validate_symbol(s, symbols)
            } else {
                Err(SchemaResolutionError::new(format!(
                    "Enum value {} is out of bound {}",
                    i,
                    symbols.len() as i32
                )).into())
            },
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(SchemaResolutionError::new(format!(
                "Enum({:?}) expected, got {:?}",
                symbols, other
            )).into()),
        }
    }

    fn resolve_union(self, schema: &UnionSchema, context: &mut SchemaParseContext) -> Result<Self, Error> {
        let v = match self {
            // Both are unions case.
            Value::Union(v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };
        // Find the first match in the reader schema.
        let (_, inner) = schema
            .find_schema(&v, context)
            .ok_or_else(|| SchemaResolutionError::new("Could not find matching type in union"))?;

        v.resolve(&inner, context)
    }

    fn resolve_array(self, schema: &Arc<SchemaTree>, context: &mut SchemaParseContext) -> Result<Self, Error> {
        match self {
            Value::Array(items) => Ok(Value::Array(items
                .into_iter()
                .map(|item| item.resolve(schema, context))
                .collect::<Result<Vec<_>, _>>()?)),
            other => Err(SchemaResolutionError::new(format!(
                "Array({:?}) expected, got {:?}",
                schema, other
            )).into()),
        }
    }

    fn resolve_map(self, schema: &Arc<SchemaTree>, context: &mut SchemaParseContext) -> Result<Self, Error> {
        match self {
            Value::Map(items) => Ok(Value::Map(items
                .into_iter()
                .map(|(key, value)| value.resolve(schema, context).map(|value| (key, value)))
                .collect::<Result<HashMap<_, _>, _>>()?)),
            other => Err(SchemaResolutionError::new(format!(
                "Map({:?}) expected, got {:?}",
                schema, other
            )).into()),
        }
    }

    fn resolve_record(self, fields: &[RecordField], context: &mut SchemaParseContext) -> Result<Self, Error> {
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(Error::from(SchemaResolutionError::new(format!(
                "Record({:?}) expected, got {:?}",
                fields, other
            )))),
        }?;

        let new_fields = fields
            .iter()
            .map(|field| {
                let value = match items.remove(&field.name) {
                    Some(value) => value,
                    None => match field.default {
                        Some(ref value) => match *field.schema {
                            SchemaTree::Enum { ref symbols, .. } => {
                                value.clone().avro().resolve_enum(symbols)?
                            },
                            _ => value.clone().avro(),
                        },
                        _ => {
                            return Err(SchemaResolutionError::new(format!(
                                "missing field {} in record",
                                field.name
                            )).into())
                        },
                    },
                };
                value
                    .resolve(&field.schema, context)
                    .map(|value| (field.name.clone(), value))
            }).collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Record(new_fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Name, RecordField, RecordFieldOrder, UnionSchema};

    #[test]
    fn validate() {
        let value_schema_valid = vec![
            (Value::Int(42), SchemaTree::Int, true),
            (Value::Int(42), SchemaTree::Boolean, false),
            (
                Value::Union(Box::new(Value::Null)),
                SchemaTree::Union(UnionSchema::new(vec![Arc::new(SchemaTree::Null), Arc::new(SchemaTree::Int)], &SchemaParseContext::new()).unwrap()),
                true,
            ),
            (
                Value::Union(Box::new(Value::Int(42))),
                SchemaTree::Union(UnionSchema::new(vec![Arc::new(SchemaTree::Null), Arc::new(SchemaTree::Int)], &SchemaParseContext::new()).unwrap()),
                true,
            ),
            (
                Value::Union(Box::new(Value::Null)),
                SchemaTree::Union(UnionSchema::new(vec![Arc::new(SchemaTree::Double), Arc::new(SchemaTree::Int)], &SchemaParseContext::new()).unwrap()),
                false,
            ),
            (
                Value::Union(Box::new(Value::Int(42))),
                SchemaTree::Union(
                    UnionSchema::new(vec![
                        Arc::new(SchemaTree::Null),
                        Arc::new(SchemaTree::Double),
                        Arc::new(SchemaTree::String),
                        Arc::new(SchemaTree::Int),
                    ], &SchemaParseContext::new()).unwrap(),
                ),
                true,
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                SchemaTree::Array(Arc::new(SchemaTree::Long)),
                true,
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                SchemaTree::Array(Arc::new(SchemaTree::Long)),
                false,
            ),
            (Value::Record(vec![]), SchemaTree::Null, false),
        ];

        for (value, schema, valid) in value_schema_valid.into_iter() {
            assert_eq!(valid, value.validate(&Schema::from_tree(schema)));
        }
    }

    #[test]
    fn validate_fixed() {
        let schema = Schema::from_tree(SchemaTree::Fixed {
            size: 4,
            name: Name::new("some_fixed"),
        });

        assert!(Value::Fixed(4, vec![0, 0, 0, 0]).validate(&schema));
        assert!(!Value::Fixed(5, vec![0, 0, 0, 0, 0]).validate(&schema));
    }

    #[test]
    fn validate_enum() {
        let schema = Schema::from_tree(SchemaTree::Enum {
            name: Name::new("some_enum"),
            doc: None,
            symbols: vec![
                "spades".to_string(),
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
            ],
        });

        assert!(Value::Enum(0, "spades".to_string()).validate(&schema));
        assert!(Value::String("spades".to_string()).validate(&schema));

        assert!(!Value::Enum(1, "spades".to_string()).validate(&schema));
        assert!(!Value::String("lorem".to_string()).validate(&schema));

        let other_schema = Schema::from_tree(SchemaTree::Enum {
            name: Name::new("some_other_enum"),
            doc: None,
            symbols: vec![
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
                "spades".to_string(),
            ],
        });

        assert!(!Value::Enum(0, "spades".to_string()).validate(&other_schema));
    }

    #[test]
    fn validate_record() {
        // {
        //    "type": "record",
        //    "fields": [
        //      {"type": "long", "name": "a"},
        //      {"type": "string", "name": "b"}
        //    ]
        // }
        let schema = Schema::from_tree(SchemaTree::Record {
            name: Name::new("some_record"),
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(SchemaTree::Long),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    schema: Arc::new(SchemaTree::String),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                },
            ],
            lookup: HashMap::new(),
        });

        assert!(
            Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("b".to_string(), Value::String("foo".to_string())),
                ("a".to_string(), Value::Long(42i64)),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("a".to_string(), Value::Boolean(false)),
                ("b".to_string(), Value::String("foo".to_string())),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("c".to_string(), Value::String("foo".to_string())),
            ]).validate(&schema)
        );

        assert!(
            !Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Null),
            ]).validate(&schema)
        );
    }
}
