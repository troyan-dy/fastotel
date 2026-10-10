use std::sync::Arc;

/// An attribute value: every type `AnyValue` of OTLP has, as the reference exporter encodes a Python value.
#[derive(Debug, Clone)]
pub enum Value {
    /// None, an `AnyValue` with no value set
    Empty,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    /// Any sequence, mixed and nested ones too
    Array(Vec<Value>),
    /// A mapping, its keys as strings
    KvList(Attributes),
}

/// Equal when they encode the same: doubles compare by their bits, so that a resource with a NaN in it still
/// groups with its own copy.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Empty, Value::Empty) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::KvList(a), Value::KvList(b)) => a == b,
            _ => false,
        }
    }
}

/// Keys and values in the order of the Python mapping.
pub type Attributes = Vec<(String, Value)>;

/// Whether two attribute sets are the same mapping: the SDK compares resources and scopes as dicts, whatever
/// the order of their keys. Keys are unique in each.
pub fn same_attributes(a: &Attributes, b: &Attributes) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(key, value)| b.iter().any(|(other, v)| key == other && value == v))
}

#[derive(Debug, Clone)]
pub struct Resource {
    pub attributes: Attributes,
    pub schema_url: String,
}

/// Equal as the SDK's `Resource.__eq__` has it, so spans group as the reference groups them.
impl PartialEq for Resource {
    fn eq(&self, other: &Self) -> bool {
        self.schema_url == other.schema_url && same_attributes(&self.attributes, &other.attributes)
    }
}

/// The instrumentation scope, the tracer a span comes from.
#[derive(Debug, Clone)]
pub struct Scope {
    pub name: String,
    pub version: String,
    pub attributes: Attributes,
    pub schema_url: String,
}

/// Equal as the SDK's `InstrumentationScope.__eq__` has it.
impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.version == other.version
            && self.schema_url == other.schema_url
            && same_attributes(&self.attributes, &other.attributes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    Unset,
    Ok,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub code: StatusCode,
    pub message: String,
}

/// The span context of a parent or of a link target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context {
    pub trace_id: u128,
    pub span_id: u64,
    pub is_remote: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub name: String,
    pub time_unix_nano: u64,
    pub attributes: Attributes,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Link {
    pub context: Context,
    pub attributes: Attributes,
    pub dropped_attributes_count: u32,
}

/// A finished span, owned by the pipeline: a copy with nothing from Python in it.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanData {
    pub trace_id: u128,
    pub span_id: u64,
    /// The W3C `tracestate` header value, `key=value` pairs joined by commas
    pub trace_state: String,
    /// Only its span id and whether it is remote are sent, as by the reference
    pub parent: Option<Context>,
    pub name: String,
    pub kind: SpanKind,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: Attributes,
    pub dropped_attributes_count: u32,
    pub events: Vec<Event>,
    pub dropped_events_count: u32,
    pub links: Vec<Link>,
    pub dropped_links_count: u32,
    pub status: Status,
    /// Shared by the spans of one resource and of one scope, which are grouped by identity when encoded
    pub resource: Arc<Resource>,
    /// None for a span created without a tracer, which the reference sends in a scope of its own
    pub scope: Arc<Option<Scope>>,
}
