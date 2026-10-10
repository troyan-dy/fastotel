use std::sync::Arc;

/// Keys and values of string attributes; attributes of the other types come with #8.
pub type Attributes = Vec<(String, String)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource {
    pub attributes: Attributes,
}

/// The instrumentation scope, the tracer a span comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// A finished span, owned by the pipeline: a copy with nothing from Python in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanData {
    pub trace_id: u128,
    pub span_id: u64,
    pub parent_span_id: Option<u64>,
    pub name: String,
    pub kind: SpanKind,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: Attributes,
    /// Shared by the spans of one resource and of one scope, which are grouped by identity when encoded
    pub resource: Arc<Resource>,
    pub scope: Arc<Scope>,
}
