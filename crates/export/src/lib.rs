//! The export pipeline of fastotel: a bounded queue, a worker thread that batches spans, encodes them as OTLP
//! protobuf and sends them over HTTP, compressed, through TLS and with retries.
//!
//! This crate does not depend on PyO3, so nothing in it can take the GIL: the extension module copies a span
//! out of Python into a [`SpanData`] and hands it over, and from then on Python is not involved.

mod encode;
mod pipeline;
mod span;
mod transport;

pub use encode::encode;
pub use pipeline::{Config, Pipeline, headers};
pub use span::{
    Attributes, Context, Event, Link, Resource, Scope, SpanData, SpanKind, Status, StatusCode,
    Value,
};
pub use transport::{Compression, tls};
