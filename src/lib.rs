use std::sync::{Arc, Mutex};
use std::time::Duration;

use fastotel_export::{
    Attributes, Config, Context, Event, Link, Pipeline, Resource, Scope, SpanData, SpanKind,
    Status, StatusCode, Value,
};
use pyo3::exceptions::{PyAttributeError, PyException, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyMapping, PySequence, PyString};

// Resources and scopes copied at most once each; more distinct ones than this are copied with every span
const MAX_CACHED: usize = 64;

/// The native half of `fastotel.OTLPSpanProcessor`.
///
/// `on_end` holds the GIL only to copy the span into a `SpanData`; the pipeline it is pushed to lives in the
/// `fastotel-export` crate, which has no access to Python.
#[pyclass(frozen, module = "fastotel._fastotel")]
struct Processor {
    pipeline: Pipeline,
    resources: Copies<Resource>,
    scopes: Copies<Option<Scope>>,
}

#[pymethods]
impl Processor {
    /// The arguments are checked and defaulted by `OTLPSpanProcessor`, as `BatchSpanProcessor` and
    /// `OTLPSpanExporter` check them; a header HTTP cannot carry raises `ValueError` here, not at every export.
    #[new]
    fn new(
        endpoint: String,
        headers: Vec<(String, String)>,
        timeout: f64,
        max_queue_size: usize,
        schedule_delay_millis: f64,
        max_export_batch_size: usize,
        export_timeout_millis: f64,
    ) -> PyResult<Self> {
        let headers = fastotel_export::headers(
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        )
        .map_err(PyValueError::new_err)?;
        let config = Config {
            headers,
            timeout: seconds(timeout),
            max_queue_size,
            max_export_batch_size,
            schedule_delay: millis(schedule_delay_millis),
            export_timeout: millis(export_timeout_millis),
            ..Config::new(endpoint)
        };
        Ok(Self {
            pipeline: Pipeline::new(config),
            resources: Copies::new(),
            scopes: Copies::new(),
        })
    }

    fn on_end(&self, span: &Bound<'_, PyAny>) -> PyResult<()> {
        match self.copy(span) {
            Ok(Some(span)) => self.pipeline.push(span),
            Ok(None) => {}
            // on_end runs inside the application's span.end(): a span that cannot be copied is dropped rather
            // than raising there, and reporting it comes with #14. KeyboardInterrupt and SystemExit go through
            Err(error) if error.is_instance_of::<PyException>(span.py()) => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    fn force_flush(&self, py: Python<'_>, timeout_millis: u64) -> bool {
        py.detach(|| {
            self.pipeline
                .force_flush(Duration::from_millis(timeout_millis))
        })
    }

    fn shutdown(&self, py: Python<'_>) -> bool {
        py.detach(|| {
            self.pipeline
                .shutdown(self.pipeline.config().export_timeout)
        })
    }

    /// The spans dropped so far because the queue was full; `stats()` exposes it with #14.
    fn dropped_spans(&self) -> u64 {
        self.pipeline.dropped_spans()
    }
}

/// Seconds as the reference exporter takes them, a float. What its requests cannot wait for fails every request
/// at once, as there: NaN, not positive, or more than a socket timeout holds (2^63 ns, about 292 years), infinity
/// included.
fn seconds(seconds: f64) -> Duration {
    // Below the limit, so that adding the timeout to the clock cannot overflow
    const MAX_SECONDS: f64 = 9_223_372_036.0;
    if seconds > 0.0 && seconds < MAX_SECONDS {
        Duration::from_secs_f64(seconds)
    } else {
        Duration::ZERO
    }
}

/// Milliseconds as the SDK takes them, a float: negative or NaN is no time, too many to hold is forever.
fn millis(millis: f64) -> Duration {
    Duration::try_from_secs_f64(millis / 1000.0).unwrap_or(if millis > 0.0 {
        Duration::MAX
    } else {
        Duration::ZERO
    })
}

impl Processor {
    /// The `ReadableSpan` as a `SpanData`, or None when it is not sampled. A string field outside the attributes
    /// that protobuf cannot take (not valid UTF-8, not a str, bytes or None) makes the reference lose the whole
    /// batch; here it fails the copy, so that only this span is dropped.
    fn copy(&self, span: &Bound<'_, PyAny>) -> PyResult<Option<SpanData>> {
        let py = span.py();
        let context = span.getattr(intern!(py, "context"))?;
        // BatchSpanProcessor exports only sampled spans; a span recorded but not sampled ends here too
        let trace_flags: u8 = context.getattr(intern!(py, "trace_flags"))?.extract()?;
        if trace_flags & 1 == 0 {
            return Ok(None);
        }
        let parent = span.getattr(intern!(py, "parent"))?;
        let parent = if parent.is_none() {
            None
        } else {
            Some(copy_context(&parent)?)
        };
        let kind = match span
            .getattr(intern!(py, "kind"))?
            .getattr(intern!(py, "value"))?
            .extract()?
        {
            1 => SpanKind::Server,
            2 => SpanKind::Client,
            3 => SpanKind::Producer,
            4 => SpanKind::Consumer,
            _ => SpanKind::Internal,
        };
        let status = span.getattr(intern!(py, "status"))?;
        let code = match status
            .getattr(intern!(py, "status_code"))?
            .getattr(intern!(py, "value"))?
            .extract()?
        {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        };
        let resource =
            self.resources
                .get_or_copy(&span.getattr(intern!(py, "resource"))?, |resource| {
                    Ok(Resource {
                        attributes: copy_attributes(
                            &resource.getattr(intern!(py, "attributes"))?,
                            RESOURCE_ATTRIBUTES,
                        )?,
                        schema_url: proto_string(&resource.getattr(intern!(py, "schema_url"))?)?,
                    })
                })?;
        let scope = self.scopes.get_or_copy(
            &span.getattr(intern!(py, "instrumentation_scope"))?,
            |scope| {
                // A span created without a tracer has no scope
                if scope.is_none() {
                    return Ok(None);
                }
                Ok(Some(Scope {
                    name: proto_string(&scope.getattr(intern!(py, "name"))?)?,
                    version: proto_string(&scope.getattr(intern!(py, "version"))?)?,
                    // Scopes have attributes from SDK 1.26
                    attributes: match optional_attr(scope, intern!(py, "attributes"))? {
                        Some(attributes) => copy_attributes(&attributes, SCOPE_ATTRIBUTES)?,
                        None => Vec::new(),
                    },
                    schema_url: proto_string(&scope.getattr(intern!(py, "schema_url"))?)?,
                }))
            },
        )?;
        let mut events = Vec::new();
        for event in span.getattr(intern!(py, "events"))?.try_iter()? {
            let event = event?;
            events.push(Event {
                name: proto_string(&event.getattr(intern!(py, "name"))?)?,
                time_unix_nano: event.getattr(intern!(py, "timestamp"))?.extract()?,
                attributes: copy_attributes(
                    &event.getattr(intern!(py, "attributes"))?,
                    EVENT_ATTRIBUTES,
                )?,
                dropped_attributes_count: dropped(&event, intern!(py, "dropped_attributes"))?,
            });
        }
        let mut links = Vec::new();
        for link in span.getattr(intern!(py, "links"))?.try_iter()? {
            let link = link?;
            links.push(Link {
                context: copy_context(&link.getattr(intern!(py, "context"))?)?,
                attributes: copy_attributes(
                    &link.getattr(intern!(py, "attributes"))?,
                    LINK_ATTRIBUTES,
                )?,
                dropped_attributes_count: dropped(&link, intern!(py, "dropped_attributes"))?,
            });
        }
        Ok(Some(SpanData {
            trace_id: context.getattr(intern!(py, "trace_id"))?.extract()?,
            span_id: context.getattr(intern!(py, "span_id"))?.extract()?,
            trace_state: copy_trace_state(&context.getattr(intern!(py, "trace_state"))?)?,
            parent,
            name: proto_string(&span.getattr(intern!(py, "name"))?)?,
            kind,
            start_time_unix_nano: span
                .getattr(intern!(py, "start_time"))?
                .extract::<Option<u64>>()?
                .unwrap_or(0),
            end_time_unix_nano: span
                .getattr(intern!(py, "end_time"))?
                .extract::<Option<u64>>()?
                .unwrap_or(0),
            attributes: copy_attributes(
                &span.getattr(intern!(py, "attributes"))?,
                SPAN_ATTRIBUTES,
            )?,
            dropped_attributes_count: dropped(span, intern!(py, "dropped_attributes"))?,
            events,
            dropped_events_count: dropped(span, intern!(py, "dropped_events"))?,
            links,
            dropped_links_count: dropped(span, intern!(py, "dropped_links"))?,
            status: Status {
                code,
                message: proto_string(&status.getattr(intern!(py, "description"))?)?,
            },
            resource,
            scope,
        }))
    }
}

// Protobuf decoders, upb, C++ and the Python one among them, refuse by default a message nested deeper than 100
// levels. The reference fails on such attribute values in ways that depend on the depth (the attribute or the
// whole batch is lost, or the request cannot be decoded), so fastotel drops the attribute that would nest deeper
const MAX_DEPTH: usize = 100;
// The depth of the KeyValue messages of each kind of attributes in an ExportTraceServiceRequest: request,
// ResourceSpans, Resource, KeyValue, and so on
const RESOURCE_ATTRIBUTES: usize = 4;
const SCOPE_ATTRIBUTES: usize = 5;
const SPAN_ATTRIBUTES: usize = 5;
const EVENT_ATTRIBUTES: usize = 6;
const LINK_ATTRIBUTES: usize = 6;

/// A `SpanContext` of a parent or a link.
fn copy_context(context: &Bound<'_, PyAny>) -> PyResult<Context> {
    let py = context.py();
    Ok(Context {
        trace_id: context.getattr(intern!(py, "trace_id"))?.extract()?,
        span_id: context.getattr(intern!(py, "span_id"))?.extract()?,
        is_remote: context.getattr(intern!(py, "is_remote"))?.is_truthy()?,
    })
}

/// A `TraceState` as the reference joins it: `key=value` pairs in its order, separated by commas.
fn copy_trace_state(trace_state: &Bound<'_, PyAny>) -> PyResult<String> {
    if trace_state.is_none() {
        return Ok(String::new());
    }
    let mut joined = String::new();
    for item in trace_state
        .call_method0(intern!(trace_state.py(), "items"))?
        .try_iter()?
    {
        let (key, value): (String, String) = item?.extract()?;
        if !joined.is_empty() {
            joined.push(',');
        }
        joined.push_str(&key);
        joined.push('=');
        joined.push_str(&value);
    }
    Ok(joined)
}

/// A string field as protobuf takes it from the reference: None is empty and bytes are decoded as UTF-8. The SDK
/// does not check the type of names, so `start_span(None)` reaches here.
fn proto_string(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_none() {
        return Ok(String::new());
    }
    if let Ok(value) = value.cast::<PyBytes>() {
        return Ok(std::str::from_utf8(value.as_bytes())
            .map_err(|error| PyValueError::new_err(error.to_string()))?
            .to_owned());
    }
    Ok(value.cast::<PyString>()?.to_str()?.to_owned())
}

/// An attribute that older SDKs do not have.
fn optional_attr<'py>(
    object: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match object.getattr(name) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.is_instance_of::<PyAttributeError>(object.py()) => Ok(None),
        Err(error) => Err(error),
    }
}

/// A dropped count, 0 when the SDK does not count them.
fn dropped(object: &Bound<'_, PyAny>, name: &Bound<'_, PyString>) -> PyResult<u32> {
    match optional_attr(object, name)? {
        Some(count) => count.extract(),
        None => Ok(0),
    }
}

/// The attributes of a mapping, `depth` being the depth of their KeyValue messages in the request. An attribute
/// the reference cannot encode is left out, as the reference leaves it out: a value of another type than OTLP
/// has, an int beyond int64, a string that is not valid UTF-8, anywhere in the value.
fn copy_attributes(attributes: &Bound<'_, PyAny>, depth: usize) -> PyResult<Attributes> {
    let py = attributes.py();
    let mut copied = Vec::new();
    if attributes.is_none() {
        return Ok(copied);
    }
    for item in attributes.call_method0(intern!(py, "items"))?.try_iter()? {
        let (key, value): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item?.extract()?;
        let attribute =
            proto_string(&key).and_then(|key| Ok((key, copy_value(&value, depth + 1)?)));
        match attribute {
            Ok(attribute) => copied.push(attribute),
            // KeyboardInterrupt and SystemExit go through
            Err(error) if error.is_instance_of::<PyException>(py) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(copied)
}

/// A value as the reference's `_encode_value` takes it, checking the types in the same order; `depth` is the
/// depth of its AnyValue message.
fn copy_value(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err("the value is nested too deeply"));
    }
    if value.is_none() {
        return Ok(Value::Empty);
    }
    if let Ok(value) = value.cast::<PyBool>() {
        return Ok(Value::Bool(value.is_true()));
    }
    if let Ok(value) = value.cast::<PyString>() {
        return Ok(Value::String(value.to_str()?.to_owned()));
    }
    if value.is_instance_of::<PyInt>() {
        return Ok(Value::Int(value.extract()?));
    }
    if let Ok(value) = value.cast::<PyFloat>() {
        return Ok(Value::Double(value.value()));
    }
    if let Ok(value) = value.cast::<PyBytes>() {
        return Ok(Value::Bytes(value.as_bytes().to_vec()));
    }
    let is_sequence = value.cast::<PySequence>().is_ok();
    let is_mapping = !is_sequence && value.cast::<PyMapping>().is_ok();
    // An ArrayValue holding AnyValues, or a KeyValueList of KeyValues holding AnyValues: the AnyValues check
    // their own depth, the list checks it for when it is empty
    if (is_sequence || is_mapping) && depth + 1 > MAX_DEPTH {
        return Err(PyValueError::new_err("the value is nested too deeply"));
    }
    if is_sequence {
        let mut values = Vec::new();
        for item in value.try_iter()? {
            values.push(copy_value(&item?, depth + 2)?);
        }
        return Ok(Value::Array(values));
    }
    if is_mapping {
        let mut values = Vec::new();
        for item in value
            .call_method0(intern!(value.py(), "items"))?
            .try_iter()?
        {
            let (key, item): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item?.extract()?;
            // The reference turns the keys of a nested mapping into strings
            let key = key.str()?.to_str()?.to_owned();
            values.push((key, copy_value(&item, depth + 3)?));
        }
        return Ok(Value::KvList(values));
    }
    Err(PyTypeError::new_err(format!(
        "invalid type {} of an attribute value",
        value.get_type().name()?
    )))
}

/// Copies of resources or scopes by the identity of the Python object, so that the attributes of a resource are
/// not copied again with every span. Both are immutable in the SDK, and the cache holds a reference to each
/// object so that its address is not reused by another one.
struct Copies<T> {
    entries: Mutex<Vec<(Py<PyAny>, Arc<T>)>>,
}

impl<T> Copies<T> {
    fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    fn get_or_copy(
        &self,
        object: &Bound<'_, PyAny>,
        copy: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<T>,
    ) -> PyResult<Arc<T>> {
        if let Some(copied) = self.find(object) {
            return Ok(copied);
        }
        // The lock is never held across a call into Python, which may switch threads while another one waits
        // for it with the GIL
        let copied = Arc::new(copy(object)?);
        let mut entries = self.entries.lock().expect("never poisoned");
        // Another thread may have copied it meanwhile
        if entries.len() < MAX_CACHED && !entries.iter().any(|(cached, _)| cached.is(object)) {
            entries.push((object.clone().unbind(), Arc::clone(&copied)));
        }
        Ok(copied)
    }

    fn find(&self, object: &Bound<'_, PyAny>) -> Option<Arc<T>> {
        let entries = self.entries.lock().expect("never poisoned");
        entries
            .iter()
            .find(|(cached, _)| cached.is(object))
            .map(|(_, copied)| Arc::clone(copied))
    }
}

/// The native part of fastotel, imported as `fastotel._fastotel`.
#[pymodule(gil_used = false)]
mod _fastotel {
    #[pymodule_export]
    use super::Processor;
}
