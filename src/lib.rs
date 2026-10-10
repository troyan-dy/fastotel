use std::sync::{Arc, Mutex};
use std::time::Duration;

use fastotel_export::{Attributes, Config, Pipeline, Resource, Scope, SpanData, SpanKind};
use pyo3::exceptions::PyException;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyString;

// The limit shutdown() waits for the last export, the default of OTEL_BSP_EXPORT_TIMEOUT
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
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
    scopes: Copies<Scope>,
}

#[pymethods]
impl Processor {
    #[new]
    fn new(endpoint: String) -> Self {
        Self {
            pipeline: Pipeline::new(Config::new(endpoint)),
            resources: Copies::new(),
            scopes: Copies::new(),
        }
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
        py.detach(|| self.pipeline.shutdown(SHUTDOWN_TIMEOUT))
    }
}

impl Processor {
    /// The `ReadableSpan` as a `SpanData`, or None when it is not sampled.
    fn copy(&self, span: &Bound<'_, PyAny>) -> PyResult<Option<SpanData>> {
        let py = span.py();
        let context = span.getattr(intern!(py, "context"))?;
        // BatchSpanProcessor exports only sampled spans; a span recorded but not sampled ends here too
        let trace_flags: u8 = context.getattr(intern!(py, "trace_flags"))?.extract()?;
        if trace_flags & 1 == 0 {
            return Ok(None);
        }
        let parent = span.getattr(intern!(py, "parent"))?;
        let parent_span_id = if parent.is_none() {
            None
        } else {
            Some(parent.getattr(intern!(py, "span_id"))?.extract()?)
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
        let resource =
            self.resources
                .get_or_copy(&span.getattr(intern!(py, "resource"))?, |resource| {
                    Ok(Resource {
                        attributes: copy_attributes(&resource.getattr(intern!(py, "attributes"))?)?,
                    })
                })?;
        let scope = self.scopes.get_or_copy(
            &span.getattr(intern!(py, "instrumentation_scope"))?,
            |scope| {
                // A span created without a tracer has no scope
                if scope.is_none() {
                    return Ok(Scope {
                        name: String::new(),
                        version: String::new(),
                    });
                }
                Ok(Scope {
                    name: scope.getattr(intern!(py, "name"))?.extract()?,
                    version: scope
                        .getattr(intern!(py, "version"))?
                        .extract::<Option<String>>()?
                        .unwrap_or_default(),
                })
            },
        )?;
        Ok(Some(SpanData {
            trace_id: context.getattr(intern!(py, "trace_id"))?.extract()?,
            span_id: context.getattr(intern!(py, "span_id"))?.extract()?,
            parent_span_id,
            name: span.getattr(intern!(py, "name"))?.extract()?,
            kind,
            start_time_unix_nano: span
                .getattr(intern!(py, "start_time"))?
                .extract::<Option<u64>>()?
                .unwrap_or(0),
            end_time_unix_nano: span
                .getattr(intern!(py, "end_time"))?
                .extract::<Option<u64>>()?
                .unwrap_or(0),
            attributes: copy_attributes(&span.getattr(intern!(py, "attributes"))?)?,
            resource,
            scope,
        }))
    }
}

/// The string attributes of a mapping; values of the other types are left out until #8.
fn copy_attributes(attributes: &Bound<'_, PyAny>) -> PyResult<Attributes> {
    let mut copied = Vec::new();
    for item in attributes
        .call_method0(intern!(attributes.py(), "items"))?
        .try_iter()?
    {
        let (key, value): (Bound<'_, PyString>, Bound<'_, PyAny>) = item?.extract()?;
        if let Ok(value) = value.cast::<PyString>() {
            copied.push((
                key.to_string_lossy().into_owned(),
                value.to_string_lossy().into_owned(),
            ));
        }
    }
    Ok(copied)
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
