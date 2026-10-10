use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded, select, unbounded};
use prost::Message;
use ureq::Agent;
use ureq::http::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use ureq::tls::{RootCerts, TlsConfig};

use crate::encode::encode;
use crate::span::SpanData;

/// How the pipeline batches and where it sends; the defaults are the SDK's.
#[derive(Debug, Clone)]
pub struct Config {
    /// The URL spans are posted to, `/v1/traces` included, as the `endpoint` of the reference exporter
    pub endpoint: String,
    /// Sent with every request, as the reference exporter's `headers`
    pub headers: HeaderMap,
    /// The spans waiting for export, the batch being sent excluded, as `BatchSpanProcessor` counts them
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub schedule_delay: Duration,
    /// How long `shutdown` waits for the last export, `OTEL_BSP_EXPORT_TIMEOUT`
    pub export_timeout: Duration,
    /// The limit for one export request, `OTEL_EXPORTER_OTLP_TIMEOUT`; None for none
    pub timeout: Option<Duration>,
}

impl Config {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            headers: HeaderMap::from_iter([(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            )]),
            max_queue_size: 2048,
            max_export_batch_size: 512,
            schedule_delay: Duration::from_millis(5000),
            export_timeout: Duration::from_millis(30000),
            timeout: Some(Duration::from_secs(10)),
        }
    }
}

/// Headers from names and values; the error names the first one HTTP cannot carry.
///
/// A value may hold any UTF-8 but no control characters: the reference's percent-decoding can produce either.
pub fn headers<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<HeaderMap, String> {
    pairs
        .into_iter()
        .map(|(name, value)| {
            Ok((
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| format!("invalid header name {name:?}"))?,
                HeaderValue::from_bytes(value.as_bytes())
                    .map_err(|_| format!("invalid value for header {name:?}: {value:?}"))?,
            ))
        })
        .collect()
}

/// The queue and the worker thread that exports from it.
///
/// Nothing starts before the first span: a process that forks after creating the pipeline has no thread to lose.
pub struct Pipeline {
    config: Config,
    // None once shut down before the first span, or when the thread could not be started
    worker: OnceLock<Option<Worker>>,
    shut_down: AtomicBool,
    dropped: AtomicU64,
}

impl Pipeline {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            worker: OnceLock::new(),
            shut_down: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Queue a span for export, starting the worker on the first one. Never blocks: when the queue is full the
    /// span is dropped and counted.
    pub fn push(&self, span: SpanData) {
        if self.shut_down.load(Ordering::Acquire) {
            return;
        }
        if let Some(worker) = self
            .worker
            .get_or_init(|| Worker::start(self.config.clone()))
            && worker.in_this_process()
            && !worker.queue.push(span)
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The spans dropped so far because the queue was full.
    pub fn dropped_spans(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Export every span queued so far; false when that takes longer than `timeout`.
    pub fn force_flush(&self, timeout: Duration) -> bool {
        match self.worker.get() {
            Some(Some(worker)) if worker.in_this_process() => {
                worker.request(Control::Flush, timeout)
            }
            _ => true,
        }
    }

    /// Export what is queued and stop the worker; spans pushed afterwards are ignored. False when the export
    /// takes longer than `timeout`, and then the worker finishes it on its own.
    pub fn shutdown(&self, timeout: Duration) -> bool {
        if self.shut_down.swap(true, Ordering::AcqRel) {
            return true;
        }
        // Waits for a worker that a concurrent first push is starting, and keeps one from starting later
        match self.worker.get_or_init(|| None) {
            Some(worker) if worker.in_this_process() => {
                let done = worker.request(Control::Shutdown, timeout);
                if done && let Some(thread) = worker.thread.lock().expect("never poisoned").take() {
                    // The worker has answered and is returning
                    let _ = thread.join();
                }
                done
            }
            _ => true,
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        if let Some(Some(worker)) = self.worker.take()
            && !worker.in_this_process()
        {
            // Dropping the channels would wake the worker of the parent, which the child does not have
            std::mem::forget(worker);
        }
    }
}

/// The pushing side of the queue.
///
/// The spans travel through an unbounded channel, which allocates as it fills, and `queued` bounds them: a
/// bounded channel would allocate every slot of a large `max_queue_size` up front. The worker is woken only when
/// a batch is full, as `BatchSpanProcessor` wakes its own, so that `on_end` does not pay for waking a thread
/// with every span; otherwise it wakes on its timer.
struct Queue {
    spans: Sender<SpanData>,
    queued: Arc<AtomicUsize>,
    max_queue_size: usize,
    max_export_batch_size: usize,
    batch_full: Sender<()>,
}

impl Queue {
    /// False when the queue is full and the span is dropped.
    fn push(&self, span: SpanData) -> bool {
        // A compare-and-swap keeps the count from passing the queue size even for a moment, so a dropped span is
        // never counted; SeqCst, so that the worker reads this count once it has the wake-up a full batch sends
        let mut queued = self.queued.load(Ordering::SeqCst);
        loop {
            if queued >= self.max_queue_size {
                return false;
            }
            match self.queued.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(now) => queued = now,
            }
        }
        // Never blocks, and fails only once the worker has stopped, which happens after a shutdown
        let _ = self.spans.send(span);
        if queued + 1 == self.max_export_batch_size {
            // A wake-up already pending is enough
            let _ = self.batch_full.try_send(());
        }
        true
    }
}

/// The worker's side of the queue: `queued` counts the spans pushed into `spans` and not taken out yet.
struct Queued {
    spans: Receiver<SpanData>,
    queued: Arc<AtomicUsize>,
}

impl Queued {
    fn len(&self) -> usize {
        self.queued.load(Ordering::SeqCst)
    }

    /// Up to `count` spans. Fewer when a push has counted its span but not sent it yet: waiting for it would
    /// hang if that thread is preempted, and the span leaves with the next export.
    fn take(&self, count: usize) -> Vec<SpanData> {
        let spans: Vec<_> = self.spans.try_iter().take(count).collect();
        self.queued.fetch_sub(spans.len(), Ordering::SeqCst);
        spans
    }
}

/// The two sides of a queue, and the channel that wakes the worker when a batch is full.
fn queue(max_queue_size: usize, max_export_batch_size: usize) -> (Queue, Queued, Receiver<()>) {
    let (spans, receiver) = unbounded();
    let queued = Arc::new(AtomicUsize::new(0));
    let (batch_full, woken) = bounded(1);
    let queue = Queue {
        spans,
        queued: Arc::clone(&queued),
        max_queue_size,
        max_export_batch_size,
        batch_full,
    };
    let queued = Queued {
        spans: receiver,
        queued,
    };
    (queue, queued, woken)
}

struct Worker {
    // A child forked after the first span inherits the channels but not the thread. Until #13 restarts the
    // worker there, the child leaves them alone: waking the parent's worker traps on macOS
    pid: u32,
    queue: Queue,
    control: Sender<Control>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

enum Control {
    Flush(Sender<()>),
    Shutdown(Sender<()>),
}

impl Worker {
    fn start(config: Config) -> Option<Self> {
        let (queue, queued, woken) = queue(config.max_queue_size, config.max_export_batch_size);
        let (control, requests) = unbounded();
        let thread = thread::Builder::new()
            .name("fastotel-export".to_owned())
            .spawn(move || run(&config, &queued, &woken, &requests))
            // Without a thread the spans are dropped; reporting it comes with #14
            .ok()?;
        Some(Self {
            pid: process::id(),
            queue,
            control,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn in_this_process(&self) -> bool {
        self.pid == process::id()
    }

    fn request(&self, make: fn(Sender<()>) -> Control, timeout: Duration) -> bool {
        let (done, answer) = bounded(1);
        if self.control.send(make(done)).is_err() {
            // The worker has stopped and exported everything on its way out
            return true;
        }
        answer.recv_timeout(timeout).is_ok()
    }
}

fn run(config: &Config, queued: &Queued, woken: &Receiver<()>, requests: &Receiver<Control>) {
    let agent: Agent = Agent::config_builder()
        .timeout_global(config.timeout)
        // A status is not an error of the transport: the export reads it
        .http_status_as_error(false)
        // The OS trust store, so corporate CAs work; the reference exporter uses certifi through requests
        .tls_config(
            TlsConfig::builder()
                .root_certs(RootCerts::PlatformVerifier)
                .build(),
        )
        .build()
        .into();
    let batch_size = config.max_export_batch_size;
    // None when the delay is too long to tell the time it ends: then only full batches and flushes export
    let after_delay = || Instant::now().checked_add(config.schedule_delay);
    let mut next_export = after_delay();
    loop {
        let wait = next_export.map_or(Duration::MAX, |at| {
            at.saturating_duration_since(Instant::now())
        });
        select! {
            recv(woken) -> woken => {
                if woken.is_err() {
                    // The pipeline is dropped
                    export(&agent, config, queued, queued.len());
                    return;
                }
                // A full batch leaves at once, and the delay starts over as after any export
                if queued.len() >= batch_size {
                    // Until less than a batch is left, or what is left is still being pushed
                    while queued.len() >= batch_size && export(&agent, config, queued, batch_size) > 0 {}
                    next_export = after_delay();
                }
            },
            recv(requests) -> request => {
                // Everything pushed before the request is counted by now
                export(&agent, config, queued, queued.len());
                match request {
                    Ok(Control::Flush(done)) => {
                        let _ = done.send(());
                    }
                    Ok(Control::Shutdown(done)) => {
                        let _ = done.send(());
                        return;
                    }
                    Err(_) => return,
                }
            },
            default(wait) => {
                export(&agent, config, queued, queued.len());
                next_export = after_delay();
            },
        }
    }
}

/// Send up to `count` queued spans in requests of at most the batch size; the number taken from the queue.
fn export(agent: &Agent, config: &Config, queued: &Queued, count: usize) -> usize {
    let mut taken = 0;
    while taken < count {
        let spans = queued.take((count - taken).min(config.max_export_batch_size));
        if spans.is_empty() {
            break;
        }
        taken += spans.len();
        let body = encode(spans).encode_to_vec();
        let mut request = agent.post(&config.endpoint);
        if let Some(headers) = request.headers_mut() {
            headers.extend(config.headers.clone());
        }
        let sent = request.send(&body[..]);
        // A failed export drops the batch: retries come with #11, counting and logging with #14
        if let Ok(response) = sent {
            // Reading the body to the end returns the connection to the pool
            let _ = response.into_body().read_to_vec();
        }
    }
    taken
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{Resource, SpanKind, Status, StatusCode};

    fn span() -> SpanData {
        SpanData {
            trace_id: 1,
            span_id: 1,
            trace_state: String::new(),
            parent: None,
            name: "span".to_owned(),
            kind: SpanKind::Internal,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Status {
                code: StatusCode::Unset,
                message: String::new(),
            },
            resource: Arc::new(Resource {
                attributes: vec![],
                schema_url: String::new(),
            }),
            scope: Arc::new(None),
        }
    }

    #[test]
    fn headers_refuse_what_http_cannot_carry() {
        let parsed = headers([("api-key", "secret"), ("x-name", "café")]).unwrap();
        assert_eq!(parsed["api-key"], "secret");
        assert_eq!(parsed["x-name"].as_bytes(), "café".as_bytes());
        assert!(headers([("bad name", "x")]).is_err());
        assert!(headers([("", "x")]).is_err());
        assert!(headers([("x", "a\r\nb")]).is_err());
        assert!(headers([("x", "a\0b")]).is_err());
    }

    #[test]
    fn a_full_queue_refuses_spans_until_the_worker_takes_some() {
        let (queue, queued, _woken) = queue(3, 2);
        assert!((0..3).all(|_| queue.push(span())));
        assert!(!queue.push(span()));
        assert_eq!(queued.len(), 3);

        assert_eq!(queued.take(2).len(), 2);
        assert_eq!(queued.len(), 1);
        assert!(queue.push(span()) && queue.push(span()));
        assert!(!queue.push(span()));
        assert_eq!(queued.len(), 3);
    }

    #[test]
    fn the_push_that_fills_a_batch_wakes_the_worker() {
        let (queue, queued, woken) = queue(10, 3);
        queue.push(span());
        queue.push(span());
        assert!(woken.try_recv().is_err());
        queue.push(span());
        assert!(woken.try_recv().is_ok());
        // Not again until the count comes back to a full batch
        queue.push(span());
        assert!(woken.try_recv().is_err());
        queued.take(4);
        (0..3).for_each(|_| {
            queue.push(span());
        });
        assert!(woken.try_recv().is_ok());
    }

    #[test]
    fn take_returns_what_is_sent_without_waiting_for_a_counted_span() {
        // A push between counting its span and sending it: the worker takes what is there and does not hang
        let (queue, queued, _woken) = queue(10, 5);
        queue.push(span());
        queue.queued.fetch_add(1, Ordering::SeqCst);
        assert_eq!(queued.take(queued.len()).len(), 1);
        assert_eq!(queued.len(), 1);
    }
}
