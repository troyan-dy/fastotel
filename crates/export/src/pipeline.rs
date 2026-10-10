use std::panic::{self, AssertUnwindSafe};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded, select, unbounded};
use prost::Message;
use ureq::http::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use ureq::tls::{RootCerts, TlsConfig};

use crate::encode::encode;
use crate::span::SpanData;
use crate::transport::{Client, Compression, Counters};

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
    /// The limit for the export of a batch, retries included, `OTEL_EXPORTER_OTLP_TIMEOUT`; zero fails every
    /// export
    pub timeout: Duration,
    /// Of the request body, `OTEL_EXPORTER_OTLP_COMPRESSION`
    pub compression: Compression,
    /// Built by [`tls`](crate::tls) from the TLS files; an `http://` endpoint does not use it
    pub tls: TlsConfig,
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
            timeout: Duration::from_secs(10),
            compression: Compression::None,
            // The OS trust store, so corporate CAs work; the reference exporter uses certifi through requests
            tls: TlsConfig::builder()
                .root_certs(RootCerts::PlatformVerifier)
                .build(),
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
                    // Not the value, which may be a secret
                    .map_err(|_| format!("invalid value for header {name:?}"))?,
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
    counters: Arc<Counters>,
}

impl Pipeline {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            worker: OnceLock::new(),
            shut_down: AtomicBool::new(false),
            counters: Arc::default(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Queue a span for export, starting the worker on the first one. Never blocks: when the queue is full, or
    /// once shutdown has taken what is queued, the span is dropped and counted.
    ///
    /// The caller checks [`is_shut_down`](Self::is_shut_down) first, so that a span ended after shutdown is not
    /// even copied; a push racing shutdown is exported or counted all the same.
    pub fn push(&self, span: SpanData) {
        match self
            .worker
            .get_or_init(|| Worker::start(self.config.clone(), Arc::clone(&self.counters)))
        {
            Some(worker) if worker.in_this_process() => {
                if !worker.queue.push(span) {
                    self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            // A child forked after the first span exports nothing until #13
            Some(_) => {}
            // Shut down before the first span, or no thread to export with
            None => self.drop_span(),
        }
    }

    /// Whether `shutdown` has been called: from then on spans are dropped and counted.
    pub fn is_shut_down(&self) -> bool {
        self.shut_down.load(Ordering::Acquire)
    }

    /// Count a span that is not pushed because the pipeline is shut down.
    pub fn drop_span(&self) {
        self.counters.dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// The spans dropped so far because the queue was full or the pipeline shut down.
    pub fn dropped_spans(&self) -> u64 {
        self.counters.dropped.load(Ordering::Relaxed)
    }

    /// The spans of batches dropped so far because their export failed, retries and all, or the worker panicked.
    pub fn failed_spans(&self) -> u64 {
        self.counters.failed.load(Ordering::Relaxed)
    }

    /// The panics caught so far, on the worker or counted by the caller with [`panicked`](Self::panicked).
    pub fn panics(&self) -> u64 {
        self.counters.panics.load(Ordering::Relaxed)
    }

    /// Count a panic caught at the boundary with Python.
    pub fn panicked(&self) {
        self.counters.panics.fetch_add(1, Ordering::Relaxed);
    }

    /// The messages of the panics caught on the worker since the last call, for the caller to log: the worker
    /// cannot reach Python.
    pub fn take_panic_messages(&self) -> Vec<String> {
        self.counters.take_panic_messages()
    }

    /// The spans the collector has rejected in partial successes so far.
    pub fn rejected_spans(&self) -> u64 {
        self.counters.rejected.load(Ordering::Relaxed)
    }

    /// The requests sent again so far.
    pub fn retries(&self) -> u64 {
        self.counters.retries.load(Ordering::Relaxed)
    }

    /// Export every span queued so far; false when that takes longer than `timeout`, after shutdown, as
    /// `BatchSpanProcessor`'s, or when the worker has stopped on a panic.
    pub fn force_flush(&self, timeout: Duration) -> bool {
        if self.is_shut_down() {
            return false;
        }
        match self.worker.get() {
            Some(Some(worker)) if worker.in_this_process() => {
                worker.request(Control::Flush, timeout)
            }
            _ => true,
        }
    }

    /// Export what is queued and stop the worker; spans pushed afterwards are dropped and counted. False when the
    /// export takes longer than `timeout`, and then the worker stops retrying and drops, counted, what it has not
    /// sent.
    pub fn shutdown(&self, timeout: Duration) -> bool {
        if self.shut_down.swap(true, Ordering::AcqRel) {
            return true;
        }
        // Waits for a worker that a concurrent first push is starting, and keeps one from starting later
        match self.worker.get_or_init(|| None) {
            Some(worker) if worker.in_this_process() => {
                let done = worker.request(Control::Shutdown, timeout);
                if !done {
                    // As the reference shuts its exporter down once the wait is over, which ends its retries
                    let _ = worker.abandon.try_send(());
                } else if let Some(thread) = worker
                    .thread
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                {
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

/// The count of a queue that shutdown has emptied for the last time: every push finds it full.
const CLOSED: usize = usize::MAX;

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
    /// False when the queue is full, or closed, and the span is dropped.
    fn push(&self, span: SpanData) -> bool {
        // A compare-and-swap keeps the count from passing the queue size even for a moment, so a dropped span is
        // never counted; SeqCst, so that the worker reads this count once it has the wake-up a full batch sends.
        // A closed queue counts CLOSED, which looks full: no other check on the application's thread
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

    /// Every span queued, and no more from now on: a later push finds the queue full. A span counted before
    /// and not sent yet is waited for, which is no longer than its push takes, since it is counted when nothing
    /// is left for that push to do but to send it.
    fn close(&self) -> Vec<SpanData> {
        let counted = self.queued.swap(CLOSED, Ordering::SeqCst);
        if counted == CLOSED {
            return Vec::new();
        }
        let mut spans: Vec<_> = self.spans.try_iter().collect();
        while spans.len() < counted {
            match self.spans.recv() {
                Ok(span) => spans.push(span),
                // The pushing side is gone, and every push with it
                Err(_) => break,
            }
        }
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
    // Tells the worker to stop retrying, once shutdown has stopped waiting for it
    abandon: Sender<()>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

enum Control {
    Flush(Sender<()>),
    Shutdown(Sender<()>),
}

impl Worker {
    fn start(config: Config, counters: Arc<Counters>) -> Option<Self> {
        let (queue, queued, woken) = queue(config.max_queue_size, config.max_export_batch_size);
        let (control, requests) = unbounded();
        let (abandon, abandoned) = bounded(1);
        let thread = thread::Builder::new()
            .name("fastotel-export".to_owned())
            .spawn(move || {
                let mut client = Client::new(&config, Arc::clone(&counters), abandoned);
                work(&config, &mut client, &counters, &queued, &woken, &requests);
            })
            // Without a thread the spans are dropped and counted; reporting it comes with #14
            .ok()?;
        Some(Self {
            pid: process::id(),
            queue,
            control,
            abandon,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn in_this_process(&self) -> bool {
        self.pid == process::id()
    }

    /// Whether the worker has answered within `timeout`. It never does once it has stopped: after a shutdown
    /// that came first, or on a panic.
    fn request(&self, make: fn(Sender<()>) -> Control, timeout: Duration) -> bool {
        let (done, answer) = bounded(1);
        self.control.send(make(done)).is_ok() && answer.recv_timeout(timeout).is_ok()
    }
}

/// Where the worker sends its batches: the HTTP client, or a test's.
trait Exporter {
    /// Send a batch, counting what is lost.
    fn export(&mut self, spans: Vec<SpanData>);
}

impl Exporter for Client {
    fn export(&mut self, spans: Vec<SpanData>) {
        let batch_size = spans.len();
        // A batch that fails after its retries is dropped and counted; logging it comes with #14
        self.send(encode(spans).encode_to_vec(), batch_size);
    }
}

/// The worker thread: [`run`], and if it panics outside a batch, the queue closed and what is in it counted as
/// failed, so that spans pushed afterwards are dropped and counted rather than left in a queue nobody takes from.
fn work(
    config: &Config,
    exporter: &mut impl Exporter,
    counters: &Counters,
    queued: &Queued,
    woken: &Receiver<()>,
    requests: &Receiver<Control>,
) {
    let ran = panic::catch_unwind(AssertUnwindSafe(|| {
        run(config, exporter, counters, queued, woken, requests);
    }));
    if let Err(payload) = ran {
        counters.worker_panicked(&*payload);
        let lost = queued.close().len();
        counters.failed.fetch_add(lost as u64, Ordering::Relaxed);
    }
}

fn run(
    config: &Config,
    exporter: &mut impl Exporter,
    counters: &Counters,
    queued: &Queued,
    woken: &Receiver<()>,
    requests: &Receiver<Control>,
) {
    let batch_size = config.max_export_batch_size;
    let mut export = |count| export(exporter, counters, config, queued, count);
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
                    export(queued.len());
                    return;
                }
                // A full batch leaves at once, and the delay starts over as after any export
                if queued.len() >= batch_size {
                    // Until less than a batch is left, or what is left is still being pushed
                    while queued.len() >= batch_size && export(batch_size) > 0 {}
                    next_export = after_delay();
                }
            },
            recv(requests) -> request => {
                match request {
                    Ok(Control::Flush(done)) => {
                        // Everything pushed before the request is counted by now
                        export(queued.len());
                        let _ = done.send(());
                    }
                    Ok(Control::Shutdown(done)) => {
                        // Pushes from now on are dropped and counted, so that none is left in the queue after the
                        // worker is gone; what is queued leaves now, or is counted as failed once abandoned
                        send_batches(exporter, counters, config, queued.close());
                        let _ = done.send(());
                        return;
                    }
                    Err(_) => {
                        export(queued.len());
                        return;
                    }
                }
            },
            default(wait) => {
                export(queued.len());
                next_export = after_delay();
            },
        }
    }
}

/// Send up to `count` queued spans in requests of at most the batch size; the number taken from the queue.
fn export(
    exporter: &mut impl Exporter,
    counters: &Counters,
    config: &Config,
    queued: &Queued,
    count: usize,
) -> usize {
    let mut taken = 0;
    while taken < count {
        let spans = queued.take((count - taken).min(config.max_export_batch_size));
        if spans.is_empty() {
            break;
        }
        taken += spans.len();
        send(exporter, counters, spans);
    }
    taken
}

/// Send `spans` in requests of at most the batch size.
fn send_batches(
    exporter: &mut impl Exporter,
    counters: &Counters,
    config: &Config,
    mut spans: Vec<SpanData>,
) {
    while !spans.is_empty() {
        let rest = spans.split_off(spans.len().min(config.max_export_batch_size));
        send(exporter, counters, spans);
        spans = rest;
    }
}

/// Export a batch; a panic loses the batch, counted, and the worker goes on with the next one.
fn send(exporter: &mut impl Exporter, counters: &Counters, spans: Vec<SpanData>) {
    let batch_size = spans.len() as u64;
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| exporter.export(spans))) {
        counters.worker_panicked(&*payload);
        counters.failed.fetch_add(batch_size, Ordering::Relaxed);
    }
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
    fn close_takes_every_counted_span_and_refuses_later_pushes() {
        let (queue, queued, _woken) = queue(10, 5);
        queue.push(span());
        // A push that has counted its span and sends it a little later: close waits for it
        queue.queued.fetch_add(1, Ordering::SeqCst);
        let late = queue.spans.clone();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            late.send(span()).unwrap();
        });
        assert_eq!(queued.close().len(), 2);
        sender.join().unwrap();
        assert!(!queue.push(span()));
        assert!(queued.close().is_empty());
    }

    /// Records the batches it is given, and panics on those of `panic_on` spans.
    struct Recorder {
        batches: Sender<usize>,
        panic_on: usize,
    }

    impl Exporter for Recorder {
        fn export(&mut self, spans: Vec<SpanData>) {
            assert_ne!(spans.len(), self.panic_on, "a bug in the exporter");
            self.batches.send(spans.len()).unwrap();
        }
    }

    /// A worker exporting to a `Recorder`, as `Worker::start` runs one.
    fn worker(
        config: Config,
        panic_on: usize,
    ) -> (
        Queue,
        Sender<Control>,
        Arc<Counters>,
        Receiver<usize>,
        JoinHandle<()>,
    ) {
        let (queue, queued, woken) = queue(config.max_queue_size, config.max_export_batch_size);
        let (control, requests) = unbounded();
        let (batches, recorded) = unbounded();
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let thread = thread::spawn(move || {
            let mut recorder = Recorder { batches, panic_on };
            work(
                &config,
                &mut recorder,
                &worker_counters,
                &queued,
                &woken,
                &requests,
            );
        });
        (queue, control, counters, recorded, thread)
    }

    fn ask(control: &Sender<Control>, make: fn(Sender<()>) -> Control) -> bool {
        let (done, answer) = bounded(1);
        control.send(make(done)).is_ok() && answer.recv_timeout(Duration::from_secs(10)).is_ok()
    }

    #[test]
    fn a_panic_in_an_export_loses_that_batch_counted_and_the_worker_goes_on() {
        let config = Config {
            max_export_batch_size: 3,
            schedule_delay: Duration::from_secs(3600),
            ..Config::new("http://localhost:4318/v1/traces")
        };
        let (queue, control, counters, recorded, thread) = worker(config, 2);
        (0..2).for_each(|_| assert!(queue.push(span())));
        assert!(ask(&control, Control::Flush));
        assert_eq!(counters.panics.load(Ordering::Relaxed), 1);
        assert_eq!(counters.failed.load(Ordering::Relaxed), 2);
        assert_eq!(
            counters.take_panic_messages(),
            ["assertion `left != right` failed: a bug in the exporter\n  left: 2\n right: 2"]
        );

        (0..4).for_each(|_| assert!(queue.push(span())));
        assert!(ask(&control, Control::Shutdown));
        thread.join().unwrap();
        assert_eq!(recorded.try_iter().collect::<Vec<_>>(), [3, 1]);
        assert_eq!(counters.panics.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn shutdown_exports_what_is_queued_and_closes_the_queue() {
        let config = Config {
            max_export_batch_size: 2,
            schedule_delay: Duration::from_secs(3600),
            ..Config::new("http://localhost:4318/v1/traces")
        };
        let (queue, control, _counters, recorded, thread) = worker(config, 0);
        assert!(queue.push(span()));
        // Counted before the shutdown and sent after it, as by a push that shutdown overtakes
        queue.queued.fetch_add(1, Ordering::SeqCst);
        let (late, sent) = (queue.spans.clone(), bounded::<()>(0));
        let sender = thread::spawn(move || {
            sent.1.recv().unwrap();
            late.send(span()).unwrap();
        });
        let (done, answer) = bounded(1);
        control.send(Control::Shutdown(done)).unwrap();
        thread::sleep(Duration::from_millis(50));
        sent.0.send(()).unwrap();
        assert!(answer.recv_timeout(Duration::from_secs(10)).is_ok());
        sender.join().unwrap();
        thread.join().unwrap();

        assert_eq!(recorded.try_iter().collect::<Vec<_>>(), [2]);
        assert!(!queue.push(span()));
        assert!(!ask(&control, Control::Flush));
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
