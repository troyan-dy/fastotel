use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded, select, unbounded};
use prost::Message;
use ureq::Agent;
use ureq::tls::{RootCerts, TlsConfig};

use crate::encode::encode;
use crate::span::SpanData;

/// How the pipeline batches and where it sends; the defaults are the SDK's.
#[derive(Debug, Clone)]
pub struct Config {
    /// The URL spans are posted to, `/v1/traces` included, as the `endpoint` of the reference exporter
    pub endpoint: String,
    /// The spans waiting for export, the batch being sent excluded, as `BatchSpanProcessor` counts them
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub schedule_delay: Duration,
    /// How long `shutdown` waits for the last export, `OTEL_BSP_EXPORT_TIMEOUT`
    pub export_timeout: Duration,
    /// The limit for one export request
    pub timeout: Duration,
}

impl Config {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            max_queue_size: 2048,
            max_export_batch_size: 512,
            schedule_delay: Duration::from_millis(5000),
            export_timeout: Duration::from_millis(30000),
            timeout: Duration::from_secs(10),
        }
    }
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
        let queued = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        if queued > self.max_queue_size {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        // Never blocks, and fails only once the worker has stopped, which happens after a shutdown
        let _ = self.spans.send(span);
        if queued == self.max_export_batch_size {
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
        self.queued.load(Ordering::Relaxed)
    }

    /// The next `count` spans, `count` at most `len()`.
    fn take(&self, count: usize) -> Vec<SpanData> {
        // A counted span is in the channel or about to be: its push has added to the count and is sending it
        let spans: Vec<_> = self.spans.iter().take(count).collect();
        self.queued.fetch_sub(spans.len(), Ordering::Relaxed);
        spans
    }
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
        let (spans, receiver) = unbounded();
        let queued = Arc::new(AtomicUsize::new(0));
        let (batch_full, woken) = bounded(1);
        let (control, requests) = unbounded();
        let queue = Queue {
            spans,
            queued: Arc::clone(&queued),
            max_queue_size: config.max_queue_size,
            max_export_batch_size: config.max_export_batch_size,
            batch_full,
        };
        let queued = Queued {
            spans: receiver,
            queued,
        };
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
        .timeout_global(Some(config.timeout))
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
                    while queued.len() >= batch_size {
                        export(&agent, config, queued, batch_size);
                    }
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

/// Send the next `count` queued spans in requests of at most the batch size.
fn export(agent: &Agent, config: &Config, queued: &Queued, mut count: usize) {
    while count > 0 {
        let spans = queued.take(count.min(config.max_export_batch_size));
        count -= spans.len();
        if spans.is_empty() {
            return;
        }
        let body = encode(spans).encode_to_vec();
        let sent = agent
            .post(&config.endpoint)
            .header("Content-Type", "application/x-protobuf")
            .send(&body[..]);
        // A failed export drops the batch: retries come with #11, counting and logging with #14
        if let Ok(response) = sent {
            // Reading the body to the end returns the connection to the pool
            let _ = response.into_body().read_to_vec();
        }
    }
}
