use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
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
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub schedule_delay: Duration,
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
}

impl Pipeline {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            worker: OnceLock::new(),
            shut_down: AtomicBool::new(false),
        }
    }

    /// Queue a span for export, starting the worker on the first one. Never blocks: when the queue is full the
    /// span is dropped.
    pub fn push(&self, span: SpanData) {
        if self.shut_down.load(Ordering::Acquire) {
            return;
        }
        if let Some(worker) = self
            .worker
            .get_or_init(|| Worker::start(self.config.clone()))
            && worker.in_this_process()
        {
            // A full queue drops the span; counting it comes with #9
            let _ = worker.spans.try_send(span);
        }
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

struct Worker {
    // A child forked after the first span inherits the channels but not the thread. Until #13 restarts the
    // worker there, the child leaves them alone: waking the parent's worker traps on macOS
    pid: u32,
    spans: Sender<SpanData>,
    control: Sender<Control>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

enum Control {
    Flush(Sender<()>),
    Shutdown(Sender<()>),
}

impl Worker {
    fn start(config: Config) -> Option<Self> {
        let (spans, queued) = bounded(config.max_queue_size);
        let (control, requests) = unbounded();
        let thread = thread::Builder::new()
            .name("fastotel-export".to_owned())
            .spawn(move || run(&config, &queued, &requests))
            // Without a thread the spans are dropped; reporting it comes with #14
            .ok()?;
        Some(Self {
            pid: process::id(),
            spans,
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

fn run(config: &Config, queued: &Receiver<SpanData>, requests: &Receiver<Control>) {
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
    let mut batch = Vec::with_capacity(config.max_export_batch_size);
    let mut next_export = Instant::now() + config.schedule_delay;
    loop {
        select! {
            recv(queued) -> span => match span {
                Ok(span) => {
                    batch.push(span);
                    if batch.len() >= config.max_export_batch_size {
                        export(&agent, config, &mut batch);
                        next_export = Instant::now() + config.schedule_delay;
                    }
                }
                // The pipeline is dropped and the queue is empty
                Err(_) => {
                    export(&agent, config, &mut batch);
                    return;
                }
            },
            recv(requests) -> request => {
                batch.extend(queued.try_iter());
                export(&agent, config, &mut batch);
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
            default(next_export.saturating_duration_since(Instant::now())) => {
                export(&agent, config, &mut batch);
                next_export = Instant::now() + config.schedule_delay;
            },
        }
    }
}

/// Send `batch` in requests of at most the batch size, leaving it empty.
fn export(agent: &Agent, config: &Config, batch: &mut Vec<SpanData>) {
    while !batch.is_empty() {
        let rest = batch.split_off(batch.len().min(config.max_export_batch_size));
        let spans = std::mem::replace(batch, rest);
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
