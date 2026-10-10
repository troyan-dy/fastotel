//! Sending a batch: compression, TLS and retries, as the OTLP/HTTP specification asks and as the reference
//! exporter (`_OTLPHTTPClient` of opentelemetry-exporter-otlp-common 1.45) does them.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use flate2::write::{GzEncoder, ZlibEncoder};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
use prost::Message;
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;
use ureq::http::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER};
use ureq::tls::{Certificate, ClientCert, PemItem, PrivateKey, RootCerts, TlsConfig};
use ureq::{Agent, Error};

use crate::pipeline::Config;

/// Attempts at one batch, the first included, as the reference makes them.
const MAX_ATTEMPTS: u32 = 6;
/// The backoff before retry n is 2^n seconds, give or take this fraction.
const JITTER: f64 = 0.2;
/// The statuses the OTLP/HTTP specification asks to retry; every other 4xx and 5xx must not be.
const RETRYABLE: [u16; 4] = [429, 502, 503, 504];

/// How the request body is compressed, `OTEL_EXPORTER_OTLP_COMPRESSION`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    /// zlib, as the reference's `zlib.compress`
    Deflate,
    Gzip,
}

impl Compression {
    /// The compression of a name the reference accepts: none, deflate or gzip.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Self::None),
            "deflate" => Some(Self::Deflate),
            "gzip" => Some(Self::Gzip),
            _ => None,
        }
    }

    fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Deflate => Some("deflate"),
            Self::Gzip => Some("gzip"),
        }
    }

    /// At flate2's default level, 6: the reference's gzip takes Python's 9, which costs more CPU for a few
    /// percent of size.
    fn compress(self, body: Vec<u8>) -> Vec<u8> {
        let level = flate2::Compression::default();
        // Writing into a Vec does not fail
        match self {
            Self::None => body,
            Self::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), level);
                encoder.write_all(&body).expect("writes to a Vec");
                encoder.finish().expect("writes to a Vec")
            }
            Self::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), level);
                encoder.write_all(&body).expect("writes to a Vec");
                encoder.finish().expect("writes to a Vec")
            }
        }
    }
}

/// The TLS settings from the contents of the reference's TLS files, checked now so that the worker never meets
/// a file it cannot use: the error names the file.
///
/// Without `certificate` the server is verified against the OS trust store, so corporate CAs work; with it,
/// against its certificates only, as requests does with `verify`. `client_certificate` holds the chain and, when
/// there is no `client_key`, the key too.
pub fn tls(
    certificate: Option<&[u8]>,
    client_certificate: Option<&[u8]>,
    client_key: Option<&[u8]>,
) -> Result<TlsConfig, String> {
    let roots = match certificate {
        None => RootCerts::PlatformVerifier,
        Some(pem) => {
            let certificates = certificates(pem, "certificate_file")?;
            // rustls skips the certificates it cannot parse, and with none left would trust nothing
            let (added, _) = RootCertStore::empty().add_parsable_certificates(
                certificates
                    .iter()
                    .map(|certificate| CertificateDer::from(certificate.der())),
            );
            if added == 0 {
                return Err("certificate_file holds no usable certificate".to_owned());
            }
            RootCerts::from(certificates)
        }
    };
    let client_cert = match client_certificate {
        None => None,
        Some(pem) => {
            let chain = certificates(pem, "client_certificate_file")?;
            let key_file = if client_key.is_some() {
                "client_key_file"
            } else {
                "client_certificate_file"
            };
            let key_pem = client_key.unwrap_or(pem);
            let key = PrivateKey::from_pem(key_pem)
                .map_err(|_| format!("{key_file} holds no private key"))?;
            check_client_cert(&chain, key_pem)?;
            Some(ClientCert::new_with_certs(&chain, key))
        }
    };
    Ok(TlsConfig::builder()
        .root_certs(roots)
        .client_cert(client_cert)
        .build())
}

/// The certificates of a PEM file, at least one.
fn certificates(pem: &[u8], file: &str) -> Result<Vec<Certificate<'static>>, String> {
    let certificates = ureq::tls::parse_pem(pem)
        .filter_map(|item| match item {
            Ok(PemItem::Certificate(certificate)) => Some(Ok(certificate)),
            Ok(_) => None,
            Err(error) => Some(Err(format!("{file} is not valid PEM: {error}"))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(format!("{file} holds no certificate"));
    }
    Ok(certificates)
}

/// ureq panics when the client certificate does not go with the key, on the worker's first TLS connection;
/// rustls tells it here.
fn check_client_cert(chain: &[Certificate<'static>], key_pem: &[u8]) -> Result<(), String> {
    // The first key of the file, as ureq takes it
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|error| format!("the client key cannot be read: {error}"))?;
    let chain = chain
        .iter()
        .map(|certificate| CertificateDer::from(certificate.der().to_vec()))
        .collect();
    CertifiedKey::from_der(chain, key, &rustls::crypto::ring::default_provider())
        .map(|_| ())
        .map_err(|error| format!("the client certificate does not go with the client key: {error}"))
}

/// What the pipeline counts, for `stats()` of #14.
#[derive(Debug, Default)]
pub struct Counters {
    /// Spans dropped because the queue was full
    pub dropped: AtomicU64,
    /// Spans of batches dropped because their export failed
    pub failed: AtomicU64,
    /// Spans the collector rejected in a partial success
    pub rejected: AtomicU64,
    /// Requests sent again
    pub retries: AtomicU64,
}

/// The worker's HTTP client: one `Agent`, so connections are reused across exports.
pub(crate) struct Client {
    agent: Agent,
    endpoint: String,
    headers: HeaderMap,
    compression: Compression,
    timeout: Duration,
    counters: Arc<Counters>,
    // A message once shutdown has stopped waiting: no more retries, as the reference's client stops retrying
    // when the processor shuts it down after its wait
    abandon: Receiver<()>,
    abandoned: bool,
}

/// How one request went.
enum Attempt {
    /// Below 400, as the reference counts success; the spans a partial success rejected
    Accepted { rejected: u64 },
    Refused {
        status: u16,
        retry_after: Option<Duration>,
    },
    /// The collector could not be reached or dropped the connection: retried
    Unreachable,
    /// Anything else, or no time left: not retried
    Failed,
}

impl Client {
    pub(crate) fn new(config: &Config, counters: Arc<Counters>, abandon: Receiver<()>) -> Self {
        let agent = Agent::config_builder()
            // A status is not an error of the transport: the export reads it
            .http_status_as_error(false)
            .tls_config(config.tls.clone())
            .build()
            .into();
        let mut headers = config.headers.clone();
        // Unless the headers carry one, as the reference's client adds it
        if let Some(encoding) = config.compression.content_encoding() {
            headers
                .entry(CONTENT_ENCODING)
                .or_insert(HeaderValue::from_static(encoding));
        }
        Self {
            agent,
            endpoint: config.endpoint.clone(),
            headers,
            compression: config.compression,
            timeout: config.timeout,
            counters,
            abandon,
            abandoned: false,
        }
    }

    /// Send an encoded batch of `spans` spans, retrying within the timeout; counts what is lost.
    pub(crate) fn send(&mut self, body: Vec<u8>, spans: usize) {
        let body = self.compression.compress(body);
        match self.deliver(&body) {
            Some(rejected) => {
                self.counters
                    .rejected
                    .fetch_add(rejected, Ordering::Relaxed);
            }
            None => {
                self.counters
                    .failed
                    .fetch_add(spans as u64, Ordering::Relaxed);
            }
        }
    }

    /// The spans rejected once the batch is accepted, None when it is dropped.
    ///
    /// The reference's loop: up to 6 attempts, within the timeout of the whole export; a retryable status or a
    /// connection error waits 2^n s with a jitter of 20%, or the `Retry-After` of the status, and a wait that
    /// would end after the timeout gives up at once.
    fn deliver(&mut self, body: &[u8]) -> Option<u64> {
        // None when the timeout is too long to tell the time it ends
        let deadline = Instant::now().checked_add(self.timeout);
        for attempt in 0..MAX_ATTEMPTS {
            let mut wait = backoff(attempt, random());
            match self.submit(body, deadline) {
                Attempt::Accepted { rejected } => return Some(rejected),
                Attempt::Refused {
                    status,
                    retry_after,
                } if RETRYABLE.contains(&status) => {
                    if let Some(retry_after) = retry_after {
                        wait = retry_after;
                    }
                }
                Attempt::Unreachable => {}
                Attempt::Refused { .. } | Attempt::Failed => return None,
            }
            if attempt + 1 == MAX_ATTEMPTS || wait > remaining(deadline) || !self.wait(wait) {
                return None;
            }
            self.counters.retries.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    fn submit(&mut self, body: &[u8], deadline: Option<Instant>) -> Attempt {
        let attempt = self.post(body, deadline);
        if matches!(attempt, Attempt::Unreachable) && !remaining(deadline).is_zero() {
            // At once, as the reference: mostly a pooled connection the collector has closed meanwhile, and a new
            // one goes through
            self.counters.retries.fetch_add(1, Ordering::Relaxed);
            return self.post(body, deadline);
        }
        attempt
    }

    fn post(&self, body: &[u8], deadline: Option<Instant>) -> Attempt {
        let remaining = remaining(deadline);
        if remaining.is_zero() {
            return Attempt::Failed;
        }
        let mut request = self
            .agent
            .post(&self.endpoint)
            .config()
            // The whole request, from connecting to the last byte of the answer
            .timeout_global((remaining != Duration::MAX).then_some(remaining))
            .build();
        if let Some(headers) = request.headers_mut() {
            headers.extend(self.headers.clone());
        }
        match request.send(body) {
            Ok(response) => {
                let status = response.status().as_u16();
                let headers = response.headers();
                let retry_after = headers
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| retry_after(value, SystemTime::now()));
                // The answer to a protobuf request is protobuf; anything else is not read as a response
                let protobuf = headers
                    .get(CONTENT_TYPE)
                    .is_none_or(|value| value.as_bytes().starts_with(b"application/x-protobuf"));
                // Reading the body to the end returns the connection to the pool
                let answer = response.into_body().read_to_vec();
                if status < 400 {
                    let rejected = match answer {
                        Ok(answer) if protobuf => rejected(&answer),
                        _ => 0,
                    };
                    Attempt::Accepted { rejected }
                } else {
                    Attempt::Refused {
                        status,
                        retry_after,
                    }
                }
            }
            Err(error) if unreachable(&error) => Attempt::Unreachable,
            Err(_) => Attempt::Failed,
        }
    }

    /// False when shutdown has stopped waiting, during the wait or before it.
    fn wait(&mut self, wait: Duration) -> bool {
        if self.abandoned {
            return false;
        }
        match self.abandon.recv_timeout(wait) {
            Ok(()) => {
                self.abandoned = true;
                false
            }
            Err(RecvTimeoutError::Timeout) => true,
            // The pipeline is dropped and has no shutdown left to abandon its retries
            Err(RecvTimeoutError::Disconnected) => {
                thread::sleep(wait);
                true
            }
        }
    }
}

/// The time left before `deadline`; `Duration::MAX` without one.
fn remaining(deadline: Option<Instant>) -> Duration {
    deadline.map_or(Duration::MAX, |deadline| {
        deadline.saturating_duration_since(Instant::now())
    })
}

/// The wait before retry `attempt + 1`, `unit` in [0, 1) choosing the jitter.
fn backoff(attempt: u32, unit: f64) -> Duration {
    Duration::from_secs_f64(f64::from(1u32 << attempt) * (1.0 - JITTER + 2.0 * JITTER * unit))
}

/// A number in [0, 1): `RandomState` has fresh random keys every time, which is all a jitter needs.
fn random() -> f64 {
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

/// `Retry-After` as the reference reads it: seconds as a float, not negative, or an HTTP-date.
fn retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<f64>() {
        // Too long to hold is longer than any timeout
        return seconds
            .is_finite()
            .then(|| Duration::try_from_secs_f64(seconds.max(0.0)).unwrap_or(Duration::MAX));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

/// The errors the reference retries: those requests raises as a `ConnectionError`, which covers refused and
/// dropped connections, DNS and TLS failures; a timeout leaves no time to retry anyway.
fn unreachable(error: &Error) -> bool {
    matches!(
        error,
        Error::Io(_)
            | Error::Timeout(_)
            | Error::HostNotFound
            | Error::ConnectionFailed
            | Error::Protocol(_)
            | Error::Tls(_)
            | Error::Rustls(_)
    )
}

/// The spans a partial success rejected; an answer that does not decode rejects none.
fn rejected(answer: &[u8]) -> u64 {
    ExportTraceServiceResponse::decode(answer)
        .ok()
        .and_then(|response| response.partial_success)
        .map_or(0, |partial| {
            u64::try_from(partial.rejected_spans).unwrap_or(0)
        })
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::{GzDecoder, ZlibDecoder};
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTracePartialSuccess;

    use super::*;

    #[test]
    fn backoff_doubles_with_a_jitter_of_a_fifth() {
        assert_eq!(backoff(0, 0.0), Duration::from_millis(800));
        assert_eq!(backoff(0, 0.5), Duration::from_secs(1));
        assert_eq!(backoff(4, 0.5), Duration::from_secs(16));
        assert!(backoff(2, 0.999_999) < Duration::from_millis(4800));
        let units: Vec<f64> = (0..100).map(|_| random()).collect();
        assert!(units.iter().all(|unit| (0.0..1.0).contains(unit)));
        assert!(units.windows(2).any(|pair| pair[0] != pair[1]));
    }

    #[test]
    fn retry_after_takes_seconds_or_a_date() {
        let now = httpdate::parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT").unwrap();
        let read = |value| retry_after(value, now);
        assert_eq!(read("120"), Some(Duration::from_secs(120)));
        assert_eq!(read(" 0.5 "), Some(Duration::from_millis(500)));
        assert_eq!(read("-3"), Some(Duration::ZERO));
        assert_eq!(read("1e300"), Some(Duration::MAX));
        assert_eq!(read("nan"), None);
        assert_eq!(read("inf"), None);
        assert_eq!(
            read("Sun, 06 Nov 1994 08:50:07 GMT"),
            Some(Duration::from_secs(30))
        );
        assert_eq!(read("Sun, 06 Nov 1994 08:00:00 GMT"), Some(Duration::ZERO));
        assert_eq!(read("soon"), None);
    }

    #[test]
    fn compression_round_trips() {
        let body = b"spans spans spans spans spans".repeat(10);
        let mut unzipped = Vec::new();
        GzDecoder::new(&Compression::Gzip.compress(body.clone())[..])
            .read_to_end(&mut unzipped)
            .unwrap();
        assert_eq!(unzipped, body);
        let mut inflated = Vec::new();
        ZlibDecoder::new(&Compression::Deflate.compress(body.clone())[..])
            .read_to_end(&mut inflated)
            .unwrap();
        assert_eq!(inflated, body);
        assert_eq!(Compression::None.compress(body.clone()), body);
    }

    #[test]
    fn a_partial_success_tells_the_rejected_spans() {
        let answer = ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: 3,
                error_message: "too old".to_owned(),
            }),
        };
        assert_eq!(rejected(&answer.encode_to_vec()), 3);
        assert_eq!(rejected(b""), 0);
        assert_eq!(rejected(b"\xff\xff"), 0);
    }

    #[test]
    fn tls_files_are_checked() {
        assert!(tls(None, None, None).is_ok());
        assert!(tls(Some(b"not pem"), None, None).is_err());
        assert!(tls(None, Some(b"not pem"), None).is_err());
    }
}
