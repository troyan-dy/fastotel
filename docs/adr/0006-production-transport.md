# 0006. The transport: the reference's retries, gzip with flate2, TLS files checked at construction

- Status: accepted
- Date: 2026-10-10
- Ticket: #11

## Context

#11 makes the export fit for production: gzip when configured, retries per the OTLP/HTTP specification (429, 502,
503, 504 and connection errors, exponential backoff with jitter, `Retry-After`, within the export timeout), batches
dropped and counted on other errors, a partial success reported, TLS through rustls with the OS trust store,
`certificate_file` and mTLS, connections reused, and a collector down for minutes costing neither memory nor CPU.
ADR 0005 resolved `compression` and the TLS files and left them for this ticket. The references are the
specification (<https://opentelemetry.io/docs/specs/otlp/#otlphttp-response>) and, where it leaves room, the
reference exporter 1.45.1 read from its source: `_OTLPHTTPClient` of `opentelemetry-exporter-otlp-common` (retries,
compression, `Retry-After`) and its requests transport. Everything here runs on the worker thread; `on_end` is
untouched.

## Decision

### Retries: the reference's loop

`crates/export/src/transport.rs` holds the worker's `Client`, which sends a batch as the reference's
`_OTLPHTTPClient.export` does:

- **Within the timeout of the export**: `timeout` (`OTEL_EXPORTER_OTLP_TIMEOUT`, 10 s) now bounds the export of a
  batch, retries and waits included, as in the reference; each request gets what is left as ureq's global timeout.
  A timeout that leaves no time (0, negative, NaN, beyond 2^63 ns: ADR 0005) sends nothing.
- **At most 6 attempts.** After attempt n (from 0) the wait is 2^n s times a jitter drawn from [0.8, 1.2), so 1, 2,
  4, 8, 16 s; a wait that would end after the timeout gives up at once instead of waiting. With the default 10 s a
  collector that stays down costs a batch 4 attempts and about 7 s of waiting.
- **What is retried**: the statuses 429, 502, 503 and 504, and the errors requests raises as `ConnectionError`:
  a refused or dropped connection, DNS, a broken HTTP answer and, as requests' `SSLError` is one, TLS failures. A
  connection error is first resent once at once, as the reference does for a pooled connection the collector closed
  meanwhile. Every other status from 400 up (400, 401, 413, 500, 501, ...) and every other error drops the batch.
  Below 400 is success, as there.
- **`Retry-After`** of a retryable status replaces the backoff, read as the reference reads it: seconds as a float
  (negative is 0; NaN and infinity are ignored) or an HTTP-date (`httpdate`, the three forms of RFC 9110, where
  Python's `parsedate_to_datetime` takes a few more). A `Retry-After` beyond the timeout gives up at once.
- **Shutdown** ends the retries once it stops waiting (`export_timeout_millis`), as the reference's processor
  shuts its exporter down after its wait: `Pipeline::shutdown` sends the worker a message that interrupts a wait,
  and from then on every batch gets one attempt. A pipeline dropped without shutdown keeps its waits.
- The random jitter comes from `RandomState`, whose keys are fresh each time: no RNG crate for one number per retry.

### What is counted, and what is not logged yet

- A dropped batch adds its spans to `failed_spans`, a resent request adds to `retries`, and a partial success adds
  its `rejected_spans` to `rejected_spans`, next to `dropped_spans` of ADR 0004. They are atomics shared by the
  pipeline and the worker (`transport::Counters`) and methods of the native `Processor`, not public: #14 makes
  them `stats()`.
- **A partial success is counted, not logged**: the worker cannot reach the `fastotel` logger without a hand-over to
  Python, which is #14's design ("the worker hands messages over without waiting on the GIL"). #14 adds the log of
  the rejected count and the error message, and the logs of failed exports. The reference does not read a partial
  success at all. The answer is decoded only when its `Content-Type` is `application/x-protobuf` or absent, so a
  proxy's text or JSON body is not misread; an answer that does not decode rejects nothing. It is never retried.

### Compression

- `gzip` and `deflate` (zlib, as the reference's `zlib.compress`) through `flate2` with its default pure-Rust
  backend (`miniz_oxide`): no C zlib, nothing to build on musl or Windows arm64. The body is compressed once per
  batch, before the retries.
- **Level 6**, flate2's default and zlib's. The reference's gzip uses Python's `GzipFile` default, 9, which costs
  noticeably more CPU for a few percent of size; the bytes differ, the decoded payload does not (a test compares
  the decoded requests of the same spans with and without compression).
- `Content-Encoding` is added unless the headers carry one, as the reference's client adds it: a user's header
  wins even when it names another encoding.

### TLS

- An `https://` endpoint is verified against the OS trust store (`rustls-platform-verifier`, ADR 0002), so
  corporate CAs work; `certificate_file` replaces it with the certificates of the file, as requests' `verify` does.
  `client_certificate_file` holds the client chain and, without `client_key_file`, the key too; a key without a
  certificate is not used (ADR 0005). `http://` stays plaintext.
- **The files are read and checked at construction**, when the endpoint is `https://`, as requests reads them only
  for https: Python reads them (a missing file raises `FileNotFoundError` with its path) and `fastotel_export::tls`
  parses them, raising `ValueError` naming the argument for a file with no usable certificate, no private key, or a
  key that does not go with the certificate. The reference takes any path and fails every export with it; this
  follows ADR 0005, which raises where fastotel cannot send, since the worker cannot report yet (#14). A directory
  of certificates, which requests also takes for `verify`, is not supported.
- The check of certificate against key matters beyond the message: ureq builds the rustls configuration on the
  first TLS connection and panics there when they do not match, which would kill the worker. `rustls` is a direct
  dependency for that check only, the version and the `ring` provider ureq already uses; still no OpenSSL or
  aws-lc.

### Connections

- One ureq `Agent` per worker for its lifetime, as before; a response body is always read to the end so that its
  connection returns to the pool. A test checks that exports and retries share one connection.

### A collector that is down

- No new mechanism: the queue stays bounded (ADR 0004) while the worker retries a batch, so spans ended meanwhile
  are dropped and counted once it is full; each batch costs at most the timeout, mostly asleep, then is dropped and
  counted; the next batch tries afresh, so exports resume with the first request after the collector is back.
  Tests cover a refused port that starts listening during the backoff, a stalled receiver hitting the timeout, and
  memory while refusing (ADR 0004's test, now with retries).

## Consequences

- #12 builds `force_flush` and `shutdown` on this: a flush during a retry waits for it, within the flush's own
  timeout; a shutdown that times out abandons the retries.
- #13 restarts the worker in a forked child: the `Client` (agent, pool, abandon channel) is per worker and goes with
  it.
- #14 reads `transport::Counters` for `stats()` (`dropped`, `failed`, `rejected`, `retries`; received, exported and
  the queue length are still to count) and logs failed exports and partial successes.
- The fake receiver (`tests/receiver.py`) now answers with scripted `Reply`s, serves TLS from an `ssl.SSLContext`,
  binds a given port, and records the raw body, the client's port and the arrival time of each request. TLS tests
  use `trustme` (dev group, below Python 3.15 where cryptography has wheels; the wheel tests install it with
  `--no-build-package` for cryptography) and skip without it.
