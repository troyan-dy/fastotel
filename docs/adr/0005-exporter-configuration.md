# 0005. The exporter configuration: OTLPSpanExporter's arguments and variables, read in Python as it reads them

- Status: accepted
- Date: 2026-10-10
- Ticket: #10

## Context

#10 asks that `OTLPSpanProcessor` read the exporter configuration exactly as the reference `OTLPSpanExporter` of
`opentelemetry-exporter-otlp-proto-http` does, so that moving from it changes nothing but the import: the endpoint,
headers, timeout, compression and TLS files, from the arguments, the `OTEL_EXPORTER_OTLP_TRACES_*` variables and the
`OTEL_EXPORTER_OTLP_*` ones. The reference is the exporter 1.45.1 read from its source: `OTLPSpanExporter.__init__`,
the `_resolve_*` functions and `_build_transport` of its `_common` module, the `_OTLPHTTPClient` of
`opentelemetry-exporter-otlp-common` and `parse_env_headers` of `opentelemetry.util.re`. ADR 0002 already gave
`endpoint` the reference's meaning, and ADR 0004 read the `OTEL_BSP_*` settings in the constructor and compared them
with a real `BatchSpanProcessor`; this follows the same pattern. Nothing here may add to the cost of `on_end`.

## Decision

### Read once, in Python, in the constructor

- `OTLPSpanProcessor(*, endpoint=None, headers=None, timeout=None, compression=None, certificate_file=None,
  client_key_file=None, client_certificate_file=None, ...)` with the batching arguments of ADR 0004: the names of
  the reference, keyword-only like the others. `python/fastotel/_exporter.py` resolves each, in the reference's
  order, and the native `Processor` takes the endpoint, the headers and the timeout; `on_end` is untouched.
- The precedence is the reference's `or` chain, so an empty argument or variable counts as unset (a `timeout` of 0
  does not, as there):
  - **endpoint**: the argument; `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` as it is; `OTEL_EXPORTER_OTLP_ENDPOINT` with
    one trailing slash removed and `/v1/traces` appended; `http://localhost:4318/v1/traces`.
  - **headers**: `content-type: application/x-protobuf` and a user agent, then the traces variable or else the
    general one, then the argument, merged name by name with the names lower-cased. The variable is parsed as the
    reference parses it, `parse_env_headers(..., liberal=True)`: W3C baggage entries, percent-decoded, and values
    with spaces that are not URL-encoded taken as they are; anything else is skipped with the reference's warning.
  - **timeout**: the argument; otherwise `float()` of the traces variable or the general one; 10 s. A value
    `float()` refuses gives 10 and the reference's warning. NaN, infinity and values not positive are taken, as
    there (see below).
  - **compression**: the argument; otherwise the variable, lower-cased and stripped, one of `none`, `deflate` and
    `gzip` (the reference accepts deflate too, though #10 names only gzip); anything else is `none` with the
    reference's warning.
  - **TLS files**: each of `certificate_file`, `client_key_file` and `client_certificate_file` from its argument,
    traces variable or general variable. As in the reference, a client key without a client certificate is not
    used, and a client certificate without a key is a file that holds both.
- Warnings go to the `fastotel` logger, as ADR 0004's error logs do, with the reference's messages.
- `parse_env_headers` is copied from `opentelemetry.util.re` (Apache-2.0) instead of imported: fastotel allows
  `opentelemetry-api` from 1.16, whose function has no liberal mode, and the configuration should not depend on the
  API version installed.
- `tests/test_config.py` builds the reference `OTLPSpanExporter` next to an `OTLPSpanProcessor` from the same
  variables and arguments, some 60 combinations, and compares the endpoint, the headers, the timeout, the
  compression, what the reference's requests session verifies with and its client certificate, and the warnings.
  Two differences are mapped in the comparison: the reference's `User-Agent` and the `Content-Encoding` its client
  adds when it compresses. Other tests check the request the fake receiver gets: the path from
  `OTEL_EXPORTER_OTLP_ENDPOINT`, the merged headers, and a request abandoned after the timeout. The `lowest` CI job
  has no reference exporter and skips the comparison only.

### What applies now

- **Headers** are sent with every request, an argument's `content-type` or `user-agent` included.
- **The timeout** limits each request (ureq's global timeout), in place of the fixed 10 s of ADR 0002. NaN or a value
  not positive fails each request at once, as the reference's requests fail; infinity, or a duration too long for
  the clock, is no limit. Retries and their deadline are #11's.
- **Compression and the TLS files** are resolved and kept on the processor (`_compression`, `_certificate_file`,
  `_client_key_file`, `_client_certificate_file`) but not passed to the pipeline: #11 wires them, and with them
  the `Content-Encoding` header, which the reference's client adds unless the headers carry one. Until then a
  configured gzip sends uncompressed requests, which collectors accept, and a `https://` endpoint is verified
  against the OS trust store whatever `certificate_file` says.

### Where fastotel differs from the reference

- **The user agent** is `fastotel/<version>`, where the reference sends `OTel-OTLP-Exporter-Python/<version>`: the
  OTLP specification asks the exporter to name itself. A `User-Agent` header given by the user replaces it, as there.
- **A header HTTP cannot carry raises `ValueError` at construction**: a name that is not a token (empty, or with a
  space, which percent-decoding can make: `a%20b=c`), or a value with a control character (`x=a%0Ab`, or an
  argument with a line break). The reference builds the exporter and then fails every export; fastotel has no way
  yet to report from the worker (#14), and a configuration that can never send should fail where it is made. A
  value beyond ASCII (`a=caf%C3%A9`) is sent as UTF-8, where requests would send Latin-1.
- **`compression` also takes a string**, `"gzip"`, `"deflate"` or `"none"` in any case; anything else raises
  `ValueError` with the reference's message, and a value that is neither a string nor an enum with a string value
  raises `TypeError`. The reference takes its two `Compression` enums, which fastotel takes by their value, and
  fails with an `AttributeError` on a string. fastotel does not depend on the reference, so it adds no enum of its
  own.
- **Not carried over**: `session` and the `OTEL_PYTHON_EXPORTER_OTLP_HTTP_*_CREDENTIAL_PROVIDER` variables (a
  `requests.Session`, which a Rust client cannot use), `max_request_size` and `meter_provider`.
  `OTEL_EXPORTER_OTLP_PROTOCOL` is not read, as the reference does not read it: fastotel is OTLP/HTTP protobuf only.

## Consequences

- #11 passes `_compression` (gzip, and deflate since the reference accepts it) and the TLS files to the native
  `Processor` and `Config`, adds `Content-Encoding` unless the headers have one, and keeps the retries within
  `Config::timeout` as the reference keeps them within its timeout.
- #14 may turn the construction errors for headers into the reference's per-export failures with a log, if that
  proves friendlier; the comparison test then gains those cases.
- #19 polishes the README table of every argument and variable.
