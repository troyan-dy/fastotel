# 0003. Span encoding: what the reference exporter sends, compared request by request

- Status: accepted
- Date: 2026-10-10
- Ticket: #8 (the compatibility test of #1)

## Context

#8 asks that fastotel's requests decode to the same `ExportTraceServiceRequest` that
`opentelemetry-exporter-otlp-proto-http` sends for the same spans, every field included, and that values the SDK
lets through but OTLP cannot express are handled as the reference handles them. The reference is the encoder of
`opentelemetry-exporter-otlp-proto-common` (`_internal/__init__.py` and `_internal/trace_encoder`), 1.45.1 at the
time of writing; its behaviour below was read from that source and checked against it by the tests, not taken
from the OTLP specification.

## Decision

### The reference is the bar, checked by a test that compares requests

- `tests/test_compatibility.py` ends the same spans into `OTLPSpanProcessor` and into a processor that collects
  them for the reference `OTLPSpanExporter`; both send to a fake receiver (`tests/receiver.py`), and the decoded
  requests must be equal after merging batches and sorting the spans inside each scope. Messages are compared in
  the protobuf text format, which makes NaN equal to itself and shows a readable diff.
- The spans: `tests/spans.py` sets every field (every attribute type on the span, the resource, the scope, events
  and links; every kind and status; local, remote and no parent; trace state; dropped counts through
  `SpanLimits`; schema URLs; a span without a scope; empty names, non-ASCII strings, the int64 bounds, NaN and
  infinities), and a `hypothesis` test generates attribute values and span shapes.
- When a new reference changes its encoding, the test fails and says where. The reference is a dev dependency
  only. The `lowest` CI job runs SDK 1.16, older than any reference that shares the current encoder, so it skips
  the comparison (`collect_ignore` in `tests/conftest.py`) and checks only that every span of `tests/spans.py`
  arrives.

### The fields, as the reference fills them

- Attributes are the whole `AnyValue`, the "extended" attributes of current SDKs: None is an `AnyValue` with no
  value, then bool, str, int, float, bytes, any sequence (mixed and nested too) as an array, any mapping as a
  key-value list with its keys turned into strings. The copy checks the types in the reference's order
  (`isinstance`, so subclasses count, and a bool is not an int).
- `flags` of a span and of a link carry only the is-remote bits: `HAS_IS_REMOTE` always, `IS_REMOTE` when the
  parent (or the linked context) is remote. **The trace flags are not in the low byte**, although OTLP has room
  for them and #8 lists them: the reference leaves them out, so fastotel does too, and only sampled spans are
  exported anyway. Likewise a link carries no trace state, and a resource no dropped attribute count.
- The trace state is `key=value` pairs joined by commas, in the order of the `TraceState`. The status is always
  present, its code and description.
- String fields (names, version, schema URLs, description, attribute keys) are what protobuf makes of the value:
  None is empty and bytes are decoded as UTF-8. The SDK does not check the type of a name, so `start_span(None)`
  and `start_span(b"name")` reach the exporter.
- Spans group into `ResourceSpans` and `ScopeSpans` by the SDK's equality, as the reference groups them in dicts:
  resources by attributes (as a dict, whatever their order) and schema URL, scopes by name, version, schema URL and
  attributes. A span without a scope goes into a `ScopeSpans` of its own with an empty scope. Values compare as
  they encode, which is stricter than Python in a few odd cases where the reference groups otherwise: Python has
  1 == 1.0 == True and 0.0 == -0.0, which can merge two scopes whose attributes differ only that way (resources
  hash such values apart, so it does not merge them); and two NaN objects are unequal in Python, so the reference
  keeps apart two resources or scopes that differ only by being built with separate NaNs, which fastotel, comparing
  doubles by their bits, merges. No span is lost either way, only the grouping differs.
- Older SDKs lack some fields (the dropped attribute counts of events and links, scope attributes before 1.26):
  they are sent as 0 or empty.

### Values OTLP cannot express

The reference catches any exception while it encodes one attribute and leaves out that attribute. fastotel leaves
it out in the same cases, named in `tests/test_compatibility.py`:

- an int beyond int64, anywhere in the value;
- a string or a mapping key that is not valid UTF-8 (a lone surrogate), anywhere in the value;
- a value of a type OTLP has no value for, a key that is not a str, bytes or None, a sequence whose iteration
  fails, a self-referencing list: things a hand-built `ReadableSpan` can hold, which the SDK cleans away
  otherwise.

Two cases where fastotel deliberately differs:

- **Nesting.** Protobuf decoders (upb, C++, the Python one) refuse a message nested deeper than 100 levels by
  default. fastotel leaves out an attribute that would put a message of the request deeper than that: a span
  attribute's `AnyValue` is the 6th level, an array adds 2 and a mapping 3. The reference depends on the
  protobuf backend: on upb it leaves out values nested deeper still, but a little below that it raises for the
  whole batch or sends a request decoders refuse; on pure-Python protobuf (free-threaded builds) it sends them
  whatever the depth. The test compares with the reference where it leaves the attribute out (upb) and checks the
  limit of fastotel on its own.
- **Strings outside attributes.** A span name, event name, status description, scope name, version or schema URL,
  or resource schema URL that protobuf cannot take (not valid UTF-8, or not a str, bytes or None) makes the
  reference's encoder raise for the whole request: the exporter logs it and loses the batch. fastotel drops that
  span alone (its copy fails in `on_end`) and sends the others.

## Consequences

- `on_end` copies more than the string attributes of #7: with 5 attributes of mixed types it takes about 3.8 µs
  on the calling thread (release build, CPython 3.14, Apple M2 Pro), against 1.9 µs for #7's slice; a rough
  figure, which #18 measures properly. The copy does one `getattr` per field, events and links included.
- A later ticket that touches the copy or the encoder runs `tests/test_compatibility.py`, which is the
  specification of the format; a difference from the reference goes into this ADR with a test that names it.
- Found while testing, relevant for #13: a `TracerProvider` re-runs resource detectors through a thread pool in a
  forked child, and when `concurrent.futures.thread` was first imported after the provider registered its fork
  handler (`TracerProvider(resource=...)` does not import it), the child deadlocks on the pool's lock, with or
  without fastotel. `tests/conftest.py` imports the module first.
