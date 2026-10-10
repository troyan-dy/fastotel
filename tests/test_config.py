"""
The exporter configuration of #10: the arguments and OTEL_EXPORTER_OTLP_* variables of the reference
OTLPSpanExporter, read with its precedence, parsing and fallbacks.
"""

import logging
import math
import time
from importlib.metadata import version
from typing import Any

import pytest
from fastotel import OTLPSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from receiver import FakeReceiver

VARIABLES = tuple(
    f"OTEL_EXPORTER_OTLP{signal}_{setting}"
    for setting in (
        "ENDPOINT",
        "HEADERS",
        "TIMEOUT",
        "COMPRESSION",
        "CERTIFICATE",
        "CLIENT_KEY",
        "CLIENT_CERTIFICATE",
    )
    for signal in ("", "_TRACES")
)
USER_AGENT = f"fastotel/{version('fastotel')}"

# The endpoint, the headers, the timeout, the compression, the CA file (True: the default trust store) and the
# client certificate as requests takes it
Settings = tuple[str, dict[str, str], str, str, str | bool, str | tuple[str, str] | None]
# The effective settings, or the error at construction, and what was logged
Outcome = tuple[Settings | str, list[tuple[int, str]]]


@pytest.fixture(autouse=True)
def _no_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


def _logged(caplog: pytest.LogCaptureFixture, loggers: tuple[str, ...]) -> list[tuple[int, str]]:
    return sorted((record.levelno, record.getMessage()) for record in caplog.records if record.name in loggers)


def _reference(caplog: pytest.LogCaptureFixture, kwargs: dict[str, Any]) -> Outcome:
    from opentelemetry.exporter.otlp.proto.http import _OTLP_HTTP_HEADERS
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    loggers = ("opentelemetry.exporter.otlp.proto.http._common", "opentelemetry.util.re")
    caplog.clear()
    exporter = OTLPSpanExporter(**kwargs)
    client = exporter._client
    headers = dict(client._headers)
    # Added by the client when it compresses, which fastotel does from #11; a header given by the user is lower case
    headers.pop("Content-Encoding", None)
    if headers["user-agent"] == _OTLP_HTTP_HEADERS["User-Agent"]:
        headers["user-agent"] = USER_AGENT
    # The requests transport, as built without a transport given
    session = client._transport._session  # type: ignore[attr-defined]
    settings = (
        exporter._endpoint,
        headers,
        repr(client._timeout),
        exporter._compression.value,
        session.verify,
        session.cert,
    )
    exporter.shutdown()  # type: ignore[no-untyped-call]
    return settings, _logged(caplog, loggers)


def _fastotel(caplog: pytest.LogCaptureFixture, kwargs: dict[str, Any]) -> Outcome:
    caplog.clear()
    try:
        processor = OTLPSpanProcessor(**kwargs)
    except ValueError as error:
        return str(error), _logged(caplog, ("fastotel",))
    client_certificate, client_key = processor._client_certificate_file, processor._client_key_file
    settings = (
        processor._endpoint,
        processor._headers,
        repr(processor._timeout),
        processor._compression,
        processor._certificate_file or True,
        (client_certificate, client_key) if client_certificate and client_key else client_certificate,
    )
    processor.shutdown()
    return settings, _logged(caplog, ("fastotel",))


def _gzip() -> Any:
    from opentelemetry.exporter.otlp.proto.http import Compression

    return Compression.Gzip


def _no_compression() -> Any:
    from opentelemetry.exporter.otlp.proto.http import Compression

    return Compression.NoCompression


@pytest.mark.parametrize(
    ("environ", "kwargs"),
    [
        ({}, {}),
        # The endpoint: the traces variable as it is, the general one with /v1/traces, the argument over both
        ({"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector:4318/custom"}, {}),
        ({"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector:4318/"}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318"}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318/"}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318//"}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector/prefix/"}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://a:4318", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://b:4318/x"}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://a:4318", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": ""}, {}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": ""}, {}),
        ({"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://b:4318/x"}, {"endpoint": "http://c:4318/v1/traces"}),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://a:4318"}, {"endpoint": ""}),
        # Headers: W3C baggage format, percent-decoded, names lower-cased; the traces variable replaces the other
        ({"OTEL_EXPORTER_OTLP_HEADERS": "api-key=secret,tenant=acme"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=1,b=2", "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "c=3"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=1", "OTEL_EXPORTER_OTLP_TRACES_HEADERS": ""}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "Authorization=Basic%20dXNlcjpwYXNz%3D%3D"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "  API-Key = Secret  ,\tb=2 , ,c=x=y,"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "café=1"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=caf%C3%A9"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=b%2Cc"}, {}),
        # Not URL-encoded, which the reference takes anyway
        ({"OTEL_EXPORTER_OTLP_HEADERS": "authorization=Bearer abc def"}, {}),
        # Invalid entries are skipped with a warning
        ({"OTEL_EXPORTER_OTLP_HEADERS": 'novalue,=b,a=b;c,q="x",ok=1'}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=b\\c,d=e"}, {}),
        # Arguments merge into the variable, per name, any case
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=1,b=2"}, {"headers": {"A": "3", "c": "4"}}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a=1"}, {"headers": {}}),
        ({}, {"headers": {"Content-Type": "application/json", "User-Agent": "mine/1"}}),
        ({}, {"headers": {"X-Tenant": " spaced value "}}),
        # The timeout: float() of the variable, or the default and a warning
        ({"OTEL_EXPORTER_OTLP_TIMEOUT": "5"}, {}),
        ({"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT": " 2.5 "}, {}),
        ({"OTEL_EXPORTER_OTLP_TIMEOUT": "7", "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT": "3"}, {}),
        ({"OTEL_EXPORTER_OTLP_TIMEOUT": "7", "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT": ""}, {}),
        *[
            ({"OTEL_EXPORTER_OTLP_TIMEOUT": value}, {})
            for value in ("abc", "5s", "1e3", "1_0", "0", "-1", "nan", "inf")
        ],
        ({"OTEL_EXPORTER_OTLP_TIMEOUT": "abc"}, {"timeout": 2.5}),
        ({"OTEL_EXPORTER_OTLP_TIMEOUT": "5"}, {"timeout": 0}),
        # Compression: none, gzip or deflate, any case; anything else is none with a warning
        *[
            ({"OTEL_EXPORTER_OTLP_COMPRESSION": value}, {})
            for value in ("gzip", " GZip ", "deflate", "none", "brotli", " ", "")
        ],
        ({"OTEL_EXPORTER_OTLP_COMPRESSION": "gzip", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION": "none"}, {}),
        ({"OTEL_EXPORTER_OTLP_COMPRESSION": "gzip", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION": ""}, {}),
        ({"OTEL_EXPORTER_OTLP_COMPRESSION": "brotli"}, {"compression": _gzip}),
        ({"OTEL_EXPORTER_OTLP_COMPRESSION": "gzip"}, {"compression": _no_compression}),
        # The TLS files: the argument, the traces variable, the general one
        ({"OTEL_EXPORTER_OTLP_CERTIFICATE": "/ca.pem"}, {}),
        ({"OTEL_EXPORTER_OTLP_CERTIFICATE": "/ca.pem", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": "/traces-ca.pem"}, {}),
        ({"OTEL_EXPORTER_OTLP_CERTIFICATE": "/ca.pem", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": ""}, {}),
        ({"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": "/traces-ca.pem"}, {"certificate_file": "/arg-ca.pem"}),
        ({"OTEL_EXPORTER_OTLP_CERTIFICATE": "/ca.pem"}, {"certificate_file": ""}),
        ({"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "/c.pem", "OTEL_EXPORTER_OTLP_CLIENT_KEY": "/k.pem"}, {}),
        (
            {
                "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "/c.pem",
                "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE": "/tc.pem",
                "OTEL_EXPORTER_OTLP_CLIENT_KEY": "/k.pem",
                "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY": "/tk.pem",
            },
            {},
        ),
        # A certificate alone holds its key; a key alone is ignored
        ({"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "/both.pem"}, {}),
        ({"OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY": "/k.pem"}, {}),
        (
            {"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "/c.pem", "OTEL_EXPORTER_OTLP_CLIENT_KEY": "/k.pem"},
            {"client_certificate_file": "/arg-c.pem", "client_key_file": "/arg-k.pem"},
        ),
        ({"OTEL_EXPORTER_OTLP_CLIENT_KEY": "/k.pem"}, {"client_certificate_file": "/arg-c.pem"}),
        # Everything at once
        (
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector:4318",
                "OTEL_EXPORTER_OTLP_HEADERS": "api-key=1",
                "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT": "4",
                "OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
                "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": "/ca.pem",
            },
            {"headers": {"tenant": "acme"}},
        ),
    ],
)
def test_reads_the_configuration_as_the_reference_exporter_does(
    environ: dict[str, str], kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The lowest job of CI runs without the reference
    pytest.importorskip("opentelemetry.exporter.otlp.proto.http")
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    caplog.set_level(logging.DEBUG)
    # The reference's enums are made here, once it is known to be there
    kwargs = {name: value() if callable(value) else value for name, value in kwargs.items()}

    reference = _reference(caplog, kwargs)
    assert _fastotel(caplog, kwargs) == reference


def _span() -> ReadableSpan:
    span = TracerProvider().get_tracer("app").start_span("span")
    span.end()
    assert isinstance(span, ReadableSpan)
    return span


def test_the_request_carries_the_configured_headers_to_the_configured_endpoint(
    receiver: FakeReceiver, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = receiver.endpoint.removesuffix("/v1/traces")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", base + "/")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Basic%20dXNlcg%3D%3D,tenant=env")
    processor = OTLPSpanProcessor(headers={"Tenant": "argument", "x-extra": "1"})
    processor.on_end(_span())
    assert processor.force_flush()
    processor.shutdown()

    [received] = receiver.received
    assert received.path == "/v1/traces"
    assert received.headers["content-type"] == "application/x-protobuf"
    assert received.headers["user-agent"] == USER_AGENT
    assert received.headers["authorization"] == "Basic dXNlcg=="
    assert received.headers["tenant"] == "argument"
    assert received.headers["x-extra"] == "1"


def test_headers_given_as_arguments_replace_the_defaults(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, headers={"User-Agent": "mine/1"})
    processor.on_end(_span())
    assert processor.force_flush()
    processor.shutdown()

    [received] = receiver.received
    assert received.headers["user-agent"] == "mine/1"


def test_a_request_is_abandoned_after_the_timeout(receiver: FakeReceiver, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "0.3")
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    with receiver.stalled():
        processor.on_end(_span())
        started = time.monotonic()
        # The flush returns once the request has given up, not after the 10 s default
        assert processor.force_flush()
        waited = time.monotonic() - started
    assert len(receiver.received) == 1
    assert 0.25 < waited < 5
    processor.shutdown()


@pytest.mark.parametrize(
    ("given", "expected"), [("gzip", "gzip"), (" GZIP ", "gzip"), ("deflate", "deflate"), ("none", "none")]
)
def test_compression_can_be_given_as_a_string(given: str, expected: str) -> None:
    # The reference takes only its enums (and fails on a string); a string is fastotel's convenience
    processor = OTLPSpanProcessor(compression=given)
    assert processor._compression == expected
    processor.shutdown()


def test_an_unknown_compression_argument_raises() -> None:
    with pytest.raises(ValueError, match="Invalid compression type: 'brotli'"):
        OTLPSpanProcessor(compression="brotli")
    with pytest.raises(TypeError, match="compression"):
        OTLPSpanProcessor(compression=1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("environ", "kwargs"),
    [
        ({}, {"headers": {"x-bad": "a\r\nb"}}),
        ({}, {"headers": {"bad name": "a"}}),
        ({}, {"headers": {"": "a"}}),
        # Percent-decoded into a line break, or into a name with a space
        ({"OTEL_EXPORTER_OTLP_HEADERS": "x-bad=a%0Ab"}, {}),
        ({"OTEL_EXPORTER_OTLP_HEADERS": "a%20b=c"}, {}),
    ],
)
def test_a_header_http_cannot_carry_raises(
    environ: dict[str, str], kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    # A deliberate difference (ADR 0005): the reference takes it and then fails every export
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="header"):
        OTLPSpanProcessor(**kwargs)


@pytest.mark.parametrize("timeout", [math.nan, -1.0, 0.0, math.inf, 1e300])
def test_any_float_timeout_is_taken(timeout: float) -> None:
    # As the reference: none is refused at construction; NaN or not positive fails each request, inf never ends
    processor = OTLPSpanProcessor(timeout=timeout)
    assert repr(processor._timeout) == repr(timeout)
    processor.shutdown()


@pytest.mark.parametrize("timeout", [0.0, -1.0, math.nan])
def test_a_timeout_not_positive_fails_each_request_at_once(receiver: FakeReceiver, timeout: float) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, timeout=timeout)
    processor.on_end(_span())
    assert processor.force_flush()
    processor.shutdown()
    assert receiver.received == []
