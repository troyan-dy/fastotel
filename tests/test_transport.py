"""
The transport of #11: retries as the OTLP/HTTP specification asks and the reference exporter makes them, gzip,
TLS through rustls, and a collector that is down for a while.
"""

import socket
import ssl
import sys
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from fastotel import OTLPSpanProcessor
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTracePartialSuccess,
    ExportTraceServiceResponse,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from receiver import FakeReceiver, Reply

if TYPE_CHECKING:
    import trustme


def _span(name: str = "span") -> ReadableSpan:
    span = TracerProvider().get_tracer("app").start_span(name, attributes={"key": "value"})
    span.end()
    assert isinstance(span, ReadableSpan)
    return span


def _export(processor: OTLPSpanProcessor, spans: int = 1) -> float:
    """
    Sends `spans` spans in one batch and returns how long the flush took.
    """
    for i in range(spans):
        processor.on_end(_span(f"span {i}"))
    started = time.monotonic()
    assert processor.force_flush()
    return time.monotonic() - started


def _counters(processor: OTLPSpanProcessor) -> tuple[int, int, int]:
    native = processor._native
    return native.failed_spans(), native.rejected_spans(), native.retries()


def _closed_port() -> int:
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port: int = unused.getsockname()[1]
    return port


@pytest.mark.parametrize("status", [429, 502, 503, 504])
def test_a_retryable_status_is_retried(receiver: FakeReceiver, status: int) -> None:
    receiver.reply(Reply(status, {"Retry-After": "0"}))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor, 3)
    first, second = receiver.requests
    assert first == second and len(receiver.spans()) == 6
    assert _counters(processor) == (0, 0, 1)
    processor.shutdown()


@pytest.mark.parametrize("status", [400, 401, 403, 413, 500, 501])
def test_any_other_status_drops_the_batch_and_counts_it(receiver: FakeReceiver, status: int) -> None:
    receiver.reply(Reply(status, {"Retry-After": "0"}))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor, 3)
    assert len(receiver.received) == 1
    assert _counters(processor) == (3, 0, 0)
    # The next batch goes out as usual
    _export(processor)
    assert len(receiver.received) == 2 and _counters(processor) == (3, 0, 0)
    processor.shutdown()


@pytest.mark.parametrize(
    "reply", [Reply(302, {"Location": "/elsewhere"}), Reply(307, {"Location": "/elsewhere"}), Reply(304), Reply(302)]
)
def test_a_redirect_is_not_followed_and_counts_as_sent(receiver: FakeReceiver, reply: Reply) -> None:
    # Below 400, as the reference counts success; it does not follow redirects either
    receiver.reply(reply)
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor)
    assert len(receiver.received) == 1 and _counters(processor) == (0, 0, 0)
    processor.shutdown()


def test_without_retry_after_the_first_retry_waits_about_a_second(receiver: FakeReceiver) -> None:
    receiver.reply(Reply(503))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor)
    first, second = receiver.received
    # 2^0 s with a jitter of 20%
    assert 0.75 < second.at - first.at < 2
    processor.shutdown()


@pytest.mark.parametrize("retry_after", ["0.3", "  0.3 "])
def test_retry_after_sets_the_wait(receiver: FakeReceiver, retry_after: str) -> None:
    receiver.reply(Reply(429, {"Retry-After": retry_after}))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor)
    first, second = receiver.received
    # Shorter than the 0.8 s the backoff would wait at least
    assert 0.29 < second.at - first.at < 0.75
    processor.shutdown()


def test_a_retry_after_beyond_the_timeout_gives_up_at_once(receiver: FakeReceiver) -> None:
    receiver.reply(Reply(503, {"Retry-After": "60"}))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, timeout=5)
    took = _export(processor, 2)
    assert len(receiver.received) == 1
    assert took < 2
    assert _counters(processor) == (2, 0, 0)
    processor.shutdown()


def test_gives_up_after_six_attempts(receiver: FakeReceiver) -> None:
    receiver.reply(*[Reply(503, {"Retry-After": "0"})] * 10)
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor)
    assert len(receiver.received) == 6
    assert _counters(processor) == (1, 0, 5)
    processor.shutdown()


def test_retries_stay_within_the_timeout(receiver: FakeReceiver) -> None:
    receiver.reply(*[Reply(503)] * 10)
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, timeout=1.8)
    took = _export(processor)
    # After about 1 s the second backoff, 2 s give or take 20%, would end after the timeout: no third attempt
    assert len(receiver.received) == 2
    assert took < 2.5
    assert _counters(processor) == (1, 0, 1)
    processor.shutdown()


def test_a_shutdown_that_stops_waiting_ends_the_retries_and_the_exports(receiver: FakeReceiver) -> None:
    receiver.reply(Reply(503, {"Retry-After": "1"}))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, export_timeout_millis=300, max_export_batch_size=2)
    for i in range(5):
        processor.on_end(_span(f"span {i}"))
    # The first batch is out before shutdown starts waiting
    assert len(receiver.wait_for_requests(1)) == 1
    started = time.monotonic()
    processor.shutdown()
    assert time.monotonic() - started < 1
    # As the reference's client stops retrying once the processor shuts it down after its wait, and the processor
    # exports nothing more: the batch being retried and the rest of the queue are dropped and counted
    time.sleep(1.5)
    assert len(receiver.received) == 1
    assert _counters(processor) == (5, 0, 0)


def test_a_hung_collector_fails_the_batch_at_the_timeout(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, timeout=0.5)
    with receiver.stalled():
        took = _export(processor, 2)
    assert len(receiver.received) == 1
    assert 0.45 < took < 3
    assert _counters(processor) == (2, 0, 0)
    processor.shutdown()


def test_a_partial_success_counts_the_rejected_spans(receiver: FakeReceiver) -> None:
    partial = ExportTracePartialSuccess(rejected_spans=2, error_message="spans too old")
    receiver.reply(Reply(200, body=ExportTraceServiceResponse(partial_success=partial).SerializeToString()))
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor, 3)
    # Not retried, as the specification requires
    assert len(receiver.received) == 1
    assert _counters(processor) == (0, 2, 0)
    processor.shutdown()


def test_a_collector_that_refuses_connections_drops_the_batch_within_the_timeout() -> None:
    processor = OTLPSpanProcessor(endpoint=f"http://127.0.0.1:{_closed_port()}/v1/traces", timeout=0.5)
    took = _export(processor, 4)
    assert took < 2
    failed, rejected, retries = _counters(processor)
    assert (failed, rejected) == (4, 0)
    if sys.platform != "win32":
        # Refused, sent again at once, and then the backoff would end after the timeout. Windows tries a refused
        # connection again for about 2 s, so there the timeout ends the first attempt
        assert retries >= 1
    processor.shutdown()


def test_exports_resume_once_the_collector_is_back() -> None:
    port = _closed_port()
    processor = OTLPSpanProcessor(endpoint=f"http://127.0.0.1:{port}/v1/traces")
    processor.on_end(_span())
    with ThreadPoolExecutor(1) as pool:
        flushed = pool.submit(processor.force_flush)
        # Refused twice at once, then waiting about a second before the next attempt
        time.sleep(0.3)
        with FakeReceiver(port=port) as receiver:
            assert flushed.result(timeout=10)
            assert len(receiver.spans()) == 1
            failed, _, retries = _counters(processor)
            assert failed == 0
            if sys.platform != "win32":
                # Windows tries a refused connection again for about 2 s, by which time the receiver may be up
                assert retries >= 2
            _export(processor)
            assert len(receiver.spans()) == 2
    processor.shutdown()


def test_connections_are_reused_across_exports_and_retries(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor)
    receiver.reply(Reply(503, {"Retry-After": "0"}))
    _export(processor)
    _export(processor)
    assert len({received.port for received in receiver.received}) == 1
    assert len(receiver.received) == 4
    processor.shutdown()


@pytest.mark.parametrize(("compression", "magic"), [("gzip", b"\x1f\x8b"), ("deflate", b"\x78")])
def test_a_compressed_request_decodes_to_the_same_payload(
    receiver: FakeReceiver, compression: str, magic: bytes
) -> None:
    span = _span()
    for given in ("none", compression):
        processor = OTLPSpanProcessor(endpoint=receiver.endpoint, compression=given)
        processor.on_end(span)
        processor.shutdown()
    plain, compressed = receiver.received
    assert "content-encoding" not in plain.headers
    assert compressed.headers["content-encoding"] == compression
    assert compressed.body.startswith(magic) and compressed.body != plain.body
    assert compressed.request == plain.request


def test_compression_from_the_environment(receiver: FakeReceiver, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", "gzip")
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    _export(processor)
    [received] = receiver.received
    assert received.headers["content-encoding"] == "gzip" and received.body.startswith(b"\x1f\x8b")
    processor.shutdown()


def test_a_content_encoding_header_given_is_sent_as_it_is(receiver: FakeReceiver) -> None:
    # As by the reference, whose client adds its own only when the headers have none
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, compression="gzip", headers={"Content-Encoding": "gzip"})
    _export(processor)
    [received] = receiver.received
    assert received.headers["content-encoding"] == "gzip"
    processor.shutdown()


@pytest.fixture
def ca() -> "trustme.CA":
    # The lowest job of CI, and pre-release Pythons without a cryptography wheel, run without trustme
    module = pytest.importorskip("trustme")
    created: trustme.CA = module.CA()
    return created


def _server_context(ca: "trustme.CA", client_ca: "trustme.CA | None" = None) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ca.issue_cert("127.0.0.1").configure_cert(context)
    if client_ca is not None:
        # mTLS: a client without a certificate this CA issued is refused
        context.verify_mode = ssl.CERT_REQUIRED
        client_ca.configure_trust(context)
    return context


@pytest.fixture
def tls_receiver(ca: "trustme.CA") -> Iterator[FakeReceiver]:
    with FakeReceiver(tls=_server_context(ca)) as running:
        yield running


@pytest.mark.parametrize("by", ["argument", "variable"])
def test_a_custom_ca_verifies_the_server(
    tls_receiver: FakeReceiver, ca: "trustme.CA", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, by: str
) -> None:
    ca.cert_pem.write_to_path(str(tmp_path / "ca.pem"))
    kwargs: dict[str, Any] = {}
    if by == "argument":
        kwargs["certificate_file"] = str(tmp_path / "ca.pem")
    else:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", str(tmp_path / "ca.pem"))
    assert tls_receiver.endpoint.startswith("https://")
    processor = OTLPSpanProcessor(endpoint=tls_receiver.endpoint, **kwargs)
    _export(processor)
    assert len(tls_receiver.spans()) == 1 and _counters(processor) == (0, 0, 0)
    processor.shutdown()


@pytest.mark.parametrize("trusting", ["the OS trust store", "another CA"])
def test_an_untrusted_server_is_refused(tls_receiver: FakeReceiver, tmp_path: Path, trusting: str) -> None:
    trustme = pytest.importorskip("trustme")
    kwargs: dict[str, Any] = {}
    if trusting == "another CA":
        trustme.CA().cert_pem.write_to_path(str(tmp_path / "other.pem"))
        kwargs["certificate_file"] = str(tmp_path / "other.pem")
    processor = OTLPSpanProcessor(endpoint=tls_receiver.endpoint, timeout=1, **kwargs)
    took = _export(processor, 2)
    assert tls_receiver.received == []
    assert took < 3
    # A TLS failure is retried within the timeout, as requests' SSLError is a ConnectionError for the reference
    assert _counters(processor)[:2] == (2, 0)
    processor.shutdown()


@pytest.mark.parametrize("key", ["in its own file", "in the certificate's file"])
def test_mtls_presents_the_client_certificate(ca: "trustme.CA", tmp_path: Path, key: str) -> None:
    ca.cert_pem.write_to_path(str(tmp_path / "ca.pem"))
    client = ca.issue_cert("client.example")
    kwargs: dict[str, Any] = {"certificate_file": str(tmp_path / "ca.pem")}
    if key == "in its own file":
        client.cert_chain_pems[0].write_to_path(str(tmp_path / "client.pem"))
        client.private_key_pem.write_to_path(str(tmp_path / "client.key"))
        kwargs["client_certificate_file"] = str(tmp_path / "client.pem")
        kwargs["client_key_file"] = str(tmp_path / "client.key")
    else:
        client.private_key_and_cert_chain_pem.write_to_path(str(tmp_path / "client.pem"))
        kwargs["client_certificate_file"] = str(tmp_path / "client.pem")
    with FakeReceiver(tls=_server_context(ca, client_ca=ca)) as receiver:
        processor = OTLPSpanProcessor(endpoint=receiver.endpoint, **kwargs)
        _export(processor)
        assert len(receiver.spans()) == 1 and _counters(processor) == (0, 0, 0)
        processor.shutdown()

        # Without the client certificate the server refuses the connection
        without = OTLPSpanProcessor(endpoint=receiver.endpoint, certificate_file=kwargs["certificate_file"], timeout=1)
        _export(without)
        assert len(receiver.spans()) == 1 and _counters(without)[0] == 1
        without.shutdown()


def test_tls_files_that_cannot_be_used_raise(ca: "trustme.CA", tmp_path: Path) -> None:
    # Where the reference would fail every export (ADR 0006)
    endpoint = "https://127.0.0.1:4318/v1/traces"
    ca.cert_pem.write_to_path(str(tmp_path / "ca.pem"))
    client = ca.issue_cert("client.example")
    client.cert_chain_pems[0].write_to_path(str(tmp_path / "client.pem"))
    ca.issue_cert("other.example").private_key_pem.write_to_path(str(tmp_path / "other.key"))
    (tmp_path / "empty.pem").write_text("no certificate here\n")

    with pytest.raises(FileNotFoundError):
        OTLPSpanProcessor(endpoint=endpoint, certificate_file=str(tmp_path / "missing.pem"))
    with pytest.raises(ValueError, match="certificate_file holds no certificate"):
        OTLPSpanProcessor(endpoint=endpoint, certificate_file=str(tmp_path / "empty.pem"))
    with pytest.raises(ValueError, match="client_certificate_file holds no private key"):
        OTLPSpanProcessor(endpoint=endpoint, client_certificate_file=str(tmp_path / "client.pem"))
    with pytest.raises(ValueError, match="does not go with the client key"):
        OTLPSpanProcessor(
            endpoint=endpoint,
            client_certificate_file=str(tmp_path / "client.pem"),
            client_key_file=str(tmp_path / "other.key"),
        )
    # A key without a certificate is not used, as by the reference
    OTLPSpanProcessor(endpoint=endpoint, client_key_file=str(tmp_path / "missing.key")).shutdown()
    # An http:// endpoint does not read them, as requests does not
    plain = "http://127.0.0.1:4318/v1/traces"
    OTLPSpanProcessor(endpoint=plain, certificate_file=str(tmp_path / "missing.pem")).shutdown()
