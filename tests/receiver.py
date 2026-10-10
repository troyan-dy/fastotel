"""
A fake OTLP/HTTP receiver for the tests: an HTTP server in the test process that decodes every request with
`opentelemetry-proto`, the way a collector would.
"""

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import TracebackType
from typing import Self

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import Span

TRACES_PATH = "/v1/traces"


@dataclass(frozen=True)
class Received:
    path: str
    # Lower-cased names: HTTP/1.1 does not care, and clients differ
    headers: dict[str, str]
    request: ExportTraceServiceRequest


class _Handler(BaseHTTPRequestHandler):
    # Keep-alive, as a collector does
    protocol_version = "HTTP/1.1"
    server: "_Server"

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path != TRACES_PATH:
            self._reply(404)
            return
        # Recorded before the answer: once an export returns, the test sees what it sent
        headers = {name.lower(): value for name, value in self.headers.items()}
        received = Received(self.path, headers, ExportTraceServiceRequest.FromString(body))
        with self.server.lock:
            self.server.received.append(received)
        self.server.answering.wait()
        # An empty body is an empty ExportTraceServiceResponse: everything accepted
        self._reply(200, content_type="application/x-protobuf")

    def _reply(self, status: int, content_type: str = "text/plain") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the signature of the base class
        pass


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        self.received: list[Received] = []
        # Cleared while the receiver is stalled
        self.answering = threading.Event()
        self.answering.set()


class FakeReceiver:
    """
    Serves on a free port of 127.0.0.1 while in its `with` block.
    """

    def __init__(self) -> None:
        self._server = _Server()
        self._thread = threading.Thread(target=self._server.serve_forever, args=(0.01,), daemon=True)

    @property
    def endpoint(self) -> str:
        """
        The URL to export spans to, `/v1/traces` included.
        """
        return f"http://127.0.0.1:{self._server.server_address[1]}{TRACES_PATH}"

    @property
    def received(self) -> list[Received]:
        """
        Every request received so far, decoded.
        """
        with self._server.lock:
            return list(self._server.received)

    @property
    def requests(self) -> list[ExportTraceServiceRequest]:
        return [received.request for received in self.received]

    def spans(self) -> list[Span]:
        """
        Every span received so far, in the order they arrived.
        """
        return [
            span
            for request in self.requests
            for resource_spans in request.resource_spans
            for scope_spans in resource_spans.scope_spans
            for span in scope_spans.spans
        ]

    def wait_for_requests(self, count: int, timeout: float = 10) -> list[ExportTraceServiceRequest]:
        """
        The requests received once there are at least `count`, or all of them after `timeout` seconds.
        """
        deadline = time.monotonic() + timeout
        while len(requests := self.requests) < count and time.monotonic() < deadline:
            time.sleep(0.005)
        return requests

    @contextmanager
    def stalled(self) -> Iterator[None]:
        """
        Records requests as they arrive but holds every answer back until the end of the block, as a collector
        that has stopped answering.
        """
        self._server.answering.clear()
        try:
            yield
        finally:
            self._server.answering.set()

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self._server.answering.set()
        self._server.shutdown()
        self._server.server_close()
