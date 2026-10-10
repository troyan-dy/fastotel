"""
A fake OTLP/HTTP receiver for the tests: an HTTP server in the test process that decodes every request with
`opentelemetry-proto`, the way a collector would, over plain HTTP or TLS, and answers as it is told to.
"""

import gzip
import socket
import ssl
import sys
import threading
import time
import zlib
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
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
    # Decompressed as its Content-Encoding says
    request: ExportTraceServiceRequest
    # As sent, compressed or not
    body: bytes
    # The client's port, which tells one connection from another
    port: int
    # time.monotonic() when the request arrived
    at: float


@dataclass(frozen=True)
class Reply:
    """
    An answer to give instead of the 200 with an empty body a collector gives when it accepts everything.
    """

    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


def _decompress(body: bytes, encoding: str | None) -> bytes:
    if encoding == "gzip":
        return gzip.decompress(body)
    if encoding == "deflate":
        return zlib.decompress(body)
    return body


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
        request = ExportTraceServiceRequest.FromString(_decompress(body, headers.get("content-encoding")))
        received = Received(self.path, headers, request, body, self.client_address[1], time.monotonic())
        with self.server.lock:
            self.server.received.append(received)
            # An empty body is an empty ExportTraceServiceResponse: everything accepted
            reply = self.server.replies.popleft() if self.server.replies else Reply()
        self.server.answering.wait()
        self._reply(reply.status, "application/x-protobuf", reply.headers, reply.body)

    def _reply(
        self, status: int, content_type: str = "text/plain", headers: dict[str, str] | None = None, body: bytes = b""
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the signature of the base class
        pass


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port: int, tls: ssl.SSLContext | None) -> None:
        super().__init__(("127.0.0.1", port), _Handler)
        self.tls = tls
        self.lock = threading.Lock()
        self.received: list[Received] = []
        self.replies: deque[Reply] = deque()
        # Cleared while the receiver is stalled
        self.answering = threading.Event()
        self.answering.set()

    def get_request(self) -> tuple[socket.socket, object]:
        connection, address = super().get_request()
        if self.tls is None:
            return connection, address
        # The handshake happens in the connection's own thread, so that a client failing it holds nothing up
        return self.tls.wrap_socket(connection, server_side=True, do_handshake_on_connect=False), address

    def finish_request(self, request: socket.socket | tuple[bytes, socket.socket], client_address: object) -> None:
        if isinstance(request, ssl.SSLSocket):
            try:
                request.do_handshake()
            except OSError:
                # A client that does not trust the server, or that the server does not trust
                return
        super().finish_request(request, client_address)

    def handle_error(self, request: object, client_address: object) -> None:
        # A client that gave up on a stalled answer has closed its connection; anything else is a bug here
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)  # type: ignore[arg-type]


class FakeReceiver:
    """
    Serves on 127.0.0.1 while in its `with` block: on a free port unless given one, over TLS when given a server
    context.
    """

    def __init__(self, port: int = 0, tls: ssl.SSLContext | None = None) -> None:
        self._server = _Server(port, tls)
        self._thread = threading.Thread(target=self._server.serve_forever, args=(0.01,), daemon=True)

    @property
    def endpoint(self) -> str:
        """
        The URL to export spans to, `/v1/traces` included.
        """
        scheme = "http" if self._server.tls is None else "https"
        return f"{scheme}://127.0.0.1:{self._server.server_address[1]}{TRACES_PATH}"

    def reply(self, *replies: Reply) -> None:
        """
        Answer the next requests with these, one each, and those after them as usual.
        """
        with self._server.lock:
            self._server.replies.extend(replies)

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
