"""
The local OTLP/HTTP receiver of the benchmarks: accepts `POST /v1/traces`, answers 200 and counts the spans.

It runs in its own process, so its work never competes with the benchmark for the GIL:

    python -m bench.sink    serve on a free port of 127.0.0.1 and print the port on the first line

`GET /stats` returns the requests, bytes and spans received so far.
"""

import json
import queue
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

TRACES_PATH = "/v1/traces"


class _TracesHandler(BaseHTTPRequestHandler):
    # Keep-alive, as a collector does: the exporter's session reuses one connection
    protocol_version = "HTTP/1.1"
    server: "_Sink"

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path != TRACES_PATH:
            self._reply(HTTPStatus.NOT_FOUND)
            return
        self.server.received.put(body)
        # An empty body is an empty ExportTraceServiceResponse: everything accepted
        self._reply(HTTPStatus.OK, content_type="application/x-protobuf")

    def do_GET(self) -> None:
        if self.path != "/stats":
            self._reply(HTTPStatus.NOT_FOUND)
            return
        self.server.received.join()
        stats = {
            "requests": self.server.requests,
            "bytes": self.server.received_bytes,
            "spans": self.server.spans,
        }
        self._reply(HTTPStatus.OK, json.dumps(stats).encode(), content_type="application/json")

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the signature of the base class
        """
        Logs nothing: a line per request would land in the benchmark's output.
        """

    def _reply(self, status: int, body: bytes = b"", content_type: str = "text/plain") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _Sink(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _TracesHandler)
        self.received: queue.Queue[bytes] = queue.Queue()
        self.requests = 0
        self.received_bytes = 0
        self.spans = 0
        # Decoding off the request thread: the exporter gets its answer as soon as the body is read
        threading.Thread(target=self._count, daemon=True).start()

    def _count(self) -> None:
        while True:  # noqa: WPS457 - a daemon thread, for as long as the sink serves
            body = self.received.get()
            request = ExportTraceServiceRequest.FromString(body)
            self.requests += 1
            self.received_bytes += len(body)
            scopes = (scope for resource in request.resource_spans for scope in resource.scope_spans)
            self.spans += sum(len(scope.spans) for scope in scopes)
            self.received.task_done()


@contextmanager
def running_sink() -> Iterator[str]:
    """
    Start the sink in a child process; yield its base URL, e.g. `http://127.0.0.1:50000`.
    """
    command = [sys.executable, "-m", "bench.sink"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)  # noqa: S603 - this interpreter
    try:
        yield f"http://127.0.0.1:{_port(process)}"
    finally:
        process.terminate()
        process.wait()


def _port(process: "subprocess.Popen[str]") -> int:
    # The first line the sink prints
    assert process.stdout is not None
    return int(process.stdout.readline())


def stats(sink: str) -> dict[str, int]:
    """
    What the sink at `sink` has received, once it has counted every request it answered.
    """
    with urlopen(f"{sink}/stats") as response:  # noqa: S310 - the local sink
        received: dict[str, int] = json.load(response)
        return received


def main() -> None:
    sink = _Sink()
    print(sink.server_address[1], flush=True)
    sink.serve_forever()


if __name__ == "__main__":
    main()
