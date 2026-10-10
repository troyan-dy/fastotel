"""
What the benchmarks time: the hot path of one span, and a CPU-bound workload that creates spans at a fixed rate.
"""

from dataclasses import dataclass
from time import perf_counter, process_time, thread_time

from opentelemetry.trace import Tracer


def span(tracer: Tracer) -> None:
    """
    The hot path: start a span, set 5 attributes of the common types, end it.
    """
    current = tracer.start_span("GET /items/{id}")
    current.set_attribute("http.request.method", "GET")
    current.set_attribute("url.path", "/items/42")
    current.set_attribute("http.response.status_code", 200)
    current.set_attribute("app.cache.hit", True)
    current.set_attribute("app.price", 9.99)
    current.end()


def hot_path(loops: int, tracer: Tracer) -> float:
    """
    Seconds that `loops` spans take, back to back.
    """
    start = perf_counter()
    for _ in range(loops):
        span(tracer)
    return perf_counter() - start


def work() -> int:
    """
    One unit of the CPU-bound workload, about 10 µs of pure Python on an Apple M2.
    """
    total = 0
    for i in range(300):
        total += i * i % 7
    return total


@dataclass(frozen=True)
class Usage:
    """
    One run of the workload: wall time, spans created, and CPU time of the whole process and of the thread
    that ran the workload. The rest of the process CPU is the exporter's: the SDK runs no other thread.
    """

    seconds: float
    spans: int
    process_cpu: float
    workload_cpu: float

    @property
    def background_cpu(self) -> float:
        return self.process_cpu - self.workload_cpu


def paced(loops: int, tracer: Tracer, rate: int) -> Usage:
    """
    Run `loops` units of work and create `rate` spans per second of wall time along the way.
    """
    created = 0
    start, process_start, thread_start = perf_counter(), process_time(), thread_time()
    for _ in range(loops):
        work()
        due = int((perf_counter() - start) * rate)
        while created < due:
            span(tracer)
            created += 1
    seconds = perf_counter() - start
    return Usage(seconds, created, process_time() - process_start, thread_time() - thread_start)
