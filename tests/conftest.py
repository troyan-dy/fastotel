# A TracerProvider asks resource detectors through a thread pool in a forked child. The pool's module takes a lock
# before a fork and reinitialises it in the child, so it has to be imported before the first provider registers its
# own handler, or the child deadlocks on that lock: TracerProvider(resource=...) does not import it
import concurrent.futures.thread  # noqa: F401, WPS301 - imported for its fork handler, as said above
from collections.abc import Iterator

import pytest
from receiver import FakeReceiver

collect_ignore: list[str] = []
try:
    from opentelemetry.exporter.otlp.proto import http  # noqa: F401
except ImportError:
    # The lowest job of CI runs an SDK older than the reference exporter the compatibility tests compare with
    collect_ignore.append("test_compatibility.py")


@pytest.fixture
def receiver() -> Iterator[FakeReceiver]:
    with FakeReceiver() as running:
        yield running
