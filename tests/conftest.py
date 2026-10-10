from collections.abc import Iterator

import pytest
from receiver import FakeReceiver


@pytest.fixture
def receiver() -> Iterator[FakeReceiver]:
    with FakeReceiver() as running:
        yield running
