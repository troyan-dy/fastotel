import json
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pytest
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

from bench import table, workload
from bench.scenarios import BASELINE, SCENARIOS
from bench.sink import TRACES_PATH, running_sink

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def sink() -> Iterator[str]:
    with running_sink() as url:
        yield url


def _stats(sink: str) -> dict[str, int]:
    with urllib.request.urlopen(sink + "/stats") as response:  # noqa: S310 - the local sink
        stats: dict[str, int] = json.load(response)
        return stats


def _post(url: str, body: bytes) -> int:
    headers = {"Content-Type": "application/x-protobuf"}
    request = urllib.request.Request(url, data=body, headers=headers)  # noqa: S310 - the local sink
    try:
        with urllib.request.urlopen(request) as response:  # noqa: S310
            return int(response.status)
    except urllib.error.HTTPError as error:
        return error.code


def test_sink_accepts_and_discards_traces(sink: str) -> None:
    before = _stats(sink)

    assert _post(sink + TRACES_PATH, b"\x0a\x00") == 200
    assert _post(sink + "/v1/metrics", b"") == 404

    after = _stats(sink)
    assert after["requests"] - before["requests"] == 1
    assert after["bytes"] - before["bytes"] == 2


@pytest.mark.parametrize("name", SCENARIOS)
def test_scenario_runs_the_hot_path_and_the_workload(name: str, sink: str) -> None:
    setup = SCENARIOS[name].setup(sink)
    try:
        tracer = setup.provider.get_tracer("fastotel.bench")
        assert workload.hot_path(100, tracer) > 0
        assert workload.paced(100, tracer, 1_000).seconds > 0
    finally:
        setup.shutdown()


def test_otlp_scenario_exports_to_the_sink(sink: str) -> None:
    setup = SCENARIOS["sdk-otlp"].setup(sink)
    before = _stats(sink)
    try:
        workload.hot_path(10, setup.provider.get_tracer("fastotel.bench"))
        assert isinstance(setup.provider, SdkTracerProvider)
        assert setup.provider.force_flush()
    finally:
        setup.shutdown()
    assert _stats(sink)["requests"] > before["requests"]


def test_paced_creates_spans_at_the_rate() -> None:
    tracer = SCENARIOS[BASELINE].setup("").provider.get_tracer("fastotel.bench")
    usage = workload.paced(20_000, tracer, 5_000)

    assert usage.spans == pytest.approx(usage.seconds * 5_000, abs=2)
    assert usage.workload_cpu > 0
    assert usage.background_cpu == pytest.approx(usage.process_cpu - usage.workload_cpu)


def _row(scenario: str, python: str, gil: bool, ops: dict[int, float], cpu: float | None = None) -> table.Row:
    throughput = {rate: table.Measure(value, 0.01) for rate, value in ops.items()}
    return table.Row(python, gil, scenario, table.Measure(1000.0, 0.02), throughput, cpu)


def test_table_shows_every_scenario_against_the_baseline() -> None:
    rows = [
        _row("api", "3.14.6", True, {5_000: 100_000.0}),
        _row("sdk-otlp", "3.14.6", True, {5_000: 90_000.0}, cpu=12.5),
    ]

    lines = table.render(rows).splitlines()

    assert lines[0].startswith("| Python | Scenario | Hot path, ns/span |")
    assert "at 5k spans/s" in lines[0]
    assert SCENARIOS["api"].title in lines[2]
    assert "100,000 ±1.0%" in lines[2]
    assert "90,000 ±1.0% (-10.0%)" in lines[3]
    assert "12.5" in lines[3]


@pytest.mark.parametrize(("ops", "go"), [(97_000.0, True), (97_100.0, False)])
def test_verdict_is_go_from_3_percent_at_5k_spans_on_the_gil_build(ops: float, go: bool) -> None:
    rows = [
        _row("api", "3.14.6", True, {5_000: 100_000.0}),
        _row("sdk-otlp", "3.14.6", True, {5_000: ops}),
        # The free-threaded build does not decide
        _row("api", "3.14.6t", False, {5_000: 100_000.0}),
        _row("sdk-otlp", "3.14.6t", False, {5_000: 50_000.0}),
    ]

    decision = table.verdict(rows)

    assert decision is not None
    assert decision.go is go
    assert decision.python == "3.14.6"


def test_make_bench_runs_the_scenarios_and_prints_one_table(tmp_path: Path) -> None:
    output = tmp_path / "results.json"
    # One value of one loop per benchmark: the commands of `make bench`, in seconds instead of minutes
    run = [sys.executable, "-m", "bench.run", "--debug-single-value", "--quiet", "--output", str(output)]
    subprocess.run(run, cwd=ROOT, check=True, capture_output=True)  # noqa: S603

    printed = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "bench.table", str(output)], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout

    table_lines = [line for line in printed.splitlines() if line.startswith("|")]
    assert len(table_lines) == 2 + len(SCENARIOS)
    for scenario in SCENARIOS.values():
        assert scenario.title in printed
    assert "Decision:" in printed
