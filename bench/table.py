"""
Prints the results of `bench.run` as one Markdown table, followed by the go/no-go decision of #6:

    python -m bench.table FILE...    one FILE per Python build
"""

import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import pyperf

from bench.run import cpu_log_of, lost_of
from bench.scenarios import BASELINE, SCENARIOS

# fastotel pays off when the SDK with the OTLP exporter costs the workload at least this share of its throughput
# at this span rate, on a build with the GIL (#6)
DECISION_SCENARIO = "sdk-otlp"
DECISION_RATE = 5_000
GO_THRESHOLD = 0.03
NANOSECONDS = 1e9
MICROSECONDS = 1e6
_WORKLOAD = re.compile(r"workload@(\d+)")


class Measure(NamedTuple):
    mean: float
    relative_stdev: float


class Row(NamedTuple):
    python: str
    gil: bool
    scenario: str
    hot_path_ns: Measure
    # Units of work per second, by span rate
    throughput: dict[int, Measure]
    # CPU time of the threads other than the workload's, per span, at DECISION_RATE; None without spans
    exporter_us_per_span: float | None
    # Share of the spans that never reach the sink, by span rate; None when the scenario sends it nothing
    lost: dict[int, float | None]


class Verdict(NamedTuple):
    python: str
    cost: float
    go: bool


def _measure(bench: pyperf.Benchmark, scale: float = 1.0, *, invert: bool = False) -> Measure:
    mean = bench.mean() if bench.get_nvalue() else 0
    stdev = bench.stdev() if bench.get_nvalue() > 1 else 0
    relative = stdev / mean if mean else 0
    if invert:
        return Measure(scale / mean if mean else 0, relative)
    return Measure(mean * scale, relative)


def load(path: Path) -> list[Row]:
    """
    The rows of one result file of `bench.run`, in the order of SCENARIOS.
    """
    suite = pyperf.BenchmarkSuite.load(str(path))
    hot_path: dict[str, Measure] = {}
    throughput: dict[str, dict[int, Measure]] = defaultdict(dict)
    metadata: dict[str, object] = {}
    for bench in suite.get_benchmarks():
        metadata = bench.get_metadata()
        benchmarked, _, kind = bench.get_name().partition("/")
        workload = _WORKLOAD.fullmatch(kind)
        if kind == "hot-path":
            hot_path[benchmarked] = _measure(bench, NANOSECONDS)
        elif workload:
            throughput[benchmarked][int(workload.group(1))] = _measure(bench, invert=True)

    # At one rate: at 10k spans/s the SDK drops spans it then never encodes, which would understate the cost
    background: dict[str, float] = defaultdict(float)
    spans: dict[str, int] = defaultdict(int)
    gil = metadata.get("fastotel_gil", "on") == "on"
    log = cpu_log_of(str(path))
    if log.exists():
        for line in log.read_text().splitlines():
            record = json.loads(line)
            gil = gil or record.get("gil", False)
            if record["rate"] == DECISION_RATE:
                background[record["scenario"]] += record["process_cpu"] - record["workload_cpu"]
                spans[record["scenario"]] += record["spans"]

    lost: dict[str, dict[int, float | None]] = defaultdict(dict)
    if lost_of(str(path)).exists():
        for measured, by_rate in json.loads(lost_of(str(path)).read_text()).items():
            lost[measured] = {int(rate): share for rate, share in by_rate.items()}

    python = str(metadata.get("fastotel_python", metadata.get("python_version", "?")))
    return [
        Row(
            python,
            gil,
            scenario,
            hot_path[scenario],
            throughput[scenario],
            background[scenario] / spans[scenario] * MICROSECONDS if spans[scenario] else None,
            lost[scenario],
        )
        for scenario in SCENARIOS
        if scenario in hot_path
    ]


def _rate(rate: int) -> str:
    thousands, rest = divmod(rate, 1000)
    return str(rate) if rest else f"{thousands}k"


def _share(share: float | None) -> str:
    return "" if share is None else f"{max(share, 0):.1%}"


def _python(row: Row) -> str:
    gil = "on" if row.gil else "off"
    return f"{row.python} (GIL {gil})"


def render(rows: list[Row]) -> str:
    rates = sorted({rate for row in rows for rate in row.throughput})
    header = ["Python", "Scenario", "Hot path, ns/span"]
    header += [f"Workload, ops/s at {_rate(rate)} spans/s" for rate in rates]
    header += [f"Exporter threads, CPU µs/span at {_rate(DECISION_RATE)} spans/s"]
    lost_rates = sorted({rate for row in rows for rate in row.lost})
    if lost_rates:
        shown = " / ".join(_rate(rate) for rate in lost_rates)
        header += [f"Spans lost at {shown} spans/s"]
    baselines = {row.python: row for row in rows if row.scenario == BASELINE}

    lines = [_cells(header), f"|{'---|' * len(header)}"]
    for row in rows:
        cells = [_python(row), SCENARIOS[row.scenario].title]
        cells.append(_spread(row.hot_path_ns))
        baseline = baselines.get(row.python)
        for rate in rates:
            measure = row.throughput.get(rate)
            if measure is None:
                cells.append("")
                continue
            reference = baseline.throughput.get(rate) if baseline and row is not baseline else None
            cells.append(_against(measure, reference))
        cells.append(_exporter_cpu(row.exporter_us_per_span))
        if lost_rates:
            shares = [row.lost.get(lost_rate) for lost_rate in lost_rates]
            cells.append("" if all(share is None for share in shares) else " / ".join(map(_share, shares)))
        lines.append(_cells(cells))
    return "\n".join(lines)


def _cells(cells: list[str]) -> str:
    joined = " | ".join(cells)
    return f"| {joined} |"


def _spread(measure: Measure) -> str:
    mean, relative_stdev = measure
    return f"{mean:,.0f} ±{relative_stdev:.1%}"


def _against(measure: Measure, reference: Measure | None) -> str:
    # The measure, and how it compares with the baseline's
    if reference and reference.mean:
        change = measure.mean / reference.mean - 1
        return f"{_spread(measure)} ({change:+.1%})"
    return _spread(measure)


def _exporter_cpu(us_per_span: float | None) -> str:
    return "" if us_per_span is None else f"{max(us_per_span, 0):.1f}"


def verdict(rows: list[Row]) -> Verdict | None:
    """
    The decision of #6, on the first build with the GIL that has both the baseline and the decision scenario.
    """
    for baseline in rows:
        if not baseline.gil or baseline.scenario != BASELINE:
            continue
        for row in rows:
            if row.python == baseline.python and row.scenario == DECISION_SCENARIO:
                reference = baseline.throughput.get(DECISION_RATE)
                measure = row.throughput.get(DECISION_RATE)
                if reference and measure and reference.mean:
                    cost = 1 - measure.mean / reference.mean
                    return Verdict(baseline.python, cost, cost >= GO_THRESHOLD)
    return None


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    rows = [row for path in argv for row in load(Path(path))]
    print(render(rows))
    print()
    decision = verdict(rows)
    if decision is None:
        print("Decision: none, the results lack the GIL build or a scenario")
    else:
        go = "go" if decision.go else "no-go"
        scenario = SCENARIOS[DECISION_SCENARIO]
        baseline = SCENARIOS[BASELINE]
        print(
            f"Decision: {go}. {scenario.title} costs the workload {decision.cost:.1%} of its throughput at "
            f"{_rate(DECISION_RATE)} spans/s on Python {decision.python} against {baseline.title}; the bar is "
            f"{GO_THRESHOLD:.0%}."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
