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

from bench.run import cpu_log_of
from bench.scenarios import BASELINE, SCENARIOS

# fastotel pays off when the SDK with the OTLP exporter costs the workload at least this share of its throughput
# at this span rate, on a build with the GIL (#6)
DECISION_SCENARIO = "sdk-otlp"
DECISION_RATE = 5_000
GO_THRESHOLD = 0.03


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
    # CPU time of the threads other than the workload's, per span; None when no span was created
    exporter_us_per_span: float | None


class Verdict(NamedTuple):
    python: str
    cost: float
    go: bool


def _measure(bench: pyperf.Benchmark, scale: float = 1.0, *, invert: bool = False) -> Measure:
    mean = bench.mean() if bench.get_nvalue() else 0.0
    stdev = bench.stdev() if bench.get_nvalue() > 1 else 0.0
    relative = stdev / mean if mean else 0.0
    if invert:
        return Measure(scale / mean if mean else 0.0, relative)
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
        scenario, _, kind = bench.get_name().partition("/")
        if kind == "hot-path":
            hot_path[scenario] = _measure(bench, 1e9)
        elif match := re.fullmatch(r"workload@(\d+)", kind):
            throughput[scenario][int(match.group(1))] = _measure(bench, invert=True)

    background: dict[str, float] = defaultdict(float)
    spans: dict[str, int] = defaultdict(int)
    log = cpu_log_of(str(path))
    if log.exists():
        for line in log.read_text().splitlines():
            record = json.loads(line)
            background[record["scenario"]] += record["process_cpu"] - record["workload_cpu"]
            spans[record["scenario"]] += record["spans"]

    python = str(metadata.get("fastotel_python", metadata.get("python_version", "?")))
    gil = metadata.get("fastotel_gil", "on") == "on"
    return [
        Row(
            python,
            gil,
            scenario,
            hot_path[scenario],
            throughput[scenario],
            background[scenario] / spans[scenario] * 1e6 if spans[scenario] else None,
        )
        for scenario in SCENARIOS
        if scenario in hot_path
    ]


def _rate(rate: int) -> str:
    return f"{rate // 1000}k" if rate % 1000 == 0 else str(rate)


def _python(row: Row) -> str:
    return f"{row.python} (GIL {'on' if row.gil else 'off'})"


def render(rows: list[Row]) -> str:
    rates = sorted({rate for row in rows for rate in row.throughput})
    header = ["Python", "Scenario", "Hot path, ns/span"]
    header += [f"Workload, ops/s at {_rate(rate)} spans/s" for rate in rates]
    header += ["Exporter threads, CPU µs/span"]
    baselines = {row.python: row for row in rows if row.scenario == BASELINE}

    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for row in rows:
        cells = [_python(row), SCENARIOS[row.scenario].title]
        cells.append(f"{row.hot_path_ns.mean:,.0f} ±{row.hot_path_ns.relative_stdev:.1%}")
        baseline = baselines.get(row.python)
        for rate in rates:
            measure = row.throughput.get(rate)
            if measure is None:
                cells.append("")
                continue
            cell = f"{measure.mean:,.0f} ±{measure.relative_stdev:.1%}"
            reference = baseline.throughput.get(rate) if baseline and row is not baseline else None
            if reference and reference.mean:
                cell += f" ({measure.mean / reference.mean - 1:+.1%})"
            cells.append(cell)
        cells.append("" if row.exporter_us_per_span is None else f"{max(row.exporter_us_per_span, 0.0):.1f}")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


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
        print(
            f"Decision: {'go' if decision.go else 'no-go'}. {SCENARIOS[DECISION_SCENARIO].title} costs the workload "
            f"{decision.cost:.1%} of its throughput at {_rate(DECISION_RATE)} spans/s on Python {decision.python} "
            f"against {SCENARIOS[BASELINE].title}; the bar is {GO_THRESHOLD:.0%}."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
