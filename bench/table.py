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
        for scenario, by_rate in json.loads(lost_of(str(path)).read_text()).items():
            lost[scenario] = {int(rate): share for rate, share in by_rate.items()}

    python = str(metadata.get("fastotel_python", metadata.get("python_version", "?")))
    return [
        Row(
            python,
            gil,
            scenario,
            hot_path[scenario],
            throughput[scenario],
            background[scenario] / spans[scenario] * 1e6 if spans[scenario] else None,
            lost[scenario],
        )
        for scenario in SCENARIOS
        if scenario in hot_path
    ]


def _rate(rate: int) -> str:
    return f"{rate // 1000}k" if rate % 1000 == 0 else str(rate)


def _share(share: float | None) -> str:
    return "" if share is None else f"{max(share, 0.0):.1%}"


def _python(row: Row) -> str:
    return f"{row.python} (GIL {'on' if row.gil else 'off'})"


def render(rows: list[Row]) -> str:
    rates = sorted({rate for row in rows for rate in row.throughput})
    header = ["Python", "Scenario", "Hot path, ns/span"]
    header += [f"Workload, ops/s at {_rate(rate)} spans/s" for rate in rates]
    header += [f"Exporter threads, CPU µs/span at {_rate(DECISION_RATE)} spans/s"]
    lost_rates = sorted({rate for row in rows for rate in row.lost})
    if lost_rates:
        header += ["Spans lost at " + " / ".join(_rate(rate) for rate in lost_rates) + " spans/s"]
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
        if lost_rates:
            shares = [row.lost.get(rate) for rate in lost_rates]
            cells.append("" if all(share is None for share in shares) else " / ".join(_share(s) for s in shares))
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
