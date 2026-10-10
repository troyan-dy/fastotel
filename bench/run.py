"""
The pyperf benchmarks of every scenario, on the interpreter that runs it:

    python -m bench.run --output FILE [pyperf options]

For each scenario: `<scenario>/hot-path`, seconds per span, and `<scenario>/workload@<rate>`, seconds per unit of
the CPU-bound workload while it creates `rate` spans per second. pyperf has no place for two more results, so
they go next to FILE: the CPU time of each workload run (`.cpu.jsonl`), and the share of spans each scenario
loses at each rate (`.lost.json`), measured after the pyperf benchmarks. `python -m bench.table` prints them all.
"""

import argparse
import atexit
import json
import platform
import sys
import sysconfig
from functools import cache
from pathlib import Path
from time import perf_counter

import pyperf
from opentelemetry.trace import Tracer

from bench import workload
from bench.scenarios import SCENARIOS
from bench.sink import running_sink, stats

RATES = (1_000, 5_000, 10_000)


@cache
def _tracer(scenario: str, sink: str) -> Tracer:
    # Set up once per worker process, on the first call: outside the timed part, and only for the scenario
    # that the worker runs
    setup = SCENARIOS[scenario].setup(sink)
    atexit.register(setup.shutdown)
    return setup.provider.get_tracer("fastotel.bench")


def _hot_path(loops: int, scenario: str, sink: str) -> float:
    return workload.hot_path(loops, _tracer(scenario, sink))


def _workload(loops: int, scenario: str, sink: str, rate: int, cpu_log: str) -> float:
    usage = workload.paced(loops, _tracer(scenario, sink), rate)
    record = {
        "scenario": scenario,
        "rate": rate,
        "seconds": usage.seconds,
        "spans": usage.spans,
        "process_cpu": usage.process_cpu,
        "workload_cpu": usage.workload_cpu,
        # An extension imported on the first export may have turned the GIL back on since the start
        "gil": _gil_enabled(),
    }
    with Path(cpu_log).open("a") as log:
        log.write(json.dumps(record) + "\n")
    return usage.seconds


def lost(scenario: str, sink: str, rate: int, seconds: float) -> float | None:
    """
    The share of spans that `scenario` creates at `rate` for `seconds` and that never reach the sink, or None when
    it sends the sink nothing. Shutting the provider down exports what is left in the queue, so the rest is lost.
    """
    setup = SCENARIOS[scenario].setup(sink)
    tracer = setup.provider.get_tracer("fastotel.bench")
    before = stats(sink)["spans"]
    created = 0
    start = perf_counter()
    while perf_counter() - start < seconds:
        created += workload.paced(1_000, tracer, rate).spans
    setup.shutdown()
    received = stats(sink)["spans"] - before
    return 1 - received / created if received else None


def cpu_log_of(output: str) -> Path:
    return Path(output).with_suffix(".cpu.jsonl")


def lost_of(output: str) -> Path:
    return Path(output).with_suffix(".lost.json")


def _gil_enabled() -> bool:
    return bool(getattr(sys, "_is_gil_enabled", lambda: True)())


def _forward(cmd: list[str], args: argparse.Namespace) -> None:
    # pyperf starts each worker with a fresh command line: hand it the sink and the log
    cmd.extend(("--sink", args.sink, "--cpu-log", args.cpu_log))


def main() -> None:
    runner = pyperf.Runner(
        # One value lasts long enough to take in a few exports even at 1k spans/s (a batch is 512 spans)
        min_time=1.0,
        processes=12,
        program_args=("-m", "bench.run"),
        add_cmdline_args=_forward,
    )
    runner.argparser.add_argument("--sink", help=argparse.SUPPRESS)
    runner.argparser.add_argument("--cpu-log", help=argparse.SUPPRESS)
    runner.argparser.add_argument(
        "--lost-seconds", type=float, default=10.0, help="how long to run each scenario and rate to count lost spans"
    )
    args = runner.parse_args()
    gil_disabled = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    runner.metadata["fastotel_python"] = platform.python_version() + ("t" if gil_disabled else "")
    # A free-threaded build turns the GIL back on for an extension that does not declare itself safe
    runner.metadata["fastotel_gil"] = "on" if _gil_enabled() else "off"

    if args.worker:
        _register(runner, args)
        return
    if not args.output:
        runner.argparser.error("--output is required: bench.table reads the results from it")
    log = cpu_log_of(args.output)
    log.unlink(missing_ok=True)
    args.cpu_log = str(log)
    with running_sink() as sink:
        args.sink = sink
        _register(runner, args)
        # In this process, after the workers: nothing else runs on the machine meanwhile
        losses = {
            scenario: {rate: lost(scenario, sink, rate, args.lost_seconds) for rate in RATES} for scenario in SCENARIOS
        }
    lost_of(args.output).write_text(json.dumps(losses))


def _register(runner: pyperf.Runner, args: argparse.Namespace) -> None:
    for scenario in SCENARIOS:
        runner.bench_time_func(f"{scenario}/hot-path", _hot_path, scenario, args.sink)
        for rate in RATES:
            name = f"{scenario}/workload@{rate}"
            runner.bench_time_func(name, _workload, scenario, args.sink, rate, args.cpu_log)


if __name__ == "__main__":
    main()
