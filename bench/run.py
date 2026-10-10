"""
The pyperf benchmarks of every scenario, on the interpreter that runs it:

    python -m bench.run --output FILE [pyperf options]

For each scenario: `<scenario>/hot-path`, seconds per span, and `<scenario>/workload@<rate>`, seconds per unit of
the CPU-bound workload while it creates `rate` spans per second. The CPU time of each workload run goes to
FILE with the `.cpu.jsonl` suffix, which pyperf has no place for. `python -m bench.table` prints both.
"""

import argparse
import atexit
import json
import platform
import sys
import sysconfig
from functools import cache
from pathlib import Path

import pyperf
from opentelemetry.trace import Tracer

from bench import workload
from bench.scenarios import SCENARIOS
from bench.sink import running_sink

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
    }
    with Path(cpu_log).open("a") as log:
        log.write(json.dumps(record) + "\n")
    return usage.seconds


def cpu_log_of(output: str) -> Path:
    return Path(output).with_suffix(".cpu.jsonl")


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
    args = runner.parse_args()
    gil_disabled = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    runner.metadata["fastotel_python"] = platform.python_version() + ("t" if gil_disabled else "")
    # A free-threaded build turns the GIL back on for an extension that does not declare itself safe
    runner.metadata["fastotel_gil"] = "on" if getattr(sys, "_is_gil_enabled", lambda: True)() else "off"

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


def _register(runner: pyperf.Runner, args: argparse.Namespace) -> None:
    for scenario in SCENARIOS:
        runner.bench_time_func(f"{scenario}/hot-path", _hot_path, scenario, args.sink)
        for rate in RATES:
            name = f"{scenario}/workload@{rate}"
            runner.bench_time_func(name, _workload, scenario, args.sink, rate, args.cpu_log)


if __name__ == "__main__":
    main()
