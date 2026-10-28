#!/usr/bin/env python3
"""Run the OpenTelemetry overhead benchmark (DRIVERS-3620) for the Java driver.

Each configuration runs in its own JVM via ``:driver-benchmarks:runOtel``. The whole set of
configurations is run REPS times, interleaved, with the order rotated by one position each
repetition, as the OpenTelemetry spec's Benchmarking section requires. The ``pre-otel``
configuration runs the same tasks from a worktree checked out at the commit before tracing
support was added (the spec's one-time comparison of the default configuration).

Usage:
  otel-bench.py --data DIR --out DIR [--reps 5] [--pre-otel-worktree PATH]
                [--configs off,api-only,sdk-ratio,sdk-always,pre-otel]
                [--iterations 100] [--min-time 60] [--report-only]
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TASKS = [("sync", "Small doc insertOne"), ("sync", "Find one by ID"),
         ("async", "Small doc insertOne"), ("async", "Find one by ID")]
ALL = ["off", "api-only", "sdk-ratio", "sdk-always", "pre-otel"]
METRICS = ["megabytes_per_sec", "wall_us_per_op", "cpu_us_per_op", "gc_us_per_op", "cpu_ex_gc_us_per_op"]


def run_one(config, rep, args):
    out = args.out / f"{config}-rep{rep:02d}.json"
    log = out.with_suffix(".log")
    if out.exists() and args.resume:
        return out
    cwd = args.pre_otel_worktree if config == "pre-otel" else ROOT
    cmd = ["./gradlew", "-q", ":driver-benchmarks:runOtel",
           f"-Dorg.mongodb.benchmarks.data={args.data}/",
           f"-Dorg.mongodb.benchmarks.output={out}",
           f"-Dorg.mongodb.benchmarks.otel.config={config}",
           f"-Dorg.mongodb.benchmarks.otel.iterations={args.iterations}",
           f"-Dorg.mongodb.benchmarks.otel.minTimeSeconds={args.min_time}"]
    if config == "pre-otel":
        cmd += ["-x", ":mongodb-crypt:downloadJava", "-x", ":mongodb-crypt:unzipJava"]
    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}
    start = time.monotonic()
    with open(log, "w") as fh:
        proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=fh, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        print(f"FAILED {config} rep {rep}: see {log}", file=sys.stderr)
        print(log.read_text()[-3000:], file=sys.stderr)
        sys.exit(1)
    print(f"  {config:<10} rep {rep} done in {time.monotonic() - start:.0f}s", flush=True)
    return out


def load(args, configs):
    samples = {}  # (config, api, task) -> metric -> [values]
    for f in sorted(args.out.glob("*-rep*.json")):
        config = f.name.rsplit("-rep", 1)[0]
        if config not in configs:
            continue
        for r in json.loads(f.read_text())["results"]:
            d = samples.setdefault((config, r["api"], r["test_name"]), {m: [] for m in METRICS})
            for m in METRICS:
                d[m].append(r[m])
    return samples


def med(xs):
    return statistics.median(xs) if xs else float("nan")


def report(args, configs):
    samples = load(args, configs)
    reps = max((len(v["megabytes_per_sec"]) for v in samples.values()), default=0)
    lines = [f"# Java driver OpenTelemetry overhead (DRIVERS-3620), {reps} repetitions, medians\n"]
    lines.append("## Throughput (median MB/s; loss vs `off`)\n")
    lines.append("| API | Task | " + " | ".join(c for c in configs) + " |")
    lines.append("|---|---|" + "---|" * len(configs))
    for api, task in TASKS:
        base = med(samples.get(("off", api, task), {}).get("megabytes_per_sec", []))
        cells = []
        for c in configs:
            v = med(samples.get((c, api, task), {}).get("megabytes_per_sec", []))
            if c == "off":
                cells.append(f"{v:.3f} (baseline)")
            else:
                loss = (1 - v / base) * 100 if base == base else float("nan")
                cells.append(f"{v:.3f} ({loss:+.1f}%)")
        lines.append(f"| {api} | {task} | " + " | ".join(cells) + " |")
    lines.append("\nA negative loss means the configuration measured faster than `off`. "
                 "For `pre-otel` the percentage is the cost of the `off` configuration relative to the pre-tracing commit.\n")

    lines.append("## CPU time per operation, excluding GC (median µs/op; delta vs `off`)\n")
    lines.append("| API | Task | " + " | ".join(configs) + " |")
    lines.append("|---|---|" + "---|" * len(configs))
    for api, task in TASKS:
        base = med(samples.get(("off", api, task), {}).get("cpu_ex_gc_us_per_op", []))
        cells = []
        for c in configs:
            v = med(samples.get((c, api, task), {}).get("cpu_ex_gc_us_per_op", []))
            if c == "off":
                cells.append(f"{v:.1f} (baseline)")
            else:
                cells.append(f"{v:.1f} ({v - base:+.1f}, {(v / base - 1) * 100:+.1f}%)")
        lines.append(f"| {api} | {task} | " + " | ".join(cells) + " |")

    lines.append("\n## Wall time per operation (median µs/op)\n")
    lines.append("| API | Task | " + " | ".join(configs) + " |")
    lines.append("|---|---|" + "---|" * len(configs))
    for api, task in TASKS:
        cells = [f"{med(samples.get((c, api, task), {}).get('wall_us_per_op', [])):.1f}" for c in configs]
        lines.append(f"| {api} | {task} | " + " | ".join(cells) + " |")

    lines.append("\n## Throughput spread across repetitions (min–max MB/s)\n")
    lines.append("| API | Task | " + " | ".join(configs) + " |")
    lines.append("|---|---|" + "---|" * len(configs))
    for api, task in TASKS:
        cells = []
        for c in configs:
            xs = samples.get((c, api, task), {}).get("megabytes_per_sec", [])
            cells.append(f"{min(xs):.3f}–{max(xs):.3f}" if xs else "n/a")
        lines.append(f"| {api} | {task} | " + " | ".join(cells) + " |")

    text = "\n".join(lines) + "\n"
    (args.out / "REPORT.md").write_text(text)
    summary = {f"{c}/{api}/{task}": {m: {"median": med(v[m]), "values": v[m]} for m in METRICS}
               for (c, api, task), v in samples.items()}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2))
    return text


def write_dashboard(args, configs, out):
    """Evergreen performance-dashboard shape (see EvergreenBenchmarkResultWriter and send-dashboard-data).

    One entry per (configuration, api, task) with the absolute MB/s and CPU/op, plus, for every
    non-baseline configuration, the overhead relative to `off` as metrics of their own, which is what
    the OpenTelemetry spec asks drivers to record."""
    samples = load(args, configs)
    results = []
    for (config, api, task), v in sorted(samples.items()):
        name = f"OTel {config} {api} {task}"
        metrics = [
            {"name": "megabytes_per_sec", "type": "MEDIAN", "value": med(v["megabytes_per_sec"]),
             "metadata": {"improvement_direction": "up", "measurement_unit": "megabytes_per_second"}},
            {"name": "cpu_ex_gc_us_per_op", "type": "MEDIAN", "value": med(v["cpu_ex_gc_us_per_op"]),
             "metadata": {"improvement_direction": "down", "measurement_unit": "microseconds_per_op"}},
        ]
        base = samples.get(("off", api, task))
        if config != "off" and base:
            loss = (1 - med(v["megabytes_per_sec"]) / med(base["megabytes_per_sec"])) * 100
            cpu = med(v["cpu_ex_gc_us_per_op"]) - med(base["cpu_ex_gc_us_per_op"])
            metrics += [
                {"name": "throughput_loss_vs_off_pct", "type": "MEDIAN", "value": loss,
                 "metadata": {"improvement_direction": "down", "measurement_unit": "percent"}},
                {"name": "added_cpu_us_per_op_vs_off", "type": "MEDIAN", "value": cpu,
                 "metadata": {"improvement_direction": "down", "measurement_unit": "microseconds_per_op"}},
            ]
        results.append({"info": {"test_name": name, "args": {"threads": 1, "configuration": config, "api": api}},
                        "metrics": metrics})
    Path(out).write_text(json.dumps(results, indent=2) + "\n")
    print(f"dashboard results: {out} ({len(results)} entries)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--configs", default=",".join(ALL))
    ap.add_argument("--pre-otel-worktree", type=Path)
    ap.add_argument("--iterations", type=int, default=100)
    ap.add_argument("--min-time", type=int, default=60)
    ap.add_argument("--resume", action="store_true", help="skip repetitions whose result file exists")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--dashboard", type=Path,
                    help="also write results in the Evergreen performance-dashboard shape to this file")
    args = ap.parse_args()
    args.data = args.data.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    configs = [c.strip() for c in args.configs.split(",") if c.strip()]
    if "off" not in configs:
        sys.exit("'off' is required as the baseline")
    if "pre-otel" in configs and not args.pre_otel_worktree and not args.report_only:
        sys.exit("--pre-otel-worktree is required for the pre-otel configuration")
    if not args.report_only:
        started = time.monotonic()
        for rep in range(1, args.reps + 1):
            order = deque(configs)
            order.rotate(-(rep - 1))
            print(f"rep {rep}/{args.reps}: order {list(order)}", flush=True)
            for c in order:
                run_one(c, rep, args)
            print(report(args, configs), flush=True)
        print(f"total {time.monotonic() - started:.0f}s")
    else:
        print(report(args, configs))
    if args.dashboard:
        write_dashboard(args, configs, args.dashboard)


if __name__ == "__main__":
    main()
