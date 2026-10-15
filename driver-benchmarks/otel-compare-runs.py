#!/usr/bin/env python3
"""Compare two otel-bench.py result directories (before/after an optimisation).
Usage: otel-compare-runs.py <before-dir> <after-dir> [<out.md>]"""
import json
import sys
from pathlib import Path

TASKS = [("sync", "Small doc insertOne"), ("sync", "Find one by ID"),
         ("async", "Small doc insertOne"), ("async", "Find one by ID")]
CONFIGS = ["off", "api-only", "sdk-ratio", "sdk-always"]


def load(d):
    return json.loads((Path(d) / "summary.json").read_text())


def med(s, cfg, api, task, metric):
    return s[f"{cfg}/{api}/{task}"][metric]["median"]


def main(before, after, out=None):
    b, a = load(before), load(after)
    lines = ["# Java OTel overhead: before vs after optimisation\n",
             f"Before: `{before}`  After: `{after}`. Medians of 5 repetitions each. "
             "Loss = throughput lost vs that run's own `off`; CPU = process CPU per op excluding GC.\n"]
    lines.append("## Overhead vs `off` (loss % / added CPU µs per op)\n")
    lines.append("| API | Task | Config | Before | After | Δ loss (pts) | Δ CPU (µs) |")
    lines.append("|---|---|---|---|---|---|---|")
    for api, task in TASKS:
        bb_mb, ab_mb = med(b, "off", api, task, "megabytes_per_sec"), med(a, "off", api, task, "megabytes_per_sec")
        bb_cpu, ab_cpu = med(b, "off", api, task, "cpu_ex_gc_us_per_op"), med(a, "off", api, task, "cpu_ex_gc_us_per_op")
        for cfg in CONFIGS[1:]:
            bl = (1 - med(b, cfg, api, task, "megabytes_per_sec") / bb_mb) * 100
            al = (1 - med(a, cfg, api, task, "megabytes_per_sec") / ab_mb) * 100
            bc = med(b, cfg, api, task, "cpu_ex_gc_us_per_op") - bb_cpu
            ac = med(a, cfg, api, task, "cpu_ex_gc_us_per_op") - ab_cpu
            lines.append(f"| {api} | {task} | {cfg} | {bl:+.1f}% / +{bc:.1f} | {al:+.1f}% / +{ac:.1f} | {al - bl:+.1f} | {ac - bc:+.1f} |")
    lines.append("\n## Absolute medians (MB/s, CPU ex-GC µs/op)\n")
    lines.append("| API | Task | Config | Before MB/s | After MB/s | Before CPU | After CPU |")
    lines.append("|---|---|---|---|---|---|---|")
    for api, task in TASKS:
        for cfg in CONFIGS:
            lines.append(f"| {api} | {task} | {cfg} | {med(b, cfg, api, task, 'megabytes_per_sec'):.3f} | "
                         f"{med(a, cfg, api, task, 'megabytes_per_sec'):.3f} | "
                         f"{med(b, cfg, api, task, 'cpu_ex_gc_us_per_op'):.1f} | {med(a, cfg, api, task, 'cpu_ex_gc_us_per_op'):.1f} |")
    lines.append("\n## sdk-ratio vs sdk-always gap (the spec's gating diagnostic)\n")
    lines.append("| API | Task | Before gap (pts loss / µs CPU) | After gap |")
    lines.append("|---|---|---|---|")
    for api, task in TASKS:
        def gap(s):
            off = med(s, "off", api, task, "megabytes_per_sec")
            r = (1 - med(s, "sdk-ratio", api, task, "megabytes_per_sec") / off) * 100
            al = (1 - med(s, "sdk-always", api, task, "megabytes_per_sec") / off) * 100
            c = med(s, "sdk-always", api, task, "cpu_ex_gc_us_per_op") - med(s, "sdk-ratio", api, task, "cpu_ex_gc_us_per_op")
            return f"{al - r:+.1f} / {c:+.1f}"
        lines.append(f"| {api} | {task} | {gap(b)} | {gap(a)} |")
    text = "\n".join(lines) + "\n"
    if out:
        Path(out).write_text(text)
    print(text)


if __name__ == "__main__":
    main(*sys.argv[1:])
