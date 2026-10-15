#!/usr/bin/env python3
"""Place the Java OTel overhead numbers beside the Python and Ruby figures posted on
mongodb/specifications#1986, on the spec's common metrics: throughput loss vs `off`
and added CPU µs/op vs `off`. Usage: otel-compare-langs.py <results-dir>"""
import json
import sys
from pathlib import Path

# Published on PR 1986 / linked driver PRs. Python: PR 3102 description (sync, 16-core x86_64, MongoDB 8.0.4, CPython 3.9).
# Ruby: PR 3108 description (Evergreen, Ruby 4.0.1+YJIT), sdk-ratio = "sdk-parent-1pct".
OTHERS = {
    "Python (sync)": {
        ("Small doc insertOne", "api-only"): (10.2, 25), ("Small doc insertOne", "sdk-ratio"): (19.1, 53),
        ("Small doc insertOne", "sdk-always"): (36.0, 130),
        ("Find one by ID", "api-only"): (8.0, 24), ("Find one by ID", "sdk-ratio"): (16.3, 56),
        ("Find one by ID", "sdk-always"): (31.6, 133),
        "pre": {"Small doc insertOne": 1.8, "Find one by ID": 0.8},
    },
    "Python (async)": {
        ("Small doc insertOne", "api-only"): (6.1, 26), ("Small doc insertOne", "sdk-ratio"): (11.3, 51),
        ("Small doc insertOne", "sdk-always"): (24.4, 131),
        ("Find one by ID", "api-only"): (4.5, 22), ("Find one by ID", "sdk-ratio"): (9.6, 50),
        ("Find one by ID", "sdk-always"): (21.7, 130),
        "pre": {"Small doc insertOne": -0.8, "Find one by ID": 0.6},
    },
    "Ruby": {
        ("Small doc insertOne", "api-only"): (2.71, 7.30), ("Small doc insertOne", "sdk-ratio"): (8.06, 20.50),
        ("Small doc insertOne", "sdk-always"): (11.22, 29.60),
        ("Find one by ID", "api-only"): (5.35, 10.81), ("Find one by ID", "sdk-ratio"): (5.97, 19.36),
        ("Find one by ID", "sdk-always"): (7.66, 26.90),
        "pre": None,
    },
}


def main(results_dir):
    summary = json.loads((Path(results_dir) / "summary.json").read_text())

    def med(cfg, api, task, metric):
        return summary[f"{cfg}/{api}/{task}"][metric]["median"]

    java = {}
    for api in ("sync", "async"):
        d = {}
        for task in ("Small doc insertOne", "Find one by ID"):
            base_mb = med("off", api, task, "megabytes_per_sec")
            base_cpu = med("off", api, task, "cpu_ex_gc_us_per_op")
            for cfg in ("api-only", "sdk-ratio", "sdk-always"):
                d[(task, cfg)] = ((1 - med(cfg, api, task, "megabytes_per_sec") / base_mb) * 100,
                                  med(cfg, api, task, "cpu_ex_gc_us_per_op") - base_cpu)
            key = f"pre-otel/{api}/{task}"
            d.setdefault("pre", {})
            if key in summary:
                d["pre"][task] = (1 - base_mb / summary[key]["megabytes_per_sec"]["median"]) * 100
        java[f"Java ({api})"] = d

    langs = {**java, **OTHERS}
    out = ["## Cross-language comparison (throughput loss vs `off` / added CPU µs per op vs `off`)\n"]
    for task in ("Small doc insertOne", "Find one by ID"):
        out.append(f"### {task}\n")
        out.append("| Driver | api-only | sdk-ratio (1%) | sdk-always | `off` vs pre-OTel commit |")
        out.append("|---|---|---|---|---|")
        for name, d in langs.items():
            cells = [f"{d[(task, c)][0]:+.1f}% / +{d[(task, c)][1]:.0f} µs" for c in ("api-only", "sdk-ratio", "sdk-always")]
            pre = d.get("pre")
            cells.append(f"{pre[task]:+.1f}%" if pre and task in pre else "n/a")
            out.append(f"| {name} | " + " | ".join(cells) + " |")
        out.append("")
    out.append("Java CPU is process CPU time excluding GC; Python is process CPU time; Ruby is CPU excluding GC. "
               "Hosts differ (Java: Apple M4 Max, MongoDB 8.2.6, Temurin 17; Python: 16-core x86_64, MongoDB 8.0.4; "
               "Ruby: Evergreen), so compare relative overhead, not absolute µs.")
    text = "\n".join(out) + "\n"
    (Path(results_dir) / "CROSS_LANGUAGE.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main(sys.argv[1])
