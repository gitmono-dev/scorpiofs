#!/usr/bin/env python3
"""Aggregate retained P1 samples using linear-interpolated quantiles."""

import argparse
import json
from pathlib import Path
import statistics


def distribution(values):
    values = sorted(values)
    if not values:
        return {"n": 0}

    def quantile(fraction):
        position = (len(values) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        return values[lower] + (values[upper] - values[lower]) * (position - lower)

    return {"n": len(values), "median_ms": statistics.median(values),
            "q25_ms": quantile(.25), "q75_ms": quantile(.75),
            "iqr_ms": quantile(.75) - quantile(.25), "p95_ms": quantile(.95),
            "min_ms": values[0], "max_ms": values[-1], "samples_ms": values}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    args = parser.parse_args()
    data = {}
    for name in ["first-directory", "read-results", "protocol-profile"]:
        data[name] = [json.loads(line) for line in
                      (args.work / (name + ".jsonl")).read_text().splitlines()]
    result = {"quantile_method": "linear interpolation at (n-1)*p",
              "counts": {}, "metrics": {}}
    for name, rows in data.items():
        result["counts"][name] = {"total": len(rows),
            "success": sum(row["status"] == "success" for row in rows),
            "failed": sum(row["status"] != "success" for row in rows)}
    for backend in ["scorpiofs", "git"]:
        ready = [row for row in data["first-directory"]
                 if row["backend"] == backend and row.get("valid")]
        result["metrics"][backend + "_first_directory"] = distribution(
            [row["first_directory_ms"] for row in ready])
        if backend == "git":
            result["metrics"]["git_clone_complete"] = distribution(
                [row["clone_complete_ms"] for row in ready])
        reads = [row for row in data["read-results"]
                 if row["backend"] == backend and row["status"] == "success"]
        for phase in ["first", "repeat"]:
            result["metrics"][backend + "_read_" + phase] = distribution(
                [row[phase + "_ms"] for row in reads])
    profiles = [row for row in data["protocol-profile"] if row["status"] == "success"]
    for phase in ["capabilities", "resolve", "root_metadata_page"]:
        result["metrics"][phase] = distribution([row[phase]["elapsed_ms"] for row in profiles])
    target = args.work / "summary.json"
    target.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    raise SystemExit(1 if any(group["failed"] for group in result["counts"].values()) else 0)


if __name__ == "__main__":
    main()
