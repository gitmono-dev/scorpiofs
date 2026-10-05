#!/usr/bin/env python3
"""Summarize exported evidence without turning partial gates into performance claims."""
import argparse
import gzip
import json
from pathlib import Path
import statistics
import tarfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    result = {"stages": [], "observations": [], "telemetry": [],
              "telemetry_scope": "entire exported run, including import and deployment",
              "rss_page_bytes": 4096, "network_note": "Interfaces kept separate; do not sum host/veth traffic."}
    for f in sorted(a.evidence.rglob("*-state.json")):
        result["stages"].append(json.loads(f.read_text()))
    for f in sorted(a.evidence.rglob("*-results.tar.gz")):
        with tarfile.open(f) as archive:
            for member in archive.getmembers():
                if not member.isfile() or not member.name.endswith(".jsonl") or "manifest" in member.name:
                    continue
                stream = archive.extractfile(member)
                for line in stream:
                    row = json.loads(line)
                    row["evidence_file"] = f.name + ":" + member.name
                    result["observations"].append(row)
    for f in sorted(a.evidence.rglob("*-samples.jsonl.gz")):
        peaks = {}
        count = 0
        first = last = None
        interfaces = {}
        with gzip.open(f, "rt") as stream:
            for line in stream:
                row = json.loads(line)
                count += 1
                first = first or row["time_ns"]
                last = row["time_ns"]
                for process in row["processes"]:
                    fields = process["stat"].rsplit(") ", 1)[1].split()
                    peak = peaks.setdefault(process["name"], {"max_individual_rss_bytes": 0, "max_individual_fds": 0})
                    peak["max_individual_rss_bytes"] = max(peak["max_individual_rss_bytes"], int(fields[21])*4096)
                    peak["max_individual_fds"] = max(peak["max_individual_fds"], process["fds"])
                for network in row["netdev"].splitlines():
                    if ":" not in network:
                        continue
                    name, counters = network.split(":", 1)
                    fields = counters.split()
                    rx, tx = int(fields[0]), int(fields[8])
                    value = interfaces.setdefault(name.strip(), {"rx_bytes": 0, "tx_bytes": 0, "last_rx": rx, "last_tx": tx})
                    value["rx_bytes"] += max(0, rx-value["last_rx"])
                    value["tx_bytes"] += max(0, tx-value["last_tx"])
                    value.update(last_rx=rx, last_tx=tx)
        for value in interfaces.values():
            value.pop("last_rx")
            value.pop("last_tx")
        result["telemetry"].append({"file": f.name, "samples": count, "first_time_ns": first,
                                    "last_time_ns": last, "process_peaks": peaks, "network_deltas": interfaces})
    groups = {}
    def metric(row, name, value, sample_count=None):
        key = (row.get("cluster"), row.get("files"), row.get("cache"), row.get("backend"), name, sample_count)
        groups.setdefault(key, []).append(value)

    for row in result["observations"]:
        if row.get("status") != "success":
            continue
        phase = row.get("phase")
        if phase == "directory_ready":
            metric(row, "first_directory_ms", row["first_directory_ms"])
            metric(row, "api_complete_ms" if row["backend"] == "scorpiofs" else "clone_complete_ms",
                   row["api_complete_ms"] if row["backend"] == "scorpiofs" else row["clone_complete_ms"])
        elif phase == "read":
            count = row["first"]["count"]
            for mode in ("first", "repeat"):
                metric(row, mode+"_read_ms", row[mode]["ms"], count)
            metric(row, "workflow_elapsed_to_first_read_ms", row["workflow_elapsed_to_first_read_ms"],
                   row["workflow_distinct_files_read"])
        elif phase == "traverse":
            metric(row, "traverse_ms", row["result"]["ms"])
        elif phase == "range_read":
            metric(row, "range_read_ms", row["ms"])
    result["metrics"] = []
    for key, values in groups.items():
        quartiles = statistics.quantiles(values, n=4, method="inclusive") if len(values) > 1 else [values[0]]*3
        cluster, files, cache, backend, name, sample_count = key
        result["metrics"].append({"cluster": cluster, "files": files, "cache": cache, "backend": backend,
                                  "metric": name, "sample_count": sample_count, "n": len(values),
                                  "median": statistics.median(values), "iqr": quartiles[2]-quartiles[0],
                                  "min": min(values), "max": max(values), "values": values})
    result["metrics_note"] = "Successful observations only; consult stage statuses and cleanup evidence before claiming a completed matrix."
    a.out.write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps({"stages": len(result["stages"]), "observations": len(result["observations"]),
                      "telemetry_files": len(result["telemetry"])}))


if __name__ == "__main__":
    main()
