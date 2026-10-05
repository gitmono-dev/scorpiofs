#!/usr/bin/env python3
"""Render node CPU and selected process RSS from the exported 1 Hz evidence."""
import argparse
import datetime as dt
import gzip
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--evidence", type=Path, required=True)
p.add_argument("--out", type=Path, required=True)
p.add_argument("--clock-ticks", type=int, default=100)
a = p.parse_args()
fig, axes = plt.subplots(2, 2, figsize=(13, 7.6), sharex=True)
colors = {"runner": "#2563eb", "service": "#e46d27", "storage": "#18927c",
          "scorpio": "#2563eb", "mega2": "#e46d27", "rustfs": "#18927c"}
for number, cluster in enumerate("ab"):
    roles = {}
    for nodefile in a.evidence.rglob(cluster+"-nodes.json"):
        for node in json.loads(nodefile.read_text())["items"]:
            roles[node["metadata"]["name"]] = node["metadata"]["labels"].get("bench-role", "unknown")
    rss = {}
    for f in sorted(a.evidence.rglob(cluster+"-*-samples.jsonl.gz")):
        times, cpu = [], []
        previous = None
        with gzip.open(f, "rt") as stream:
            for line in stream:
                row = json.loads(line)
                stamp = row["time_ns"]/1e9
                fields = list(map(int, row["proc_stat"].splitlines()[0].split()[1:]))
                active = sum(fields[i] for i in (0, 1, 2, 5, 6))
                usage = 0 if previous is None else max(0, (active-previous[1])/a.clock_ticks/(stamp-previous[0]))
                previous = stamp, active
                times.append(stamp)
                cpu.append(usage)
                for proc in row["processes"]:
                    if proc["name"] not in ("scorpio", "mega2", "rustfs"):
                        continue
                    stat = proc["stat"].rsplit(") ", 1)[1].split()
                    series = rss.setdefault(proc["name"], {})
                    bucket = int(stamp)//5*5
                    # One process per service; max protects against transient duplicate PIDs.
                    series[bucket] = max(series.get(bucket, 0), int(stat[21])*4096/2**30)
        if not times:
            continue
        buckets = {}
        for stamp, usage in zip(times[1:], cpu[1:]):
            buckets.setdefault(int(stamp)//5*5, []).append(usage)
        stamps = sorted(buckets)
        role = roles.get(row["node"], row["node"])
        axes[number, 0].plot([dt.datetime.fromtimestamp(t, dt.timezone.utc) for t in stamps],
                            [np.mean(buckets[t]) for t in stamps], label=role,
                            color=colors.get(role), linewidth=1.1)
    for name, values in rss.items():
        stamps = sorted(values)
        axes[number, 1].plot([dt.datetime.fromtimestamp(t, dt.timezone.utc) for t in stamps],
                            [values[t] for t in stamps], label=name, color=colors[name], linewidth=1.2)
    for statefile in a.evidence.rglob(cluster+"-*-reduced-v1-state.json"):
        state = json.loads(statefile.read_text())
        if not state["steps"] or state["stage"] not in ("measure", "client-cache"):
            continue
        start = dt.datetime.fromtimestamp(state["steps"][0]["start_utc"], dt.timezone.utc)
        end = dt.datetime.fromtimestamp(state["end_utc"], dt.timezone.utc)
        for ax in axes[number]:
            ax.axvspan(start, end, color="#dbeafe" if state["stage"] == "measure" else "#fef3c7", alpha=.4)
    axes[number, 0].set_title(f"Cluster {cluster.upper()} - node CPU (5 s mean)", loc="left")
    axes[number, 1].set_title(f"Cluster {cluster.upper()} - selected process RSS (5 s maximum)", loc="left")
    axes[number, 0].set_ylabel("CPU cores used (excludes steal / iowait)")
    axes[number, 1].set_ylabel("RSS (GiB)")
    for ax in axes[number]:
        ax.set_ylim(bottom=0)
        ax.grid(alpha=.18)
        ax.legend(loc="upper left", ncol=3, fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.timezone.utc))
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
for ax in axes[1]:
    ax.set_xlabel("Time (UTC; Beijing time = UTC + 8 h)")
fig.suptitle("Alibaba Cloud medium run - import, correctness gates and 50k measurements", fontsize=14)
fig.text(.5, .015, "Blue shade: existing-cache core rounds. Yellow shade: client cold. Raw sampling is 1 Hz; curves include preparation.",
         ha="center", fontsize=9)
fig.tight_layout(rect=(0, .04, 1, .95))
a.out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(a.out.with_suffix(".png"), dpi=150)
fig.savefig(a.out.with_suffix(".svg"))
print(a.out.with_suffix(".png"))
