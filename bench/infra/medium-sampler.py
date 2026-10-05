#!/usr/bin/env python3
"""Raw 1 Hz node telemetry including every interface; never read process argv/env."""
import json
import os
from pathlib import Path
import time

proc = Path("/hostproc")
out = Path("/samples/samples.jsonl")
names = {"scorpio", "mega2", "postgres", "redis-server", "rustfs", "libra", "git"}
with out.open("a", buffering=1) as stream:
    while True:
        start = time.monotonic()
        row = {"time_ns": time.time_ns(), "node": os.environ.get("NODE_NAME"),
               "proc_stat": (proc / "stat").read_text(),
               "diskstats": (proc / "diskstats").read_text(),
               "netdev": (proc / "net/dev").read_text(),
               "meminfo": (proc / "meminfo").read_text(), "processes": []}
        for path in proc.glob("[0-9]*"):
            try:
                name = (path / "comm").read_text().strip()
                if name not in names:
                    continue
                row["processes"].append({"pid": path.name, "name": name,
                    "stat": (path / "stat").read_text(), "io": (path / "io").read_text(),
                    "fds": sum(1 for _ in (path / "fd").iterdir())})
            except (OSError, PermissionError):
                continue
        stream.write(json.dumps(row) + "\n")
        time.sleep(max(0, 1 - (time.monotonic() - start)))
