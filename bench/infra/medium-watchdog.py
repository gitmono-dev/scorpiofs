#!/usr/bin/env python3
"""One-shot local deadline guard for this approved run; no recurring automation."""
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import time

state_path = Path(sys.argv[1])
controller = Path(__file__).parent / "ack-medium.py"
while True:
    try:
        if state_path.exists():
            state = json.loads(state_path.read_text())
            if state.get("cleanup_verified"):
                break
            deadline = state.get("deadline_utc")
            if deadline and dt.datetime.now(dt.timezone.utc) >= dt.datetime.fromisoformat(deadline):
                print("deadline reached: requesting deletion of run-owned clusters", flush=True)
                subprocess.run([sys.executable, str(controller), "destroy", "--state", str(state_path)], timeout=300)
                # Keep retrying if an API failure prevented cleanup; final inventory audit remains mandatory.
    except Exception as error:
        print(type(error).__name__ + ": " + str(error), flush=True)
    time.sleep(30)
