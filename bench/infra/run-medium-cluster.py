#!/usr/bin/env python3
"""Bounded single-cluster stages; root coordinator decides whether to advance."""
import argparse
import json
from pathlib import Path
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kubeconfig", required=True)
    p.add_argument("--cluster", required=True)
    p.add_argument("--stage", choices=["preflight", "primary", "verify", "measure", "client-cache", "cache", "workspaces", "development", "concurrency", "soak"], required=True)
    p.add_argument("--files", type=int, default=200000)
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--cache-rounds", type=int, default=5)
    p.add_argument("--soak-duration", type=int, default=1800)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--attempt", default="v1")
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    log = a.out/f"{a.cluster}-{a.stage}-{a.attempt}.log"
    statefile = a.out/f"{a.cluster}-{a.stage}-{a.attempt}-state.json"
    if log.exists():
        raise SystemExit("stage output already exists")
    state = {"cluster": a.cluster, "stage": a.stage, "status": "running", "steps": []}

    def save():
        statefile.write_text(json.dumps(state, indent=2)+"\n")

    def kube(argv, timeout=1800):
        row = {"args": argv, "start_utc": time.time()}
        state["steps"].append(row)
        save()
        with log.open("ab", buffering=0) as f:
            f.write(("\nSTEP "+json.dumps(argv)+"\n").encode())
            r = subprocess.run(["kubectl", "--kubeconfig", a.kubeconfig, "-n", "gitmono", *argv],
                               stdout=f, stderr=subprocess.STDOUT, timeout=timeout)
        row.update(exit_code=r.returncode, end_utc=time.time())
        save()
        if r.returncode:
            raise RuntimeError("step failed: " + str(argv[:4]))

    def driver(action, files, rounds=5, cache="existing-server", label=None, timeout=1800, backend="both"):
        if action == "seed":
            # Applying the stack does not imply protocol readiness. Bound startup
            # retries before writing the non-repeatable seed evidence.
            kube(["wait", "--for=condition=Ready", "pod/medium-runner", "--timeout=300s"], 330)
            kube(["wait", "--for=condition=complete", "job/rustfs-init", "--timeout=300s"], 330)
            kube(["exec", "medium-runner", "-c", "runner", "--", "bash", "-ec",
                  "for i in $(seq 1 60); do curl -fsS http://mega2:8000/api/v2/snapshots/capabilities && exit 0; sleep 2; done; exit 1"], 180)
        extra = ["--bootstrap-initial"] if action == "seed" and files == 100000 else []
        kube(["exec", "medium-runner", "-c", "runner", "--", "timeout", "--kill-after=30", str(timeout),
              "python3", "/bench-medium/medium-cloud-run.py", action, "--cluster", a.cluster,
              "--files", str(files), "--rounds", str(rounds), "--cache", cache, "--backend", backend,
              "--label", label or a.attempt] + extra, timeout+60)

    save()
    try:
        if a.stage == "preflight":
            driver("seed", 100000, timeout=3600)
            # A restart must not lose PostgreSQL, object storage or vault state.
            kube(["rollout", "restart", "deployment/postgres", "deployment/rustfs", "deployment/redis", "deployment/mega2"])
            for name in ("postgres", "rustfs", "redis", "mega2"):
                kube(["rollout", "status", "deployment/"+name, "--timeout=300s"], 330)
            # Healthy means the actual protocol endpoint responds, not only a Running pod.
            kube(["exec", "medium-runner", "-c", "runner", "--", "bash", "-ec",
                  "for i in $(seq 1 60); do curl -fsS http://mega2:8000/api/v2/snapshots/capabilities && exit 0; sleep 2; done; exit 1"], 180)
            driver("verify", 100000)
            driver("measure", 100000, rounds=3)
        elif a.stage == "verify":
            driver("verify", a.files, rounds=a.rounds)
        elif a.stage in ("primary", "measure"):
            if a.stage == "primary":
                driver("seed", a.files, timeout=3600)
                driver("verify", a.files)
            driver("measure", a.files, rounds=a.rounds)
            kube(["exec", "medium-runner", "-c", "runner", "--", "python3", "/bench-medium/medium-profile.py",
                  "--base", "http://mega2:8000", "--scope", "/project", "--rounds", str(a.rounds)])
        elif a.stage == "client-cache":
            driver("measure", a.files, rounds=a.cache_rounds, cache="client-cold")
        elif a.stage == "cache":
            # Avoid recursive ownership walks over the object store on each cold restart.
            kube(["patch", "deployment/rustfs", "--type=merge", "-p", json.dumps({
                "spec": {"template": {"spec": {"securityContext": {"fsGroupChangePolicy": "OnRootMismatch"}}}}
            })])
            kube(["rollout", "status", "deployment/rustfs", "--timeout=300s"], 330)
            driver("measure", a.files, rounds=a.cache_rounds, cache="client-cold")
            driver("measure", a.files, rounds=a.cache_rounds, cache="warm")
            for number in range(a.cache_rounds):
                for backend in (["scorpiofs", "git"] if number%2 == 0 else ["git", "scorpiofs"]):
                    kube(["rollout", "restart", "deployment/postgres", "deployment/rustfs", "deployment/redis", "deployment/mega2"])
                    for name in ("postgres", "rustfs", "redis", "mega2"):
                        kube(["rollout", "status", "deployment/"+name, "--timeout=300s"], 330)
                    kube(["exec", "medium-runner", "-c", "runner", "--", "bash", "-ec",
                         "for i in $(seq 1 60); do curl -fsS http://mega2:8000/api/v2/snapshots/capabilities && exit 0; sleep 2; done; exit 1"], 180)
                    sampler = subprocess.check_output(["kubectl", "--kubeconfig", a.kubeconfig, "-n", "gitmono",
                                                       "get", "pods", "-l", "app=medium-sampler", "-o", "json"])
                    for pod in json.loads(sampler)["items"]:
                        kube(["exec", pod["metadata"]["name"], "--", "sh", "-ec", "sync; echo 3 > /proc/sys/vm/drop_caches"])
                    driver("measure", a.files, rounds=1, cache="end-to-end-cold",
                           label=f"{a.attempt}-e2e-{number+1}-{backend}", backend=backend)
        else:
            timeout = 7200 if a.stage == "soak" else 3600
            kube(["exec", "medium-runner", "-c", "runner", "--", "timeout", "--kill-after=30", str(timeout),
                  "python3", "/bench-medium/medium-extended.py", a.stage, "--cluster", a.cluster,
                  "--files", str(a.files), "--rounds", str(a.rounds), "--duration", str(a.soak_duration) if a.stage == "soak" else "60"], timeout+60)
        state["status"] = "success"
    except Exception as error:
        state.update(status="failed", error=str(error))
        save()
        raise
    finally:
        state["end_utc"] = time.time()
        save()


if __name__ == "__main__":
    main()
