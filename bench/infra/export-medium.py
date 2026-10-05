#!/usr/bin/env python3
"""Export measured evidence only, never fixture bodies, registry Secrets or kubeconfig."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kubeconfig", required=True)
    p.add_argument("--cluster", required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    def kube(argv, output=None, compressed=False):
        cmd = ["kubectl", "--kubeconfig", a.kubeconfig, "-n", "gitmono", *argv]
        if output is None:
            return subprocess.check_output(cmd, timeout=300)
        destination = a.out/output
        if destination.exists():
            raise RuntimeError("preserve earlier export: "+str(destination))
        if compressed:
            with destination.open("wb") as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, timeout=300)
        else:
            destination.write_bytes(subprocess.check_output(cmd, timeout=300))
            return
        if result.returncode:
            raise RuntimeError("export failed: "+result.stderr.decode(errors="replace"))

    exec_prefix = ["exec", "medium-runner", "-c", "runner", "--"]
    kube(["get", "pods", "-o", "json"], a.cluster+"-pods.json")
    kube(["get", "nodes", "-o", "json"], a.cluster+"-nodes.json")
    kube(["get", "pvc,pv", "-o", "json"], a.cluster+"-volumes.json")
    kube(["get", "events", "-o", "json"], a.cluster+"-events.json")
    kube(exec_prefix+["cat", "/fixture/manifest.refs.json"], a.cluster+"-refs.json")
    kube(exec_prefix+["cat", "/fixture/manifest.summary.json"], a.cluster+"-fixture-summary.json")
    # Remote compression avoids sending thousands of uncompressed sampling rows over EIP.
    script = "import os,tarfile,sys,pathlib; out=tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz'); " \
             "root=pathlib.Path('/data/results'); paths=[str(p) for p in root.iterdir() if p.is_file() and p.suffix in ('.json','.jsonl','.log')]+[str(p) for p in root.glob('*/ready-git.log')]; " \
             "[(out.add(p,arcname=os.path.relpath(p,'/data/results'))) for p in paths]; out.close()"
    kube(exec_prefix+["python3", "-c", script], a.cluster+"-results.tar.gz", compressed=True)
    kube(exec_prefix+["bash", "-ec", "for p in /data/*daemon.log /data/cold-*/daemon.log; do [ ! -f \"$p\" ] || { echo \"LOG $p\"; tail -2000 \"$p\"; }; done"], a.cluster+"-daemon-tail.log")
    for name in ("mega2", "postgres", "rustfs", "redis"):
        kube(["logs", "deployment/"+name, "--tail=2000"], a.cluster+"-"+name+".log")
    samplers = json.loads(kube(["get", "pods", "-l", "app=medium-sampler", "-o", "json"]))
    for pod in samplers["items"]:
        name = pod["metadata"]["name"]
        kube(["exec", name, "--", "gzip", "-c", "/samples/samples.jsonl"], a.cluster+"-"+name+"-samples.jsonl.gz", compressed=True)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in a.out.iterdir() if p.is_file()}
    (a.out/(a.cluster+"-export-hashes.json")).write_text(json.dumps(hashes, indent=2)+"\n")
    print(json.dumps({"cluster": a.cluster, "export_files": len(hashes), "export_bytes": sum(p.stat().st_size for p in a.out.iterdir() if p.is_file())}))


if __name__ == "__main__":
    main()
