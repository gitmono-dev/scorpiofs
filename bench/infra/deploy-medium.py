#!/usr/bin/env python3
"""Deploy one run-scoped cluster from WSL; registry auth remains in memory."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--kubeconfig", required=True)
    ap.add_argument("--stack", required=True)
    ap.add_argument("--docker-config", type=Path, default=Path.home() / ".docker/config.json")
    ap.add_argument("--registry", required=True, help="Registry key in Docker auth config")
    ap.add_argument("--pull-registry", required=True, help="Registry hostname used by the cluster")
    a = ap.parse_args()

    def kube(*args, data=None):
        p = subprocess.run(["kubectl", "--kubeconfig", a.kubeconfig, *args],
                           input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
        if p.returncode:
            raise RuntimeError(p.stderr.decode(errors="replace"))
        return p.stdout

    nodes = json.loads(kube("get", "nodes", "-o", "json"))["items"]
    if len(nodes) != 3:
        raise SystemExit("expected exactly three nodes; refusing deployment")
    roles = [n["metadata"]["labels"].get("bench-role") for n in nodes]
    if sorted(roles) != ["runner", "service", "storage"]:
        raise SystemExit("node role mismatch: " + str(roles))
    for n in nodes:
        if not any(c["type"] == "Ready" and c["status"] == "True" for c in n["status"]["conditions"]):
            raise SystemExit("node not Ready")
    print(kube("create", "namespace", "gitmono", "--dry-run=client", "-o", "json").decode()[:0], end="")
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "gitmono",
                  "labels": {"pod-security.kubernetes.io/enforce": "privileged"}}}
    print(kube("apply", "-f", "-", data=json.dumps(namespace).encode()).decode())
    config = json.loads(a.docker_config.read_text())
    auth = {"auths": {a.pull_registry: config["auths"][a.registry]}}
    import base64
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "acr-pull", "namespace": "gitmono"},
              "type": "kubernetes.io/dockerconfigjson", "data": {
                  ".dockerconfigjson": base64.b64encode(json.dumps(auth).encode()).decode()}}
    print(kube("apply", "-f", "-", data=json.dumps(secret).encode()).decode())
    print(kube("apply", "-f", a.stack).decode())
    print(kube("get", "nodes", "-L", "bench-role").decode())


if __name__ == "__main__":
    main()
