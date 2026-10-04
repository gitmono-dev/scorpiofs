#!/usr/bin/env python3
"""Render isolated persistent medium stack from proven ACK services (PyYAML)."""
import argparse
import json
import uuid
from pathlib import Path
import yaml


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runner", required=True)
    p.add_argument("--backend", required=True)
    p.add_argument("--fixture", required=True)
    p.add_argument("--instance-uuid", type=uuid.UUID, help="Reuse the deployment identity when re-rendering")
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    base = Path(__file__).parent / "k8s/mega2-ack.yaml"
    docs = [d for d in yaml.safe_load_all(base.read_text()) if d and d["kind"] not in ("Secret",)]
    for d in docs:
        if d["kind"] == "Namespace":
            d["metadata"]["labels"] = {"pod-security.kubernetes.io/enforce": "privileged"}
        if d["kind"] == "Service":
            d["spec"]["type"] = "ClusterIP"
            for port in d["spec"]["ports"]:
                port.pop("nodePort", None)
        if d["kind"] in ("Deployment", "Job"):
            name = d["metadata"]["name"]
            spec = d["spec"]["template"]["spec"]
            for container in spec.get("containers", []) + spec.get("initContainers", []):
                if container.get("image") == "mega2:local":
                    container["image"] = a.backend
            role = "storage" if name in ("postgres", "rustfs", "rustfs-init") else "service"
            spec["nodeSelector"] = {"bench-role": role}
            spec.pop("affinity", None)
            if name == "mega2":
                spec["containers"] = [c for c in spec["containers"] if c["name"] == "mega2"]
                spec["containers"][0]["env"].extend([
                    {"name": "MEGA_MST2__ENABLED", "value": "true"},
                    {"name": "MEGA_MST2__INSTANCE_UUID", "value": str(a.instance_uuid or uuid.uuid4())},
                    {"name": "MEGA_MST2__PUBLICATION_ENABLED", "value": "false"},
                ])
                spec["containers"][0]["image"] = a.backend
                spec["containers"][0]["resources"] = {"requests": {"cpu": "4", "memory": "8Gi"},
                                                       "limits": {"cpu": "7", "memory": "13Gi"}}
                spec["volumes"] = [{"name": "state", "persistentVolumeClaim": {"claimName": "mega2-data"}}]
            elif name == "postgres":
                spec["volumes"] = [{"name": "d", "persistentVolumeClaim": {"claimName": "postgres-data"}}]
                spec["containers"][0]["resources"]["limits"] = {"cpu": "2", "memory": "4Gi"}
            elif name == "rustfs":
                spec["securityContext"]["fsGroupChangePolicy"] = "OnRootMismatch"
                spec["volumes"] = [{"name": "d", "persistentVolumeClaim": {"claimName": "rustfs-data"}}]
                spec["containers"][0]["resources"]["limits"] = {"cpu": "5", "memory": "9Gi"}
            if d["kind"] == "Deployment":
                d["spec"]["strategy"] = {"type": "Recreate"}
    docs.insert(1, {"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
        "metadata": {"name": "medium-essd-pl0"}, "provisioner": "diskplugin.csi.alibabacloud.com",
        "parameters": {"type": "cloud_essd", "performanceLevel": "PL0"},
        "reclaimPolicy": "Delete", "volumeBindingMode": "WaitForFirstConsumer", "allowVolumeExpansion": False})
    for name, gb in (("rustfs-data", 100), ("postgres-data", 30), ("mega2-data", 20), ("runner-data", 200)):
        docs.insert(2, {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "gitmono"},
            "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "medium-essd-pl0",
                     "resources": {"requests": {"storage": f"{gb}Gi"}}}})
    docs.append({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "medium-runner", "namespace": "gitmono"},
        "spec": {"nodeSelector": {"bench-role": "runner"}, "imagePullSecrets": [{"name": "acr-pull"}],
          "restartPolicy": "Never", "shareProcessNamespace": True,
          "initContainers": [{"name": "fixture", "image": a.fixture, "command": ["bash", "-ec",
              "mkdir -p /data/fixture; cp -a /fixture/. /data/fixture/"],
              "volumeMounts": [{"name": "data", "mountPath": "/data"}]}],
          "containers": [{"name": "runner", "image": a.runner, "securityContext": {"privileged": True},
              "command": ["bash", "-ec", "ln -s /data/fixture /fixture; git config --global --add safe.directory /fixture/repo.git; mkdir -p /data/scorpio/{mount,store,upper,cl,mounts}; /usr/local/bin/docker-entrypoint.sh serve --http-addr 0.0.0.0:2725 > /data/scorpio-daemon.log 2>&1 & echo $! > /data/daemon.pid; exec sleep infinity"],
              "env": [{"name": k, "value": v} for k, v in {
                "SCORPIO_BASE_URL": "http://mega2:8000", "SCORPIO_LFS_URL": "http://mega2:8000/api/v1/lfs",
                "SCORPIO_MST2_BASE_URL": "http://mega2:8000", "SCORPIO_MST2_SCOPE": "/project",
                "SCORPIO_MST2_LOWER_ENABLED": "true", "SCORPIO_MOUNT_OWNER": "0:0",
                "SCORPIO_WORKSPACE": "/data/scorpio/mount", "SCORPIO_STORE_PATH": "/data/scorpio/store",
                "SCORPIO_CONFIG_FILE": "/data/scorpio/config.toml",
                "SCORPIO_ANTARES_UPPER_ROOT": "/data/scorpio/upper", "SCORPIO_ANTARES_CL_ROOT": "/data/scorpio/cl",
                "SCORPIO_ANTARES_MOUNT_ROOT": "/data/scorpio/mounts", "SCORPIO_ANTARES_STATE_FILE": "/data/scorpio/state.toml",
                "RUST_LOG": "warn"}.items()],
              "volumeMounts": [{"name": "data", "mountPath": "/data"}, {"name": "fuse", "mountPath": "/dev/fuse"}],
              "resources": {"requests": {"cpu": "4", "memory": "8Gi"}, "limits": {"cpu": "7", "memory": "13Gi"}}}],
          "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "runner-data"}},
                      {"name": "fuse", "hostPath": {"path": "/dev/fuse"}}]}})
    drivers = {"medium-cloud-run.py": Path(__file__).parent / "medium-cloud-run.py",
               "medium-extended.py": Path(__file__).parent / "medium-extended.py",
               "medium-monorepo.py": Path(__file__).parents[1] / "workload/medium-monorepo.py",
               "medium-profile.py": Path(__file__).parents[1] / "cases/medium-profile.py",
               "git-shallow-gate.py": Path(__file__).parents[1] / "cases/git-shallow-gate.py",
               "reduce-medium-history.py": Path(__file__).parents[1] / "workload/reduce-medium-history.py",
               "first-directory-ready.py": Path(__file__).parents[1] / "bin/first-directory-ready.py"}
    docs.append({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "medium-drivers", "namespace": "gitmono"},
                 "data": {name: path.read_text() for name, path in drivers.items()}})
    runner = next(d for d in docs if d["kind"] == "Pod" and d["metadata"]["name"] == "medium-runner")
    runner["spec"]["volumes"].append({"name": "drivers", "configMap": {"name": "medium-drivers"}})
    runner["spec"]["containers"][0]["volumeMounts"].append({"name": "drivers", "mountPath": "/bench-medium", "readOnly": True})
    docs.append({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "medium-sampler", "namespace": "gitmono"},
                 "data": {"sampler.py": (Path(__file__).parent / "medium-sampler.py").read_text()}})
    docs.append({"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": {"name": "medium-sampler", "namespace": "gitmono"},
        "spec": {"selector": {"matchLabels": {"app": "medium-sampler"}},
           "template": {"metadata": {"labels": {"app": "medium-sampler"}}, "spec": {
              "hostPID": True, "hostNetwork": True, "imagePullSecrets": [{"name": "acr-pull"}],
              "containers": [{"name": "sampler", "image": a.runner, "securityContext": {"privileged": True},
                 "command": ["python3", "/scripts/sampler.py"],
                 "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
                 "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "200m", "memory": "128Mi"}},
                 "volumeMounts": [{"name": "proc", "mountPath": "/hostproc", "readOnly": True},
                                  {"name": "samples", "mountPath": "/samples"},
                                  {"name": "scripts", "mountPath": "/scripts"}]}],
              "volumes": [{"name": "proc", "hostPath": {"path": "/proc"}},
                          {"name": "samples", "hostPath": {"path": "/var/lib/scorpio-medium-samples", "type": "DirectoryOrCreate"}},
                          {"name": "scripts", "configMap": {"name": "medium-sampler"}}]}}}})
    a.out.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": docs}, indent=2) + "\n")
    print(json.dumps({"documents": len(docs), "data_gib_per_cluster": 350, "mega2_state": "PVC"}))


if __name__ == "__main__":
    main()
