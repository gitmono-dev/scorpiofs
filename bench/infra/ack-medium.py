#!/usr/bin/env python3
"""Run-scoped ACK lifecycle. Invoke on Windows with the existing Alibaba CLI profile.

State contains resource IDs, never credentials. No automatic instance substitution.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import shutil
import secrets
import subprocess
import tempfile

def cli(*args, body=None):
    argv = [shutil.which("aliyun") or shutil.which("aliyun.exe"), *args]
    if body is None:
        result = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", timeout=120)
    else:
        result = subprocess.run(argv + ["--header", "Content-Type=application/json", "--body", json.dumps(body)],
                                capture_output=True, text=True, encoding="utf-8", timeout=120)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return json.loads(result.stdout) if result.stdout.strip() else {}


def require_time_remaining(state):
    deadline = state.get("deadline_utc")
    if not deadline:
        raise RuntimeError("a run deadline is required before provisioning")
    if dt.datetime.now(dt.timezone.utc) >= dt.datetime.fromisoformat(deadline):
        raise RuntimeError("run deadline reached; refusing new resources")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=["preflight", "create", "status", "nodepools", "kubeconfig", "destroy"])
    ap.add_argument("--state", type=Path, required=True)
    ap.add_argument("--run-id")
    ap.add_argument("--vpc")
    ap.add_argument("--vswitch")
    ap.add_argument("--region", default="cn-hangzhou")
    ap.add_argument("--zone", default="cn-hangzhou-h")
    ap.add_argument("--max-hours", type=float, default=4)
    args = ap.parse_args()
    state = json.loads(args.state.read_text()) if args.state.exists() else {
        "run_id": args.run_id, "vpc": args.vpc, "vswitch": args.vswitch,
        "region": args.region, "zone": args.zone, "max_hours": args.max_hours,
        "clusters": [], "budget_cny": 300, "max_nodes": 6}
    if not all(state.get(k) for k in ("run_id", "vpc", "vswitch")):
        ap.error("new state requires --run-id, --vpc and --vswitch")
    if not 0 < state["max_hours"] <= 4:
        ap.error("--max-hours must be within (0, 4]")
    REGION, VPC, SWITCH, RUN = (state[k] for k in ("region", "vpc", "vswitch", "run_id"))

    def save():
        args.state.parent.mkdir(parents=True, exist_ok=True)
        args.state.write_text(json.dumps(state, indent=2) + "\n")

    if args.action == "preflight":
        evidence = {}
        evidence["clusters"] = cli("cs", "GET", "/clusters", "--region", REGION)
        evidence["nodes_quote"] = cli("ecs", "DescribePrice", "--RegionId", REGION,
            "--ResourceType", "instance", "--InstanceType", "ecs.e-c1m2.2xlarge",
            "--PriceUnit", "Hour", "--Period", "1", "--SystemDisk.Category", "cloud_essd", "--SystemDisk.Size", "40")
        evidence["disk_quote"] = cli("ecs", "DescribePrice", "--RegionId", REGION,
            "--ResourceType", "disk", "--PriceUnit", "Hour", "--Period", "1",
            "--DataDisk.1.Category", "cloud_essd", "--DataDisk.1.Size", "100", "--DataDisk.1.PerformanceLevel", "PL0")
        evidence["account_limits"] = cli("ecs", "DescribeAccountAttributes", "--RegionId", REGION)
        evidence["stock"] = cli("ecs", "DescribeAvailableResource", "--RegionId", REGION,
            "--ZoneId", state["zone"], "--DestinationResource", "InstanceType",
            "--InstanceChargeType", "PostPaid", "--InstanceType", "ecs.e-c1m2.2xlarge")
        evidence["nat_before"] = cli("vpc", "DescribeNatGateways", "--RegionId", REGION, "--PageSize", "50")
        evidence["eip_before"] = cli("vpc", "DescribeEipAddresses", "--RegionId", REGION, "--PageSize", "100")
        evidence["disks_before"] = cli("ecs", "DescribeDisks", "--RegionId", REGION, "--PageSize", "100")
        evidence["ecs_before"] = cli("ecs", "DescribeInstances", "--RegionId", REGION, "--PageSize", "100")
        for name, page in (("nat_before", 50), ("eip_before", 100),
                           ("disks_before", 100), ("ecs_before", 100)):
            data = evidence[name]
            if data.get("TotalCount", 0) > page or data.get("NextToken"):
                raise RuntimeError("preflight inventory requires pagination: " + name)
        node_price = evidence["nodes_quote"]["PriceInfo"]["Price"]["TradePrice"]
        disk_price = evidence["disk_quote"]["PriceInfo"]["Price"]["TradePrice"]
        # Two NAT gateways / endpoints / control-plane miscellaneous reserve 6 CNY/h;
        # 30 GiB egress at historical 0.8 plus 60 CNY contingency.
        estimate = (node_price * 6 + disk_price * 7 + 6) * state["max_hours"] + 24 + 60
        state["preflight"] = evidence
        state["estimate_cny"] = round(estimate, 2)
        state["preflight_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        save()
        print(json.dumps({"estimate_cny": state["estimate_cny"], "node_hour_cny": node_price,
                          "disk_100gb_hour_cny": disk_price, "budget_cny": 300}))
        if estimate > 300:
            raise SystemExit("quote estimate exceeds approved cap")
    elif args.action == "create":
        if not state.get("preflight") or state.get("estimate_cny", 301) > 300:
            raise SystemExit("preflight required within approved cap")
        stamp = dt.datetime.fromisoformat(state["preflight_utc"])
        if dt.datetime.now(dt.timezone.utc) - stamp > dt.timedelta(hours=1):
            raise SystemExit("preflight older than one hour; recheck")
        state.setdefault("started_utc", dt.datetime.now(dt.timezone.utc).isoformat())
        state.setdefault("deadline_utc", (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=state["max_hours"])).isoformat())
        save()
        for letter in ("a", "b"):
            require_time_remaining(state)
            name = f"mst2-medium-{letter}-{RUN}"
            if any(c["name"] == name for c in state["clusters"]):
                continue
            # Inventory-check before retry protects against an ambiguous timed-out create.
            all_clusters = cli("cs", "GET", "/clusters", "--region", REGION)
            candidates = all_clusters if isinstance(all_clusters, list) else all_clusters.get("clusters", [])
            matches = [c for c in candidates if c.get("name") == name]
            if matches:
                raise SystemExit("existing matching name requires reconciliation before retry: " + name)
            body = {"name": name, "cluster_type": "ManagedKubernetes", "cluster_spec": "ack.standard",
                    "region_id": REGION, "vpcid": VPC, "vswitch_ids": [SWITCH],
                    "master_vswitch_ids": [SWITCH], "worker_vswitch_ids": [SWITCH],
                    "container_cidr": "10.244.0.0/16" if letter == "a" else "10.245.0.0/16",
                    "service_cidr": "10.96.0.0/16" if letter == "a" else "10.97.0.0/16",
                    "kubernetes_version": "1.34.10-aliyun.1", "num_of_nodes": 0,
                    "snat_entry": True, "endpoint_public_access": True, "deletion_protection": False,
                    "runtime": {"name": "containerd", "version": "2.1.9"},
                    "addons": [{"name": "flannel"}, {"name": "csi-plugin"}, {"name": "csi-provisioner"},
                               {"name": "logtail-ds", "disabled": True},
                               {"name": "nginx-ingress-controller", "disabled": True}],
                    "tags": [{"key": "purpose", "value": RUN}], "timeout_mins": 30}
            require_time_remaining(state)
            response = cli("cs", "POST", "/clusters", "--region", REGION, body=body)
            state["clusters"].append({"name": name, "id": response["cluster_id"], "create_response": response})
            save()
            print(json.dumps(response), flush=True)
    elif args.action == "status":
        for c in state["clusters"]:
            info = cli("cs", "GET", f'/clusters/{c["id"]}', "--region", REGION)
            pools = cli("cs", "GET", f'/clusters/{c["id"]}/nodepools', "--region", REGION)
            c["last_state"] = info.get("state")
            c["last_pools"] = pools
            save()
            print(json.dumps({"id": c["id"], "name": c["name"], "state": c["last_state"], "pools": pools}))
    elif args.action == "nodepools":
        for c in state["clusters"]:
            require_time_remaining(state)
            if c.get("last_state") != "running":
                raise SystemExit("cluster must be running before creating pools")
            for role in ("storage", "service", "runner"):
                require_time_remaining(state)
                existing = cli("cs", "GET", f'/clusters/{c["id"]}/nodepools', "--region", REGION)
                pools = existing.get("nodepools", []) if isinstance(existing, dict) else existing
                names = [p.get("nodepool_info", {}).get("name") for p in pools]
                if f"medium-{role}" in names:
                    continue
                if any(name not in ["medium-storage", "medium-service", "medium-runner"] for name in names):
                    raise SystemExit("unexpected nodepool; inspect default pool before adding cost")
                body = {"nodepool_info": {"name": f"medium-{role}", "type": "ess"},
                        "auto_scaling": {"enable": False},
                        "management": {"enable": False},
                        "kubernetes_config": {"runtime": "containerd", "runtime_version": "2.1.9",
                           "cms_enabled": False, "cpu_policy": "none", "labels": [{"key": "bench-role", "value": role}]},
                        "scaling_group": {"vswitch_ids": [SWITCH], "instance_types": ["ecs.e-c1m2.2xlarge"],
                           "instance_charge_type": "PostPaid", "desired_size": 1, "min_size": 1, "max_size": 1,
                           "system_disk_category": "cloud_essd", "system_disk_size": 40,
                           "system_disk_performance_level": "PL1", "spot_strategy": "NoSpot",
                           "login_password": "M2!" + secrets.token_hex(10) + "a9",
                           "image_type": "Ubuntu", "tags": [{"key": "purpose", "value": RUN}]}}
                require_time_remaining(state)
                response = cli("cs", "POST", f'/clusters/{c["id"]}/nodepools', "--region", REGION, body=body)
                c.setdefault("pool_responses", {})[role] = response
                save()
                print(json.dumps({"cluster": c["id"], "role": role, "response": response}), flush=True)
    elif args.action == "kubeconfig":
        for c in state["clusters"]:
            response = cli("cs", "GET", f'/k8s/{c["id"]}/user_config', "--region", REGION)
            path = args.state.parent / (c["name"] + ".conf")
            path.write_text(response["config"])
            print("saved kubeconfig: " + str(path))
    else:
        for c in state["clusters"]:
            response = cli("cs", "DELETE", f'/clusters/{c["id"]}', "--region", REGION)
            c["delete_response"] = response
            save()
            print(json.dumps({"cluster": c["id"], "delete_response": response}), flush=True)


if __name__ == "__main__":
    main()
