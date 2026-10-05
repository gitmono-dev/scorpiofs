#!/usr/bin/env python3
"""Inventory run-owned residuals, preserving resources present before the run."""
import argparse
import datetime as dt
import json
from pathlib import Path
import runpy


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--mark-clean", action="store_true")
    a = p.parse_args()
    state = json.loads(a.state.read_text())
    api = runpy.run_path(str(Path(__file__).with_name("ack-medium.py")))["cli"]
    ids = {c["id"] for c in state["clusters"]}
    run = state["run_id"]
    names = {f"mst2-medium-{letter}-{run}" for letter in ("a", "b")}
    inventory_path = a.state.parent / "audit-pre-cleanup.json"
    inventory = json.loads(inventory_path.read_text()) if inventory_path.exists() else {}
    identity_fields = {"nat": "NatGatewayId", "eip": "AllocationId", "slb": "LoadBalancerId",
                       "security_groups": "SecurityGroupId", "snapshots": "SnapshotId",
                       "scaling_groups": "ScalingGroupId", "instances": "InstanceId"}

    def known(name, row):
        field = identity_fields[name]
        return row.get(field) in {r[field] for r in inventory.get(name, [])}

    def owned(row):
        tags = row.get("Tags", {}).get("Tag", [])
        return any(t.get("TagValue", t.get("Value")) in ids | {run} for t in tags)

    before_disks = {d["DiskId"] for d in state["preflight"]["disks_before"]["Disks"]["Disk"]}
    before_instances = {d["InstanceId"] for d in state["preflight"]["ecs_before"]["Instances"]["Instance"]}
    disks = api("ecs", "DescribeDisks", "--RegionId", state["region"], "--PageSize", "100")
    instances = api("ecs", "DescribeInstances", "--RegionId", state["region"], "--PageSize", "100")
    clusters = api("cs", "GET", "/clusters", "--region", state["region"])
    rows = clusters if isinstance(clusters, list) else clusters.get("clusters", [])
    result = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "run_id": run,
              # A timed-out create can succeed without recording its ID in state.
              "clusters": [r for r in rows if r.get("cluster_id") in ids or r.get("name") in names],
              "instances": [r for r in instances["Instances"]["Instance"] if (owned(r) or known("instances", r)) and r["InstanceId"] not in before_instances],
              "disks": [r for r in disks["Disks"]["Disk"] if owned(r) and r["DiskId"] not in before_disks]}
    active_disks = a.state.parent/"disks-active.json"
    known_disks = set()
    if active_disks.exists():
        known_disks = {r["DiskId"] for r in json.loads(active_disks.read_text())["Disks"]["Disk"]} - before_disks
        result["disks"] = [r for r in disks["Disks"]["Disk"] if r["DiskId"] in known_disks or (owned(r) and r["DiskId"] not in before_disks)]
    for name, service, action, key, item, page in (
        ("nat", "vpc", "DescribeNatGateways", "NatGateways", "NatGateway", "50"),
        ("eip", "vpc", "DescribeEipAddresses", "EipAddresses", "EipAddress", "100"),
        ("slb", "slb", "DescribeLoadBalancers", "LoadBalancers", "LoadBalancer", "100"),
        ("security_groups", "ecs", "DescribeSecurityGroups", "SecurityGroups", "SecurityGroup", "100"),
        ("snapshots", "ecs", "DescribeSnapshots", "Snapshots", "Snapshot", "100"),
        ("scaling_groups", "ess", "DescribeScalingGroups", "ScalingGroups", "ScalingGroup", "50"),
    ):
        data = api(service, action, "--RegionId", state["region"], "--PageSize", page)
        if data.get("TotalCount", 0) > int(page) or data.get("NextToken"):
            raise RuntimeError("inventory requires pagination: " + name)
        result[name] = [r for r in data[key][item] if owned(r) or known(name, r) or (name == "snapshots" and r.get("SourceDiskId") in known_disks)]
    if disks.get("TotalCount", 0) > 100 or instances.get("TotalCount", 0) > 100:
        raise RuntimeError("inventory requires pagination")
    result["original_instances_preserved"] = before_instances <= {r["InstanceId"] for r in instances["Instances"]["Instance"]}
    categories = ("clusters", "instances", "disks", "nat", "eip", "slb", "security_groups", "snapshots", "scaling_groups")
    result["remaining_counts"] = {k: len(result[k]) for k in categories}
    result["cleanup_verified"] = not any(result["remaining_counts"].values()) and result["original_instances_preserved"]
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(result, indent=2)+"\n")
    if a.mark_clean:
        if not result["cleanup_verified"]:
            raise SystemExit("remaining run-owned resources; state remains unverified")
        state.update(cleanup_verified=True, cleanup_verified_utc=result["utc"])
        a.state.write_text(json.dumps(state, indent=2)+"\n")
    print(json.dumps({k: result[k] for k in ("remaining_counts", "original_instances_preserved", "cleanup_verified")}))


if __name__ == "__main__":
    main()
