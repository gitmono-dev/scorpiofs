#!/usr/bin/env python3
"""Measure snapshot protocol phases separately from mount readiness."""

import argparse
import json
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:19000")
    parser.add_argument("--scope", default="/project/bench50k")
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(suffix, body=None, lease=None, method=None):
        headers = {"Content-Type": "application/json"}
        if lease:
            headers["X-Mega-Snapshot-Lease"] = lease
        req = urllib.request.Request(
            args.base + suffix, headers=headers, method=method,
            data=None if body is None else json.dumps(body).encode(),
        )
        start = time.monotonic_ns()
        with client.open(req, timeout=90) as response:
            data = response.read()
            stats = {"elapsed_ms": round((time.monotonic_ns() - start) / 1_000_000, 3),
                     "http": response.status, "bytes": len(data)}
        return data, stats

    failed = False
    for number in range(1, args.rounds + 1):
        lease = None
        result = {"round": number, "scope": args.scope, "status": "failed"}
        try:
            _, result["capabilities"] = request("/api/v2/snapshots/capabilities")
            raw, result["resolve"] = request("/api/v2/snapshots/resolve", {
                "target": {"kind": "latest"}, "scope": args.scope,
                "delivery": "full", "lease_seconds": 300,
                "supported_metadata_codecs": [1],
            })
            resolved = json.loads(raw)
            lease = resolved["lease_id"]
            descriptor = resolved["descriptor"]
            _, result["root_metadata_page"] = request(
                "/api/v2/snapshots/" + descriptor["snapshot_id"] + "/metadata/pages",
                {"items": [{"directory_path": "/", "route": [],
                            "expected_digest": descriptor["metadata_root"]}]}, lease,
            )
            result.update(status="success", snapshot_id=descriptor["snapshot_id"])
        except Exception as error:
            result["error"] = str(error)
        finally:
            if lease:
                try:
                    request("/api/v2/snapshots/leases/" + lease, lease=lease, method="DELETE")
                    result["lease_released"] = True
                except Exception as error:
                    result.update(status="failed", cleanup_error=str(error))
            print(json.dumps(result), flush=True)
            failed |= result["status"] != "success"
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
