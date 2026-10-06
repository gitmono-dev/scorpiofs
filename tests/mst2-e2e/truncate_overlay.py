"""Check copy-up and truncation on a disposable fixed v3 workspace."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:37255")
    parser.add_argument("--scope", default="/project/bench50k")
    parser.add_argument("--file", default="svc00/pkg000/mod00/f00000.rs")
    args = parser.parse_args()
    relative = Path(args.file)
    if relative.is_absolute() or ".." in relative.parts:
        parser.error("--file must be a scope-relative path without '..'")
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(path, body=None, method="GET"):
        request = urllib.request.Request(
            args.api + path, method=method,
            headers={"Content-Type": "application/json"},
            data=None if body is None else json.dumps(body).encode(),
        )
        with client.open(request, timeout=60) as response:
            raw = response.read()
            return json.loads(raw) if raw else None

    created = call("/v3/workspaces", {
        "target": {"kind": "latest"}, "scope": args.scope,
        "delivery": "lazy", "upper_policy": "private",
    }, "POST")
    result = {"status": "running", "scope": args.scope, "file": args.file, "checks": []}
    try:
        assert created["mount_state"] == "mounted" and created["metadata_ready"]
        result.update(workspace_id=created["workspace_id"],
                      generation=created["generation"], snapshot_id=created["snapshot_id"])
        mount = Path(created["mountpoint"])
        # This host-side regression explicitly checks the private upper owned by
        # the fixed workspace. It does not request another projection or refresh.
        upper = mount.parent() / "upper"
        target = mount / relative
        original = target.read_bytes()

        def check(label, expected):
            actual = target.read_bytes()
            host = (upper / relative).read_bytes()
            assert actual == expected, (label, "visible bytes", actual.hex(), expected.hex())
            assert host == expected, (label, "upper bytes", host.hex(), expected.hex())
            assert target.stat().st_size == len(expected), (label, "visible size")
            assert (upper / relative).stat().st_size == len(expected), (label, "upper size")
            result["checks"].append({"phase": label, "bytes": len(expected),
                                     "sha256": hashlib.sha256(actual).hexdigest()})

        # The first writable open itself truncates an existing lower file.
        fd = os.open(target, os.O_WRONLY | os.O_TRUNC)
        os.close(fd)
        check("first_copy_up_truncate_without_write", b"")
        target.write_bytes(original)
        check("rewrite_original", original)
        with target.open("ab") as stream:
            stream.write(b"appended-old-tail\n")
        check("append", original + b"appended-old-tail\n")
        fd = os.open(target, os.O_WRONLY | os.O_TRUNC)
        os.close(fd)
        check("truncate_after_append_without_write", b"")
        target.write_bytes(b"short")
        check("short_rewrite", b"short")
        with target.open("r+b") as stream:
            stream.truncate(2)
        check("explicit_ftruncate", b"sh")
        target.write_bytes(original)
        check("restore_original", original)

        new_file = mount / ("truncate-new-" + str(uuid.uuid4()))
        new_file.write_bytes(b"long-new-content")
        new_file.write_bytes(b"new")
        assert new_file.read_bytes() == b"new"
        new_file.unlink()
        assert not new_file.exists()
        result["checks"].append({"phase": "new_file_truncate_and_delete"})
        target.unlink()
        assert not target.exists(), "deleted lower file must stay hidden"
        result["checks"].append({"phase": "lower_file_whiteout"})
        result["status"] = "success"
    except Exception as error:
        result.update(status="failed", error=str(error))
        raise
    finally:
        try:
            call("/v3/workspaces/" + created["workspace_id"] + "/destroy",
                 {"discard_dirty": True}, "POST")
            result["mount_deleted"] = True
        finally:
            print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
