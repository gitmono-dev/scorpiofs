#!/usr/bin/env python3
"""Deterministic, byte-sized medium monorepo; manifest stays outside the repo."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import subprocess


def generate(root, manifest, count, seed):
    rng = random.Random(seed)
    root.mkdir(parents=True, exist_ok=True)
    total = 0
    dirs = set()
    with manifest.open("w") as out:
        for i in range(count):
            # Assignment depends only on index, so the 100k tree is a 200k prefix.
            bucket = i % 1000
            size = rng.randint(1024, 8192) if bucket < 700 else (
                rng.randint(8192, 65536) if bucket < 999 else rng.randint(1048576, 8388608))
            if i % 40 == 0:
                rel = f"wide/f{i:06d}.dat"
            else:
                depth = 3 + i % 4
                pieces = [f"svc{i % 40:02d}", f"pkg{(i // 40) % 100:03d}"]
                pieces += [f"d{level}-{(i // 4000 + level) % 10}" for level in range(depth - 2)]
                rel = "/".join(pieces + [f"f{i:06d}." + ["rs", "json", "md", "txt"][i % 4]])
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            dirs.add(str(path.parent.relative_to(root)))
            # Half structured text, half unique pseudo-random hexadecimal data.
            # Avoid both unrealistically identical blobs and only incompressible noise.
            header = f"// fixture seed={seed} file={i} path={rel}\n".encode()
            payload = bytearray(header)
            while len(payload) < size:
                payload.extend(f"record {len(payload):08d} value=".encode())
                payload.extend(rng.randbytes(24).hex().encode())
                payload.extend(b"; source data configuration test document\n")
            data = bytes(payload[:size])
            path.write_bytes(data)
            mode = 0o755 if i % 5000 == 0 else 0o644
            path.chmod(mode)
            out.write(json.dumps({"path": rel, "type": "file", "mode": mode,
                                  "size": size, "sha256": hashlib.sha256(data).hexdigest()}) + "\n")
            total += size
            if (i + 1) % 10000 == 0:
                print(json.dumps({"generated": i + 1, "bytes": total}), flush=True)
    summary = {"files": count, "logical_bytes": total, "leaf_directories": len(dirs), "seed": seed}
    manifest.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)


def verify(root, manifest):
    total = count = 0
    expected = set()
    with manifest.open() as src:
        for line in src:
            row = json.loads(line)
            p = root / row["path"]
            data = p.read_bytes()
            if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
                raise RuntimeError("content mismatch: " + row["path"])
            if p.stat().st_mode & 0o777 != row["mode"]:
                raise RuntimeError("mode mismatch: " + row["path"])
            expected.add(row["path"])
            total += len(data)
            count += 1
    actual = set()
    for p in root.rglob("*"):
        rel = p.relative_to(root)
        if ".git" in rel.parts:
            continue
        if p.is_file() or p.is_symlink():
            actual.add(str(rel))
    if actual != expected:
        raise RuntimeError(f"tree mismatch: extra={len(actual-expected)}, missing={len(expected-actual)}")
    print(json.dumps({"verified_files": count, "logical_bytes": total}), flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("action", choices=["generate", "verify"])
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--files", type=int, default=200000)
    ap.add_argument("--seed", type=int, default=20261003)
    args = ap.parse_args()
    if args.action == "generate":
        if args.root.exists() and any(args.root.iterdir()):
            raise SystemExit("generation requires an empty directory")
        if args.files < 1:
            raise SystemExit("files must be positive")
        generate(args.root, args.manifest, args.files, args.seed)
    else:
        verify(args.root, args.manifest)


if __name__ == "__main__":
    main()
