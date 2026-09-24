#!/usr/bin/env bash
# bench/workload/gen-repo.sh — 生成合成大仓库（git 仓库），保证评测两侧输入一致。
# 用法:
#   gen-repo.sh --out /tmp/synth-100k --files 100000 --avg-size 12 --depth 4 [--seed 42]
#   --avg-size 单位 KB；文件大小按对数正态近似（2KB~8×avg 分布），全部 <200KB（避开 mega2 LFS 阈值）
set -euo pipefail
OUT=""; FILES=100000; AVGSZ=12; DEPTH=4; SEED=42
while [ $# -gt 0 ]; do case "$1" in
  --out) OUT="$2"; shift 2;; --files) FILES="$2"; shift 2;;
  --avg-size) AVGSZ="$2"; shift 2;; --depth) DEPTH="$2"; shift 2;;
  --seed) SEED="$2"; shift 2;; *) echo "unknown arg $1" >&2; exit 1;;
esac; done
[ -n "$OUT" ] || { echo "usage: $0 --out DIR [--files N] [--avg-size KB] [--depth D] [--seed S]" >&2; exit 1; }
command -v python3 >/dev/null || { echo "need python3" >&2; exit 1; }

rm -rf "$OUT"; mkdir -p "$OUT"
cd "$OUT"
git init -q -b main

# 用 python 确定性生成文件树（同 seed 同输出）——shell 循环 10 万次太慢
python3 - "$OUT" "$FILES" "$AVGSZ" "$DEPTH" "$SEED" <<'PY'
import os, random, sys
out, n, avg, depth, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
rng = random.Random(seed)
words = ["core","util","net","fs","mem","sched","io","test","conf","api",
         "node","worker","driver","policy","metric","cache","log","auth","store","sync"]
exts  = ["rs","rs","rs","py","py","go","toml","json","md","yaml","c","h"]
dirs = [""]
for d in range(depth):
    dirs = dirs + [os.path.join(p, f"{rng.choice(words)}{d}{i}") for p in dirs[:max(1,len(dirs)//2)] for i in range(4)]
def abspath(i):
    d = dirs[i % len(dirs)]
    return os.path.join(d, f"{rng.choice(words)}_{i}.{rng.choice(exts)}")
buf = bytearray()
import math
for i in range(n):
    kb = max(1, min(180, int(rng.lognormvariate(math.log(avg), 0.8))))
    path = abspath(i)
    full = os.path.join(out, path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        f.write(f"// synth file {i} seed={seed}\n")
        base = f"fn synth_{i}(x: u64) -> u64 {{ x.wrapping_add({rng.randrange(1<<32)}) }}\n"
        f.write(base * max(1, kb * 8))  # ~8 行/KB 近似
    if i % 20000 == 0: print(f"  {i}/{n}", file=sys.stderr)
print(f"generated {n} files under {len(dirs)} dirs", file=sys.stderr)
PY

git add -A
git commit -qm "synthetic repo: files=$FILES depth=$DEPTH seed=$SEED"
echo "synthetic repo ready: $OUT ($(git rev-parse --short HEAD))"
