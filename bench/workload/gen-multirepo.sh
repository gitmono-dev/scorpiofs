#!/usr/bin/env bash
# bench/workload/gen-multirepo.sh — 生成 1 公共库 + 5 服务的关联仓库组。
# 产出两个变体（内容等价）：
#   $OUT/git/    — gitea 用：common + svc-a..e，服务以 submodule 引用 common
#   $OUT/mono/   — mega2 用：同一内容的单树（common/ + svc-*/ 顶层目录）
# 用法: gen-multirepo.sh --out /tmp/multi [--services 5]
set -euo pipefail
OUT=""; SVCS=5
while [ $# -gt 0 ]; do case "$1" in
  --out) OUT="$2"; shift 2;; --services) SVCS="$2"; shift 2;;
  *) echo "unknown arg $1" >&2; exit 1;;
esac; done
[ -n "$OUT" ] || { echo "usage: $0 --out DIR [--services N]" >&2; exit 1; }
rm -rf "$OUT"; mkdir -p "$OUT/git" "$OUT/mono"
NAMES=(common); for i in $(seq 0 $((SVCS-1))); do NAMES+=("svc-$(printf '%c' $((97+i)))"); done

api_file() { # $1=repo  $2=func  $3=ver
  cat <<EOF
// $1/src/api.rs
pub const VERSION: &str = "$3";
pub fn $2(x: u64) -> u64 { x.wrapping_mul(2).wrapping_add($3 as u64) }
EOF
}
svc_file() { # $1=repo $2=dep-func $3=port
  cat <<EOF
// $1/src/main.rs
mod shim;
fn main() {
    let v = shim::dep_call(21);
    println!("$1 listening on :$3, dep() = {v}");
}
EOF
}
shim_git() { cat <<EOF
// $1/src/shim.rs — git variant: include_path to the submodule checkout
#[path = "../../common/src/api.rs"]
mod api;
pub fn dep_call(x: u64) -> u64 { api::dep(x) }
EOF
}
shim_mono() { cat <<EOF
// $1/src/shim.rs — mono variant: path into the shared tree
#[path = "../../common/src/api.rs"]
mod api;
pub fn dep_call(x: u64) -> u64 { api::dep(x) }
EOF
}

# ---------- git variant: N independent repos ----------
declare -A GITURL
for name in "${NAMES[@]}"; do
  r="$OUT/git/$name"; mkdir -p "$r/src"
  if [ "$name" = common ]; then
    api_file "$name" dep 1 > "$r/src/api.rs"
  else
    port=$((8000 + RANDOM % 1000))
    svc_file "$name" dep "$port" > "$r/src/main.rs"
    shim_git "$name" > "$r/src/shim.rs"
  fi
  ( cd "$r" && git init -q -b main && git add -A \
    && git commit -qm "init $name (git variant)" )
  GITURL[$name]="$r"
done
# 服务挂 submodule（相对路径，gitea 上同样成立）
for name in "${NAMES[@]:1}"; do
  ( cd "$OUT/git/$name" \
    && git -c protocol.file.allow=always submodule add -q ../common common \
    && git commit -qm "track common via submodule" )
done
echo "git variant: $OUT/git ($(for n in "${NAMES[@]}"; do echo -n "$n "; done))"

# ---------- mono variant: single tree, same content ----------
for name in "${NAMES[@]}"; do
  mkdir -p "$OUT/mono/$name/src"
  if [ "$name" = common ]; then
    api_file "$name" dep 1 > "$OUT/mono/$name/src/api.rs"
  else
    cp "$OUT/git/$name/src/main.rs" "$OUT/mono/$name/src/main.rs"
    shim_mono "$name" > "$OUT/mono/$name/src/shim.rs"
  fi
done
( cd "$OUT/mono" && git init -q -b main && git add -A \
  && git commit -qm "init monorepo (mono variant: common + ${SVCS} services)" )
echo "mono variant: $OUT/mono"
