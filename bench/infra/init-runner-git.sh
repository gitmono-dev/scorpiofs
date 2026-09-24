#!/usr/bin/env bash
# bench/infra/init-runner-git.sh — git 对照侧 runner 初始化。
set -euo pipefail
echo "== 1/3 system deps =="
sudo apt-get update -qq
sudo apt-get install -y -qq git curl python3 jq vnstat

echo "== 2/3 git best-practice config（对照侧最优配置，报告附录引用） =="
git config --global user.name bench
git config --global user.email bench@gitmono.local
git config --global core.fsmonitor true
git config --global core.untrackedcache true
git config --global fetch.parallel 8
git config --global protocol.version 2
git version

echo "== 3/3 opencode =="
if ! command -v opencode >/dev/null; then
  curl -fsSL https://opencode.ai/install | bash
fi
echo "  configure provider: opencode auth login"

cat <<'EOF'
== runner-git ready ==
后续步骤:
  1. GITEA_TOKEN=<token from gitea> 导出
  2. bash bench/workload/seed-git.sh --tree <filtered> --repo <name>
  3. bash bench/cases/exp1.sh all   （GITEA 指向 gitea 节点时跑对照线）
EOF
