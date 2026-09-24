#!/usr/bin/env bash
# bench/infra/init-runner-mono.sh — mono 侧 runner 初始化（Ubuntu 22.04/24.04 ECS）。
# 安装: build 工具链、rust stable、scorpiofs+libra 二进制（从源码或预编译）、opencode；
# systemd unit: scorpio daemon（FUSE allow_other，sudo 权限走 unit）。
set -euo pipefail
M2_REPO="${M2_REPO:-https://github.com/gitmono-dev/scorpiofs}"
LIBRA_REPO="${LIBRA_REPO:-https://github.com/gitmono-dev/libra}"
SCORPIOFS_DIR="${SCORPIOFS_DIR:-$HOME/scorpiofs}"
LIBRA_DIR="${LIBRA_DIR:-$HOME/libra}"

echo "== 1/5 system deps =="
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev libfuse3-dev \
  fuse3 curl git python3 jq vnstat docker.io
sudo usermod -aG fuse "$USER" || true
# FUSE: daemon 需要 allow_other
grep -q user_allow_other /etc/fuse.conf || echo user_allow_other | sudo tee -a /etc/fuse.conf

echo "== 2/5 rust stable =="
if ! command -v cargo >/dev/null; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi
export PATH="$HOME/.cargo/bin:$PATH"

echo "== 3/5 build scorpiofs + libra =="
[ -d "$SCORPIOFS_DIR" ] || git clone -q "$M2_REPO" "$SCORPIOFS_DIR"
[ -d "$LIBRA_DIR" ] || git clone -q "$LIBRA_REPO" "$LIBRA_DIR"
(cd "$SCORPIOFS_DIR" && cargo build --release)
(cd "$LIBRA_DIR" && cargo build --release)
sudo install -m755 "$SCORPIOFS_DIR/target/release/scorpio" /usr/local/bin/scorpio
sudo install -m755 "$LIBRA_DIR/target/release/libra" /usr/local/bin/libra

echo "== 4/5 systemd unit (scorpio daemon) =="
sudo tee /etc/systemd/system/scorpio.service >/dev/null <<EOF
[Unit]
Description=ScorpioFS daemon (FUSE)
After=network-online.target
[Service]
Type=simple
User=root
Environment=SCORPIO_MEGA_URL=${MEGA_URL:-http://127.0.0.1:19000}
WorkingDirectory=/root
ExecStart=/usr/local/bin/scorpio serve
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
echo "  (start with: sudo systemctl start scorpio  — after config.toml is in place)"

echo "== 5/5 opencode =="
if ! command -v opencode >/dev/null; then
  curl -fsSL https://opencode.ai/install | bash
fi
echo "  configure provider: opencode auth login   (free model of your choice)"

cat <<'EOF'
== runner-mono ready ==
后续步骤:
  1. /etc/scorpio/config.toml 或 scorpiofs 目录下的 scorpio.toml 指向 mega2
  2. sudo systemctl start scorpio && curl http://127.0.0.1:37251/antares/health
  3. bash bench/workload/seed-mono.sh --src <repo> --name <name>
  4. bash bench/cases/exp1.sh all
EOF
