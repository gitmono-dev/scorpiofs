#!/usr/bin/env bash
# Deploy the Mega2 git server on a plain ECS with podman-compose, pulling the
# self-built image from the user's ACR *personal* registry over the VPC endpoint.
#
# Verified working end to end (clone -> commit -> push -> read back).
#
# Requires on the host: podman, podman-compose, git, and a one-time
#   podman login <vpc-endpoint> -u <user>
# (ACR's registry password, from the console's 访问凭证 page — NOT the AK.)
#
# Traps this script already handles (each cost a debugging round):
#   * MEGA_OBJECT_STORAGE__S3__ENDPOINT_URL — note the _URL suffix. Plain
#     ..._S3__ENDPOINT is silently ignored and mega2 falls back to
#     http://localhost:9000, giving confusing "connect" retry storms.
#   * mega2 needs an initialized vault: `mega2 config vault reset --force`
#     must run ON THE COMPOSE NETWORK (it talks to postgres). A bare
#     `podman run` without --net cannot reach postgres and times out.
#   * The S3 bucket must exist before mega2 starts, or it exits with
#     NoSuchBucket. rustfs/rc is NOT on the daocloud mirror allowlist, so the
#     image has to be pushed to ACR first; and its CLI is
#       rc alias set <name> <endpoint> <ak> <sk>   (positional, not -u/-p)
#       rc bucket create <alias>/<bucket>          (`mb` is a deprecated alias)
#   * rustfs runs as a non-root uid (10001) — its data dir must be chowned,
#     else it dies with "Io error: Permission denied".
#   * mega2's state dir (/var/lib/mega2) must be a volume, or the vault key
#     is lost on every container recreate.
set -uo pipefail

REG="${ACR_REG:-crpi-r1oa3ys0csfxkdeq-vpc.cn-hangzhou.personal.cr.aliyuncs.com}"
REPO="${ACR_REPO:-gitmono/gitmono1}"
MEGA_TAG="${MEGA_TAG:-v0.3.0-mega2}"
RC_TAG="${RC_TAG:-v0.1.36-rustfs-rc}"
WORK="${WORK:-/srv/gitmono-demo}"

echo "== prepare dirs (rustfs needs a writable, non-root-owned dir) =="
mkdir -p "$WORK"/{pg,rustfs,megastate}
chmod 777 "$WORK/rustfs" "$WORK/megastate"

echo "== stage public base images under the names compose expects =="
for i in postgres:18.6-alpine3.24 redis:8.10.1-alpine3.23; do
  podman tag "docker.m.daocloud.io/library/$i" "docker.io/library/$i" 2>/dev/null || true
done
podman pull -q "$REG/$REPO:$RC_TAG" 2>/dev/null || true

echo "== write compose =="
cat > "$WORK/compose.yml" <<EOF
services:
  postgres:
    image: postgres:18.6-alpine3.24
    environment:
      POSTGRES_USER: mega2
      POSTGRES_PASSWORD: mega2_local_password
      POSTGRES_DB: mega2
    volumes: ["$WORK/pg:/var/lib/postgresql"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mega2 -d mega2"]
      interval: 3s
      retries: 30

  redis:
    image: redis:8.10.1-alpine3.23
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 3s
      retries: 30

  rustfs:
    image: rustfs/rustfs:1.0.0
    command: ["/data"]
    environment:
      RUSTFS_ACCESS_KEY: rustfs
      RUSTFS_SECRET_KEY: rustfs_secret
    volumes: ["$WORK/rustfs:/data"]

  rustfs-init:
    image: rustfs/rc:v0.1.36
    depends_on: [rustfs]
    entrypoint: >
      /bin/sh -c "
      rc alias set local http://rustfs:9000 rustfs rustfs_secret &&
      rc bucket create local/mega2 || true;
      exec sleep infinity"

  mega2:
    image: $REG/$REPO:$MEGA_TAG
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }
      rustfs-init: { condition: service_started }
    environment:
      MEGA_BASE_DIR: /var/lib/mega2
      MEGA_DATABASE__DB_TYPE: postgres
      MEGA_DATABASE__DB_URL: postgres://mega2:mega2_local_password@postgres:5432/mega2
      MEGA_DATABASE__MIN_CONNECTION: "1"
      MEGA_REDIS__URL: redis://redis:6379
      MEGA_MONOREPO__PUSH_POLICY: trunk
      MEGA_CEDAR__ENFORCEMENT: "off"
      MEGA_GIT__ANONYMOUS_ACCESS: "true"
      MEGA_GIT__PUSH_AUTH: none
      MEGA_GIT__SSH_RECEIVE_PACK: "false"
      MEGA_OBJECT_STORAGE__STORAGE_TYPE: s3compatible
      MEGA_OBJECT_STORAGE__S3__REGION: us-east-1
      MEGA_OBJECT_STORAGE__S3__BUCKET: mega2
      MEGA_OBJECT_STORAGE__S3__ACCESS_KEY_ID: rustfs
      MEGA_OBJECT_STORAGE__S3__SECRET_ACCESS_KEY: rustfs_secret
      MEGA_OBJECT_STORAGE__S3__ENDPOINT_URL: http://rustfs:9000
    volumes: ["$WORK/megastate:/var/lib/mega2"]
    ports: ["127.0.0.1:19001:8000"]
EOF

echo "== start deps =="
cd "$WORK"
podman-compose -f compose.yml -p gitmono up -d postgres redis rustfs rustfs-init 2>&1 | tail -3
sleep 12

echo "== initialize the vault (must share the compose network) =="
podman run --rm --net gitmono_default \
  -v "$WORK/megastate:/var/lib/mega2" \
  -e MEGA_BASE_DIR=/var/lib/mega2 \
  -e MEGA_DATABASE__DB_TYPE=postgres \
  -e MEGA_DATABASE__DB_URL=postgres://mega2:mega2_local_password@postgres:5432/mega2 \
  -e MEGA_REDIS__URL=redis://redis:6379 \
  "$REG/$REPO:$MEGA_TAG" config vault reset --force 2>&1 | tail -2

echo "== start mega2 =="
podman-compose -f compose.yml -p gitmono up -d 2>&1 | tail -3
for i in $(seq 1 30); do
  code=$(curl -sS -o /dev/null -w "%{http_code}" http://127.0.0.1:19001/api/openapi.json 2>/dev/null)
  [ "$code" = "200" ] && break
  sleep 3
done
echo "== mega2 api: $code =="
podman ps --format "{{.Names}} {{.Status}}" | grep gitmono
