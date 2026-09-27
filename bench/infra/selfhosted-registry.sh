#!/usr/bin/env bash
# Self-hosted container registry on the 2c4g ECS (112.124.50.201).
#
# Why: ACR Enterprise instances cost money and ACR Personal's API is
# unreachable from this account; the ECS already runs and has spare
# capacity (2 vCPU / 3.5 GB, 30 GB free — the registry uses ~26 MB plus
# whatever images are stored).
#
# Setup already applied on the host (via Cloud Assistant, no SSH needed):
#   dnf install -y podman podman-docker
#   podman pull docker.m.daocloud.io/library/registry:2   # Docker Hub is blocked
#   podman run -d --name registry --restart=always -p 5000:5000 \
#          -v /srv/registry:/var/lib/registry registry:2
#
# Notes learned the hard way:
#   - Docker Hub is unreachable from the ECS; use a mirror prefix
#     (docker.m.daocloud.io works; aliyun's registry.cn-hangzhou.aliyuncs.com
#     requires auth for library/* and returns access denied).
#   - The security group must allow :5000 from the CLIENT's real egress IP —
#     this client has TWO rotating egress IPs (202.119.42.194 / 202.119.41.223),
#     and the proxy egress (189.24.123.153) is a third, different one. Allow all
#     that actually reach the host, not just the proxy's.
#   - The registry speaks plain HTTP, so the client's dockerd needs the address
#     under `insecure-registries` in /etc/docker/daemon.json + a restart.
#
# Usage (from WSL):
#   bash selfhosted-registry.sh            # status + catalog
#   bash selfhosted-registry.sh pull       # pull both images from the ECS
set -uo pipefail
REG="${REG:-112.124.50.201:5000}"

case "${1:-status}" in
  status)
    echo "== registry $REG =="
    curl -sS --noproxy '*' -m 10 -o /dev/null -w "  /v2/ -> %{http_code}\n" "http://$REG/v2/"
    echo -n "  catalog: "; curl -sS --noproxy '*' "http://$REG/v2/_catalog"; echo
    for n in mega2 scorpiofs; do
      echo -n "  $n: "; curl -sS --noproxy '*' "http://$REG/v2/$n/tags/list"; echo
    done
    ;;
  pull)
    for n in mega2 scorpiofs; do
      echo "== pull $REG/$n =="
      docker pull "$REG/$n:latest" 2>&1 | tail -2
    done
    ;;
  push)
    bash "$(dirname "$0")/push-to-selfhosted.sh"
    ;;
  *) echo "usage: $0 [status|pull|push]" >&2; exit 1;;
esac
