#!/usr/bin/env bash
# Push the runtime images to the self-hosted registry on the ECS (plain HTTP, :5000).
set -uo pipefail
REG="${REG:-112.124.50.201:5000}"
cd /mnt/d/--------code----------/mega-scorpiofs/scorpiofs

echo "== registry reachability =="
curl -sS --noproxy '*' -m 10 -o /dev/null -w "  /v2/ -> %{http_code}\n" "http://$REG/v2/" \
  || { echo "  unreachable"; exit 1; }

echo "== allow insecure (plain-HTTP) registry =="
if [ -f /etc/docker/daemon.json ]; then
  sudo python3 - "$REG" <<'PY'
import json, sys
reg = sys.argv[1]
p = "/etc/docker/daemon.json"
d = json.load(open(p))
d.setdefault("insecure-registries", [])
if reg not in d["insecure-registries"]:
    d["insecure-registries"].append(reg)
json.dump(d, open(p, "w"), indent=2)
print("updated", p, "->", d["insecure-registries"])
PY
else
  echo "{\"insecure-registries\":[\"$REG\"]}" | sudo tee /etc/docker/daemon.json >/dev/null
  echo "created /etc/docker/daemon.json"
fi
sudo systemctl restart docker
for i in $(seq 1 20); do docker info >/dev/null 2>&1 && break; sleep 1; done
docker info 2>/dev/null | grep -A3 "Insecure Registries" | head -4

echo "== tag + push =="
push_one() { # <local-image> <repo-name>
  local src="$1" repo="$2" full="$REG/$2:latest"
  echo "  -- $src -> $full"
  docker tag "$src" "$full" || return 1
  docker push "$full" 2>&1 | tail -2
}
push_one "mega2:local"       "mega2"
push_one "scorpiofs:runtime" "scorpiofs"

echo "== verify in registry =="
curl -sS --noproxy '*' "http://$REG/v2/_catalog"; echo
for n in mega2 scorpiofs; do
  echo -n "  $n tags: "
  curl -sS --noproxy '*' "http://$REG/v2/$n/tags/list"; echo
done
