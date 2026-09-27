#!/usr/bin/env bash
# Deploy the mega2 stack to ACK, pulling images from the user's ACR personal
# registry (a managed endpoint the nodes reach natively — this is what the
# earlier VPC-self-hosted-registry attempt could not do).
#
# Steps: wait for a node -> build the ACR imagePullSecret -> apply the
# manifests -> wait for mega2 -> run the git push smoke test.
set -uo pipefail
export KUBECONFIG="$HOME/gitmono-ack.conf"
HERE="$(cd "$(dirname "$0")" && pwd)"
YAML="$HERE/k8s/mega2-ack.yaml"

REG="${ACR_REG_ENDPOINT:-crpi-r1oa3ys0csfxkdeq-vpc.cn-hangzhou.personal.cr.aliyuncs.com}"
USER="${ACR_USER:-aliyun8193774171}"
PASS="${ACR_PASS:?set ACR_PASS (ACR registry password)}"

echo "== wait for a Ready node =="
for i in $(seq 1 40); do
  N=$(kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready")
  [ "$N" -ge 1 ] && { echo "  node ready"; break; }
  sleep 15
done
kubectl get nodes --no-headers 2>&1 | head -3

echo "== create namespace first (the secret needs it) =="
kubectl create namespace gitmono --dry-run=client -o yaml | kubectl apply -f - 2>&1 | tail -1

echo "== build the ACR pull secret =="
# Create it directly with kubectl rather than templating it into the YAML:
# embedding a JSON object inside stringData trips the YAML parser
# ("cannot unmarshal object into Go struct field Secret.stringData of type string").
kubectl -n gitmono create secret docker-registry acr-pull \
  --docker-server="$REG" \
  --docker-username="$USER" \
  --docker-password="$PASS" \
  --dry-run=client -o yaml | kubectl apply -f - 2>&1 | tail -1

echo "== apply manifests (secret placeholder stripped out) =="
# Drop the placeholder Secret document; the real one was created above.
python3 - "$YAML" <<'PY' | kubectl apply -f - 2>&1 | tail -12
import sys, re
doc = open(sys.argv[1], encoding='utf-8').read()
# remove the Secret block (from its apiVersion up to the next '---')
doc = re.sub(r'apiVersion: v1\nkind: Secret\n.*?\n---\n', '', doc, count=1, flags=re.S)
sys.stdout.write(doc)
PY

echo "== wait for mega2 =="
kubectl -n gitmono rollout status deploy/mega2 --timeout=300s 2>&1 | tail -2
kubectl -n gitmono get pods 2>&1 | head -12
