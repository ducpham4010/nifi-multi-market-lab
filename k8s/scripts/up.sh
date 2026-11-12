#!/usr/bin/env bash
# Build the whole Kubernetes lab in the right order. From scratch after down.sh, or
# safely again on a running lab (every step is idempotent and keeps existing data).
#   bash k8s/scripts/up.sh
#   SKIP_BUILD=1 bash k8s/scripts/up.sh      # reuse the NiFi image already in the registry
#
#   1 deploy.sh            image, namespaces, Secrets from .env, manifests, wait for pods
#   2 Gateway routes       wait until Vault VN/INDO and GitLab answer on the LAN URLs
#   3 vault-init.py        init/unseal, KV seed, Kubernetes auth roles per NiFi namespace
#   4 gitlab-bootstrap.py  group/project, protected main, 3 Project Access Tokens -> Vault
#   5 nifi-sync.sh         Jobs (ServiceAccount nifi): shared-runtime and Registry Client
#   6 check-k8s.py         read-only readiness checks
source "$(dirname "$0")/common.sh"
scripts="$K8S_DIR/scripts"
started=$SECONDS

step() { printf '\n######## %s\n' "$*"; }

wait_http() { # url label: wait for HTTP 200 through the Gateway (up to 10 minutes)
  local code=000
  for _ in $(seq 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1" || true)"
    if [[ "$code" == 200 ]]; then echo "   $2: HTTP 200"; return 0; fi
    sleep 5
  done
  echo "   $2 not reachable at $1 (last HTTP $code)" >&2
  return 1
}

step "1/6 Deploy infrastructure (deploy.sh)"
bash "$scripts/deploy.sh"

step "2/6 Wait for LAN routes through the Gateway"
for m in vn indo; do
  wait_http "http://vault-$m.$HOST_IP.sslip.io:31080/v1/sys/health?standbyok=true&sealedcode=200&uninitcode=200" "Vault $m"
done
wait_http "http://gitlab.$HOST_IP.sslip.io:31080/users/sign_in" "GitLab"

step "3/6 Vault: init/unseal, KV seed, Kubernetes auth roles (vault-init.py)"
python3 "$scripts/vault-init.py"

step "4/6 GitLab: project, protected main, Project Access Tokens (gitlab-bootstrap.py)"
python3 "$scripts/gitlab-bootstrap.py"

step "5/6 NiFi: shared-runtime and Registry Client through ServiceAccount Jobs"
bash "$scripts/nifi-sync.sh" params all
bash "$scripts/nifi-sync.sh" registry all

step "6/6 Readiness checks (check-k8s.py)"
python3 "$scripts/check-k8s.py"

printf '\nLab ready in %d min. Vault keys and root tokens: .private/k8s/; Project Access Tokens: bash k8s/scripts/gitlab-token.sh stag|vn|indo\n' \
  $(((SECONDS - started) / 60))
