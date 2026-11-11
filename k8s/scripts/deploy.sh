#!/usr/bin/env bash
# Build/push the NiFi image, create Secrets from .env, apply all manifests, wait.
# Idempotent: safe to re-run. Does not touch Docker Compose volumes.
#   bash k8s/scripts/deploy.sh            # full deploy
#   SKIP_BUILD=1 bash k8s/scripts/deploy.sh
source "$(dirname "$0")/common.sh"

if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
  echo "== Build and push $NIFI_K8S_IMAGE (in-cluster registry, NodePort 31102)"
  docker build -q --build-arg NIFI_IMAGE="${NIFI_IMAGE:-apache/nifi:2.6.0}" -t "$NIFI_K8S_IMAGE" nifi
  docker push -q "$NIFI_K8S_IMAGE"
fi

if [[ "${PRELOAD_FROM_DOCKER:-1}" == 1 ]] && command -v docker >/dev/null && sudo -n true 2>/dev/null; then
  # Optional speed-up: reuse images already pulled by Docker instead of downloading again.
  for image in apache/kafka:3.9.1 postgres:16.9 hashicorp/vault:1.19.5 gitlab/gitlab-ce:18.2.1-ce.0; do
    if sudo -n crictl inspecti "docker.io/$image" >/dev/null 2>&1; then continue; fi
    if docker image inspect "$image" >/dev/null 2>&1; then
      echo "== Preload $image from Docker into containerd"
      docker save "$image" | sudo -n ctr -n k8s.io images import - >/dev/null || echo "   preload failed; kubelet will pull"
    fi
  done
fi

echo "== Namespaces"
kubectl apply -k "$K8S_DIR/namespaces"

secret() { # namespace name key=value...
  local ns="$1" name="$2"; shift 2
  local args=()
  for kv in "$@"; do args+=(--from-literal="$kv"); done
  kubectl -n "$ns" create secret generic "$name" "${args[@]}" --dry-run=client -o yaml \
    | kubectl label --local -f - app.kubernetes.io/part-of=nifi-market-lab -o yaml \
    | kubectl apply -f - >/dev/null
  echo "   secret $ns/$name"
}
echo "== Secrets from .env (never committed)"
for m in stag vn indo; do
  secret "nifi-$m" nifi-admin username=admin "password=$NIFI_PASSWORD" "sensitive-props-key=$NIFI_SENSITIVE_PROPS_KEY"
done
for m in vn indo; do secret "postgres-$m" postgres-auth "password=$DB_PASSWORD"; done
secret gitlab gitlab-root "password=$GITLAB_PASSWORD"

echo "== Apply manifests"
kubectl apply -k "$K8S_DIR"

echo "== Wait for workloads"
wait_sts() { kubectl -n "$1" rollout status "statefulset/$2" --timeout="${3:-600s}"; }
for m in vn indo; do wait_sts "postgres-$m" postgres; wait_sts "kafka-$m" kafka; wait_sts "vault-$m" vault; done
for m in vn indo; do kubectl -n "kafka-$m" wait --for=condition=complete job/kafka-init-topics --timeout=300s; done
for m in stag vn indo; do wait_sts "nifi-$m" nifi 900s; done
wait_sts gitlab gitlab 1800s
kubectl -n lab-gateway get gateway lab
echo "Deploy finished. Next: python3 k8s/scripts/vault-init.py"
