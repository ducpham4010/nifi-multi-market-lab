#!/usr/bin/env bash
# Tear down the whole Kubernetes lab so it can be rebuilt from scratch (up.sh).
#   bash k8s/scripts/down.sh          # shows what will be deleted, asks to type "down"
#   bash k8s/scripts/down.sh --yes    # no prompt (automation)
#
# Deletes every object of k8s/ (11 namespaces with all PVC data: NiFi flows, Kafka,
# PostgreSQL, Vault, GitLab; the Vault TokenReview ClusterRoleBindings) and waits
# until namespaces, volumes and the Envoy proxy are gone. Then moves .private/k8s
# (Vault unseal keys/root tokens, release token, e2e evidence) to
# .private/k8s-archive-<time>: those keys belong to the deleted Vault data and
# would make vault-init.py refuse to initialize the new Vaults.
#
# Kept: .env, the NiFi image in the registry, images already in containerd, cluster
# add-ons (Envoy Gateway, GatewayClass eg-lab, local-path, registry), Docker Compose
# volumes and .private/vault-*-init.json of the Compose lab.
source "$(dirname "$0")/common.sh"

assume_yes=0
for arg in "$@"; do
  case "$arg" in
    --yes|-y) assume_yes=1 ;;
    *) echo 'Usage: down.sh [--yes]' >&2; exit 1 ;;
  esac
done

LABEL=app.kubernetes.io/part-of=nifi-market-lab
read -r -a namespaces <<<"$(kubectl get ns -l "$LABEL" -o jsonpath='{.items[*].metadata.name}')"
private="$REPO_DIR/.private/k8s"

if [[ ${#namespaces[@]} -eq 0 ]]; then
  echo "No lab namespace found (label $LABEL)."
else
  echo "This deletes the Kubernetes lab and ALL its data (cannot be undone):"
  echo "  namespaces: ${namespaces[*]}"
  for ns in "${namespaces[@]}"; do
    { kubectl -n "$ns" get pvc --no-headers 2>/dev/null || true; } | awk -v ns="$ns" '{print "  pvc " ns "/" $1 " (" $4 ")"}'
  done
fi
[[ -d "$private" ]] && echo "  then move .private/k8s (Vault keys of these Vaults) to an archive folder"

if [[ $assume_yes != 1 ]]; then
  [[ -t 0 ]] || { echo 'Not a terminal: re-run with --yes to confirm.' >&2; exit 1; }
  read -r -p 'Type "down" to continue: ' answer
  [[ "$answer" == down ]] || { echo 'Cancelled.'; exit 1; }
fi

wait_until_empty() { # description, seconds, command printing remaining items
  local what="$1" timeout="$2"; shift 2
  local deadline=$((SECONDS + timeout)) left
  while left="$("$@")"; [[ -n "$left" ]]; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $what to disappear:" >&2
      echo "$left" >&2
      exit 1
    fi
    sleep 5
  done
  echo "   $what: gone"
}
lab_namespaces_left() {
  [[ ${#namespaces[@]} -eq 0 ]] || kubectl get ns "${namespaces[@]}" --ignore-not-found -o name
}
lab_volumes_left() {
  kubectl get pv -o jsonpath='{range .items[*]}{.metadata.name} {.spec.claimRef.namespace}{"\n"}{end}' \
    | awk 'NR==FNR {lab[$1]=1; next} ($2 in lab) {print $1}' <(printf '%s\n' "${namespaces[@]}") -
}
envoy_proxy_left() {
  kubectl -n envoy-gateway-system get deploy,svc -o name \
    -l gateway.envoyproxy.io/owning-gateway-name=lab,gateway.envoyproxy.io/owning-gateway-namespace=lab-gateway
}

if [[ ${#namespaces[@]} -gt 0 ]]; then
  echo "== Delete lab objects (kubectl delete -k k8s)"
  kubectl delete -k "$K8S_DIR" --ignore-not-found --wait=false
  echo "== Wait for deletion"
  wait_until_empty "namespaces" 900 lab_namespaces_left
  wait_until_empty "persistent volumes (local-path data)" 300 lab_volumes_left
  wait_until_empty "Envoy proxy of Gateway lab (NodePort 31080/31443)" 300 envoy_proxy_left
fi

if [[ -d "$private" && -n "$(ls -A "$private")" ]]; then
  archive="$private-archive-$(date +%Y%m%d-%H%M%S)"
  mv "$private" "$archive"
  echo "== Moved .private/k8s to ${archive#"$REPO_DIR"/}"
  echo "   It holds the keys of the deleted Vaults; keep it for audit or delete it."
fi

echo "Lab removed. Rebuild from scratch: bash k8s/scripts/up.sh"
