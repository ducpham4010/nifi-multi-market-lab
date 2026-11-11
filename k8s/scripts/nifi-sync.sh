#!/usr/bin/env bash
# Run a sync Job from its suspended CronJob template and show its log.
#   bash k8s/scripts/nifi-sync.sh params   stag|vn|indo|all   # Vault -> shared-runtime
#   bash k8s/scripts/nifi-sync.sh registry stag|vn|indo|all   # Vault PrAT -> Registry Client
# Both jobs authenticate to Vault with ServiceAccount nifi of the target namespace.
source "$(dirname "$0")/common.sh"
kind="${1:?Usage: nifi-sync.sh params|registry stag|vn|indo|all}"
target="${2:?Usage: nifi-sync.sh params|registry stag|vn|indo|all}"
case "$kind" in
  params) cronjob=vault-param-sync ;;
  registry) cronjob=gitlab-registry-sync ;;
  *) echo 'First argument: params or registry' >&2; exit 1 ;;
esac
case "$target" in
  all) markets=(stag vn indo) ;;
  stag|vn|indo) markets=("$target") ;;
  *) echo 'Second argument: stag, vn, indo or all' >&2; exit 1 ;;
esac
status=0
for m in "${markets[@]}"; do
  ns="nifi-$m"
  job="$cronjob-$(date +%Y%m%d%H%M%S)"
  echo "== $ns: job/$job"
  kubectl -n "$ns" create job --from="cronjob/$cronjob" "$job" >/dev/null
  result=''
  for _ in $(seq 150); do  # up to 5 minutes; stops at the first terminal state
    result="$(kubectl -n "$ns" get "job/$job" -o jsonpath='{.status.succeeded}/{.status.failed}')"
    [[ "$result" == 1/* || "$result" == */1 ]] && break
    sleep 2
  done
  if [[ "$result" == 1/* ]]; then
    kubectl -n "$ns" logs "job/$job"
  else
    kubectl -n "$ns" logs "job/$job" || true
    echo "FAILED: $ns job/$job" >&2
    status=1
  fi
done
exit "$status"
