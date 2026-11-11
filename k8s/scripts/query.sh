#!/usr/bin/env bash
# Show rows written by a NiFi instance (via kubectl exec, local socket in the pod).
#   bash k8s/scripts/query.sh stag|vn|indo
source "$(dirname "$0")/common.sh"
market="${1:?Usage: query.sh stag|vn|indo}"
ns="$(market_pg_ns "$market")" || { echo 'Use stag, vn or indo' >&2; exit 1; }
schema="$(market_schema "$market")"
kubectl -n "$ns" exec postgres-0 -- psql -U admin -d market \
  -c "SELECT * FROM $schema.trip_events ORDER BY event_id;"
