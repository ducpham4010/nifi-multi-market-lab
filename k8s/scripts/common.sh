#!/usr/bin/env bash
# Shared settings for the Kubernetes lab scripts. Run on the lab host (kubectl + docker).
set -euo pipefail
K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "$K8S_DIR/.." && pwd)"
cd "$REPO_DIR"
if [[ ! -f .env ]]; then echo 'Copy .env.example to .env first (credentials stay out of Git).' >&2; exit 1; fi
set -a
# shellcheck disable=SC1091
source .env
set +a
: "${HOST_IP:?Set HOST_IP in .env}"
NIFI_K8S_IMAGE="${NIFI_K8S_IMAGE:-localhost:31102/nifi-market-lab/nifi:2.6.0-lab1}"
NIFI_SENSITIVE_PROPS_KEY="${NIFI_SENSITIVE_PROPS_KEY:-lab-sensitive-key-123456}"
command -v kubectl >/dev/null || { echo 'kubectl not found' >&2; exit 1; }

# market -> namespaces / topic / schema used by produce/query helpers
market_kafka_ns() { case "$1" in stag|vn) echo kafka-vn;; indo) echo kafka-indo;; *) return 1;; esac; }
market_pg_ns()    { case "$1" in stag|vn) echo postgres-vn;; indo) echo postgres-indo;; *) return 1;; esac; }
market_topic()    { case "$1" in stag) echo trips-staging;; vn|indo) echo trips;; *) return 1;; esac; }
market_schema()   { case "$1" in stag) echo staging;; vn|indo) echo market;; *) return 1;; esac; }
